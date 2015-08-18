
extern crate byteorder;
extern crate rustc_serialize;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use std::io::prelude::*;
use std::net::TcpStream;
use std::str::{Utf8Error};
use std::string::{FromUtf8Error};
use std::fmt;
use std::io;
use std::convert::From;
use std::collections::hash_map::HashMap;

extern crate protobuf; // depend on rust-protobuf runtime
pub mod ql2;
use ql2::Term_TermType;
use ql2::Response_ResponseType;

use rustc_serialize::json;

#[derive(Debug)]
pub struct UnknownError {
    description: String
}

impl UnknownError {
    fn new(description : String) -> UnknownError {
        UnknownError{ description: description }
    }
}

impl fmt::Display for UnknownError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.description, f)
    }
}

#[macro_use] extern crate wrapped_enum;

wrapped_enum!{
    #[derive(Debug)]
    /// More Docs
    pub enum Error {
        /// Converting bytes to utf8 string
        FromUtf8Error(FromUtf8Error),
        /// Utf8Error
        Utf8Error(Utf8Error),
        /// IO
        Io(io::Error),
        /// Byteorder
        Byteorder(byteorder::Error),
        /// Connection Error
        ConnectionError(String),
        /// Unknown Error
        UnknownError(UnknownError),
        /// rustc_serialize json parsing error
        JsonParse(rustc_serialize::json::ParserError),
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::FromUtf8Error(ref error) => fmt::Display::fmt(error, f),
            Error::Utf8Error(ref error) => fmt::Display::fmt(error, f),
            Error::Io(ref error) => fmt::Display::fmt(error, f),
            Error::Byteorder(ref error) => fmt::Display::fmt(error, f),
            Error::ConnectionError(ref s) => fmt::Display::fmt(s, f),
            Error::UnknownError(ref error) => fmt::Display::fmt(error, f),
            Error::JsonParse(ref error) => fmt::Display::fmt(error, f),
        }
    }
}


fn read_string<T: Read>(r : &mut T) -> Result<String, Error> {
    let bytes = try!(r.bytes().take_while(|b| {
        match *b {
            Err(_) => false,
            Ok(x) => x != 0,
        }
    }).collect::<Result<Vec<u8>,_>>());
    let s = try!(String::from_utf8(bytes));
    Ok(s)
}

pub use ConnectionState::*;
#[derive(Debug)]
pub enum ConnectionState {
    Open(TcpStream),
    Closed,
}

pub struct Connection {
    state: ConnectionState,
    query_count: u64,
    host: String,
    port: u16,
    default_db: Option<String>,
    auth_key: String,
    timeout_secs: u32,
}

impl Connection {
    pub fn connect(&mut self) -> Option<Error> {
        match self.state {
            Open(_) => Some(From::from(UnknownError::new(
                "Connection must be closed before calling connect.".into()
            ))),
            Closed => match TcpStream::connect((&*self.host, self.port)) {
                Ok(stream) => {
                    self.state = Open(stream);
                    None
                },
                Err(e) => {
                    Some(Error::ConnectionError(format!("{}", e)))
                }
            }
        }
    }

    pub fn close(&mut self) {
        self.state = Closed
    }

    pub fn reconnect(&mut self) -> Option<Error> {
        self.close();
        self.connect()
    }

    pub fn use_(&mut self, default_db: Option<&str>) {
        self.default_db = default_db.map(|x| x.to_string());
    }

    fn handshake(&mut self) -> Result<(), Error> {
        let v4 = 0x400c2d20u32;
        let json = 0x7e6970c7u32;

        match self.state {
            Open(ref mut stream) => {
                try!(stream.write_u32::<LittleEndian>(v4));
                try!(stream.write_u32::<LittleEndian>(self.auth_key.len() as u32));
                for byte in self.auth_key.bytes() {
                    try!(stream.write_u8(byte));
                }
                try!(stream.write_u32::<LittleEndian>(json));
                let res = try!(read_string(stream));
                if res != "SUCCESS" {
                  Err(Error::ConnectionError(res))
                } else {
                  Ok(())
                }
            },
            Closed => Err(From::from(UnknownError::new("Tried to handshake on a closed connection!".to_string())))
        }
    }

    fn send(&mut self, raw_string : &str) -> Result<json::Json, Error> {
        match self.state {
            Open(ref mut stream) => {
                self.query_count += 1;
                try!(stream.write_u64::<LittleEndian>(self.query_count));

                let bytes = raw_string.as_bytes();
                try!(stream.write_u32::<LittleEndian>(bytes.len() as u32));
                try!(stream.write_all(bytes));

                let query_token_resp = try!(stream.read_u64::<LittleEndian>());
                if query_token_resp != self.query_count {
                  return Err(UnknownError::new(
                    format!("Query token ({}) does not match {}",
                      query_token_resp, self.query_count)
                    ).into()
                  );
                }

                let resp_len = stream.read_u32::<LittleEndian>().unwrap();
                let mut resp_bytes = Read::by_ref(stream).take(resp_len as u64);
                json::Json::from_reader(&mut resp_bytes).map_err(Error::from)
            },
            Closed => Err(From::from(UnknownError::new("Tried to send on a closed connection!".to_string())))
        }
    }
}

pub struct Rethink;

impl Rethink {
    pub fn connect_default() -> Result<Connection, Error> {
        Rethink::connect(&"localhost".to_string(), 28015, Some(&"test".to_string()), &"".to_string(), 20)
    }

    pub fn connect(host: &str, port: u16, default_db: Option<&str>, auth_key: &str, timeout_secs: u32) -> Result<Connection, Error> {
        let mut c = Connection{
            state: Closed,
            query_count: 0,
            host: host.to_string(),
            port: port,
            default_db: default_db.map(|x| x.to_string()),
            auth_key: auth_key.to_string(),
            timeout_secs: timeout_secs,
        };
        match c.connect() {
            Some(error) => Err(error),
            None => {
                try!(c.handshake());
                Ok(c)
            }
        }
    }

    pub fn db_create(db_name: &str) -> ReQL {
        let mut args = Vec::new();
        args.push(ReQL::string(db_name));

        ReQL::Term {
            command: Term_TermType::DB_CREATE,
            arguments: args,
            optional_arguments: HashMap::new()
        }
    }

    pub fn db_drop(db_name: &str) -> ReQL {
        let mut args = Vec::new();
        args.push(ReQL::string(db_name));

        ReQL::Term {
            command: Term_TermType::DB_DROP,
            arguments: args,
            optional_arguments: HashMap::new()
        }
    }
}

#[derive(Debug)]
pub enum ReQL {
    Term {
        command: Term_TermType,
        arguments: Vec<ReQL>,
        optional_arguments: HashMap<String, ReQL>
    },
    Datum(Datum)
}

#[derive(Debug)]
pub enum Datum {
    Null,
    Bool(bool),
    String(String),
    Number(f64),
    Array(Vec<Datum>),
    Object(HashMap<String, Datum>),
    // Time ZonedTime |
    // Point LonLat |
    // Line Line |
    // Polygon Polygon |
    // Binary SB.ByteString
}

impl Datum {
    pub fn serialize(&self) -> String {
        match self {
            &Datum::Null => "null".to_string(),
            &Datum::Bool(b) => (if b { "true" } else { "false" }).to_string(),
            &Datum::String(ref s) => s.clone(),
            &Datum::Number(n) => n.to_string(),
            _ => "unimplemented".to_string()
        }
    }
}

impl ReQL {
    pub fn string(string: &str) -> Self {
        ReQL::Datum(Datum::String(string.to_string()))
    }

    pub fn run(&self, connection: &mut Connection) -> Result<RethinkResponse, Error> {
        let string_reql = self.serialize_toplevel();
        connection.send(&string_reql).and_then(|json| { RethinkResponse::from_json(json) })
    }

    fn serialize_toplevel(&self) -> String {
        format!("[1,{},{{}}]", self.serialize())
    }

    fn serialize(&self) -> String {
        match self {
            &ReQL::Term{ref command,
                        ref arguments,
                        ref optional_arguments} => {
                            format!("[{},{:?},{}]", *command as u32, arguments.iter().map(|a| {
                                a.serialize()
                            }).collect::<Vec<_>>(), "{}")
                        },
            &ReQL::Datum(ref d) => d.serialize()
        }
    }
}

#[derive(Debug)]
pub struct RethinkResponse {
    response_type: Response_ResponseType,
    result: Vec<json::Json>,
    backtrace: Option<Vec<String>>,
    // profile: ???,
    // notes: Vec<notes>,
}

impl RethinkResponse {
    fn from_json(json: json::Json) -> Result<RethinkResponse, Error> {
        // TODO(zach): this is so unreadable
        if let json::Json::Object(mut o) = json {
            return Ok(RethinkResponse {
                response_type: match ::protobuf::ProtobufEnum::from_i32(match o.remove("t") {
                    Some(json::Json::U64(n)) => n as i32,
                    Some(json::Json::I64(n)) => n as i32,
                    Some(json::Json::F64(n)) => n.floor() as i32,
                    _ => return Err(From::from(UnknownError::new("Parse error: rethink response type was non-numeric".to_string())))
                }) {
                    Some(response_type) => response_type,
                    None => return Err(From::from(UnknownError::new("Parse error: unrecognized rethink response type".to_string())))
                },
                result: match o.remove("r") {
                    Some(json::Json::Array(json_array)) => json_array,
                    Some(..) => return Err(From::from(UnknownError::new("Parse error: \"r\" field of rethink response should be an array".to_string()))),
                    None => return Err(From::from(UnknownError::new("Parse error: rethink response didn't contain a response field".to_string())))
                },
                backtrace: o.remove("b").and_then(|backtrace_json| {
                    match backtrace_json {
                        json::Json::Array(backtrace_array) => {
                            Some(backtrace_array.into_iter().map(|backtace_item| {
                                // TODO(zach): I hope these are strings!
                                match backtace_item {
                                    json::Json::String(s) => s,
                                    _ => "".to_string()
                                }
                            }).collect())
                        },
                        _ => None
                    }
                })
            })
        } else {
            return Err(From::from(UnknownError::new("Parse error: expected the rethink json response to be an object".to_string())))
        }
    }
}

#[test]
fn deserialize_response() {
    // TODO(zach)
}

#[test]
fn it_works() {
  let mut conn = Rethink::connect_default().unwrap();
  println!("{}", conn.send(r#"[1,[39,[[15,[[14,["blog"]],"users"]],{"name":"Michel"}]],{}]"#).unwrap());
  // println!("{}", conn.send(r#"[1,"foo",{}]"#).unwrap());
  panic!("ASDF");
}

#[macro_use] extern crate matches;

#[test]
fn connect_disconnect() {
    let mut conn = Rethink::connect_default().unwrap();
    assert!(matches!(conn.state, Open(_)));

    conn.close();
    assert!(matches!(conn.state, Closed));

    conn.connect();
    assert!(matches!(conn.state, Open(_)));

    conn.reconnect();
    assert!(matches!(conn.state, Open(_)));
}

#[test]
fn use_default_db() {
    let mut conn = Rethink::connect_default().unwrap();
    assert!(matches!(conn.default_db, Some(ref db_name) if db_name == "test"));

    conn.use_(Some("other_name"));
    assert!(matches!(conn.default_db, Some(ref db_name) if db_name == "other_name"));

    conn.use_(None);
    assert!(matches!(conn.default_db, None));
}

#[test]
fn create_db() {
    let mut conn = Rethink::connect_default().unwrap();
    let res = Rethink::db_create("db_create_test").run(&mut conn).unwrap();
    match res.response_type {
        Response_ResponseType::SUCCESS_ATOM => {
            match res.result.first().unwrap() {
                &json::Json::Object(ref o) => {
                    let json_create_count = o.get("dbs_created").unwrap();
                    match json_create_count {
                        &json::Json::U64(n) => assert!(n == 1),
                        _ => panic!("unrecognized response: {:?}", res)
                    }
                }
                _ => panic!("unrecognized response: {:?}", res)
            }
        }
        _ => panic!("got an unexpected response type: {:?}", res.response_type)
    }
}

#[test]
fn drop_db() {
    let mut conn = Rethink::connect_default().unwrap();
    Rethink::db_create("db_drop_test").run(&mut conn).unwrap();
    let res = Rethink::db_drop("db_drop_test").run(&mut conn).unwrap();
    match res.response_type {
        Response_ResponseType::SUCCESS_ATOM => {
            match res.result.first().unwrap() {
                &json::Json::Object(ref o) => {
                    let json_create_count = o.get("dbs_dropped").unwrap();
                    match json_create_count {
                        &json::Json::U64(n) => assert!(n == 1),
                        _ => panic!("unrecognized response: {:?}", res)
                    }
                }
                _ => panic!("unrecognized response: {:?}", res)
            }
        }
        _ => panic!("got an unexpected response type: {:?}", res.response_type)
    }
}
