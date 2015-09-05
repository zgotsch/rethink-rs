use std::net::TcpStream;

extern crate byteorder;
use self::byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};

use std::io;
use std::io::Read;
use std::io::Write;
use std::error::Error;
use std::string::FromUtf8Error;

extern crate rustc_serialize;
use self::rustc_serialize::json;

wrapped_enum!{
    #[derive(Debug)]
    /// An error while trying to read a C string
    // TODO(zach): Do not expose
    pub enum ReadStringError {
        /// An error decoding utf8
        FromUtf8Error(FromUtf8Error),
        /// An IO error
        IoError(io::Error)
    }
}

// Read a C string from a Read
fn read_string<T: Read>(r : &mut T) -> Result<String, ReadStringError> {
    let bytes = try!(r.bytes().take_while(|b| {
        match *b {
            Err(_) => false,
            Ok(x) => x != 0,
        }
    }).collect::<Result<Vec<u8>,_>>());
    let s = try!(String::from_utf8(bytes));
    Ok(s)
}

#[derive(Debug)]
pub enum ConnectionError {
    ConnectionError(String),
    InvalidOperationError(String),
    RethinkError(String),
    IoError(String)
}

impl From<byteorder::Error> for ConnectionError {
    fn from(e: byteorder::Error) -> Self {
        ConnectionError::IoError(match e {
            byteorder::Error::UnexpectedEOF => "Unexpected EOF",
            byteorder::Error::Io(ref inner) => inner.description()
        }.to_string())
    }
}


impl From<ReadStringError> for ConnectionError {
    fn from(e: ReadStringError) -> Self {
        ConnectionError::IoError(match e {
            ReadStringError::FromUtf8Error(ref inner) => inner.description(),
            ReadStringError::IoError(ref inner) => inner.description()
        }.to_string())
    }
}

#[derive(Debug)]
pub enum SendError {
    ClosedConnectionError,
    ResponseParseError(json::ParserError),
    MismatchedQueryTokenError(String),
    IoError(String)
}

impl From<byteorder::Error> for SendError {
    fn from(e: byteorder::Error) -> Self {
        SendError::IoError(match e {
            byteorder::Error::UnexpectedEOF => "Unexpected EOF",
            byteorder::Error::Io(ref inner) => inner.description()
        }.to_string())
    }
}

impl From<io::Error> for SendError {
    fn from(e: io::Error) -> Self {
        SendError::IoError(e.description().to_string())
    }
}

use self::ConnectionState::*;
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
    pub fn new(host: &str, port: u16, default_db: Option<&str>, auth_key: Option<&str>, timeout_secs: u32) -> Self {
        Connection {
            state: ConnectionState::Closed,
            query_count: 0,
            host: host.to_string(),
            port: port,
            default_db: default_db.map(|x| x.to_string()),
            auth_key: auth_key.unwrap_or("").to_string(),
            timeout_secs: timeout_secs
        }
    }

    pub fn connect(&mut self) -> Result<(), ConnectionError> {
        match self.state {
            Open(_) => Err(ConnectionError::InvalidOperationError(
                "Connection must be closed before calling connect.".into()
            )),
            Closed => match TcpStream::connect((&*self.host, self.port)) {
                Ok(stream) => {
                    self.state = Open(stream);
                    self.handshake()
                },
                Err(e) => {
                    Err(ConnectionError::ConnectionError(e.description().into()))
                }
            }
        }
    }

    pub fn close(&mut self) {
        self.state = Closed
    }

    pub fn reconnect(&mut self) -> Result<(), ConnectionError> {
        self.close();
        self.connect()
    }

    pub fn use_(&mut self, default_db: Option<&str>) {
        self.default_db = default_db.map(|x| x.to_string());
    }

    fn handshake(&mut self) -> Result<(), ConnectionError> {
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
                  Err(ConnectionError::RethinkError(res))
                } else {
                  Ok(())
                }
            },
            Closed => Err(ConnectionError::InvalidOperationError(
                "Tried to handshake on a closed connection!".into()))
        }
    }

    // TODO(zach): Do not expose
    pub fn send(&mut self, raw_string : &str) -> Result<json::Json, SendError> {
        match self.state {
            Open(ref mut stream) => {
                self.query_count += 1;
                try!(stream.write_u64::<LittleEndian>(self.query_count));

                let bytes = raw_string.as_bytes();
                try!(stream.write_u32::<LittleEndian>(bytes.len() as u32));
                try!(stream.write_all(bytes));

                let query_token_resp = try!(stream.read_u64::<LittleEndian>());
                if query_token_resp != self.query_count {
                  return Err(SendError::MismatchedQueryTokenError(
                    format!("Query token ({}) does not match {}",
                      query_token_resp, self.query_count)
                    )
                  );
                }

                let resp_len = stream.read_u32::<LittleEndian>().unwrap();
                let mut resp_bytes = Read::by_ref(stream).take(resp_len as u64);
                json::Json::from_reader(&mut resp_bytes).map_err(|parser_error| {
                    SendError::ResponseParseError(parser_error)})
            },
            Closed => Err(SendError::ClosedConnectionError)
        }
    }

    // TODO(zach): Do not expose
    pub fn serialize_params(&self) -> String {
        // TODO(zach): currently only default database affects this
        match self.default_db {
            Some(ref db_name) => format!(r##"{{"db":[14,["{}"]]}}"##, db_name),
            _ => "{}".to_string()
        }
    }

    pub fn is_open(&self) -> bool {
        match self.state {
            Open(..) => true,
            Closed => false
        }
    }

    pub fn default_db(&self) -> &Option<String> {
        return &self.default_db
    }
}
