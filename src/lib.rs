
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

pub struct Connection {
    stream: TcpStream,
    query_count: u64,
    default_db: String,
    auth_key: String,
    timeout_secs: u32,
}

impl Connection {
    pub fn connect(host: &str, port: u16, default_db: &str, auth_key: &str, timeout_secs: u32) -> Result<Connection, Error> {
        let stream = try!(TcpStream::connect((host, port)));
        Ok(Connection{
            stream: stream,
            query_count: 0,
            default_db: default_db.to_string(),
            auth_key: auth_key.to_string(),
            timeout_secs: timeout_secs,
        })
    }

    fn handshake(&mut self) -> Result<(), Error> {
        let v4 = 0x400c2d20u32;
        let json = 0x7e6970c7u32;
        try!(self.stream.write_u32::<LittleEndian>(v4));
        try!(self.stream.write_u32::<LittleEndian>(self.auth_key.len() as u32));
        for byte in self.auth_key.bytes() {
            try!(self.stream.write_u8(byte));
        }
        try!(self.stream.write_u32::<LittleEndian>(json));
        let res = try!(read_string(&mut self.stream));
        if res != "SUCCESS" {
          Err(Error::ConnectionError(res))
        } else {
          Ok(())
        }
    }

    fn send(&mut self, raw_string : &str) -> Result<json::Json, Error> {
      self.query_count += 1;
      try!(self.stream.write_u64::<LittleEndian>(self.query_count));

      let bytes = raw_string.as_bytes();
      try!(self.stream.write_u32::<LittleEndian>(bytes.len() as u32));
      try!(self.stream.write_all(bytes));

      let query_token_resp = try!(self.stream.read_u64::<LittleEndian>());
      if query_token_resp != self.query_count {
        return Err(UnknownError::new(
          format!("Query token ({}) does not match {}",
            query_token_resp, self.query_count)
          ).into()
        );
      }

      let resp_len = self.stream.read_u32::<LittleEndian>().unwrap();
      let mut resp_bytes = Read::by_ref(&mut self.stream).take(resp_len as u64);
      json::Json::from_reader(&mut resp_bytes).map_err(Error::from)
    }
}

pub struct Rethink;

impl Rethink {
    pub fn connect_default() -> Result<Connection, Error> {
        Rethink::connect(&"localhost".to_string(), 28015, &"test".to_string(), &"".to_string(), 20)
    }

    pub fn connect(host: &str, port: u16, default_db: &str, auth_key: &str, timeout_secs: u32) -> Result<Connection, Error> {
        Connection::connect(host, port, default_db, auth_key, timeout_secs)
    }
}

#[test]
fn it_works() {
  let mut conn = Rethink::connect_default().unwrap();
  conn.handshake().unwrap();
  println!("{}", conn.send(r#"[1,[39,[[15,[[14,["blog"]],"users"]],{"name":"Michel"}]],{}]"#).unwrap());
  panic!("ASDF");
}
