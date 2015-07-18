extern crate byteorder;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use std::io::prelude::*;
use std::net::TcpStream;
use std::str;
use std::string::FromUtf8Error;
use std::fmt;
use std::io;

#[macro_use] extern crate wrapped_enum;

wrapped_enum!{
    #[derive(Debug)]
    /// More Docs
    pub enum Error {
        /// Converting bytes to utf8 string
        FromUtf8Error(FromUtf8Error),

        /// IO
        Io(io::Error),
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::FromUtf8Error(ref error) => fmt::Display::fmt(error, f),
            Error::Io(ref error) => fmt::Display::fmt(error, f),
        }
    }
}


fn read_string<T: Read>(r : &mut T) -> Result<String, Error> {
    let u = r.bytes();
    let bytes = try!(u.take_while(|b| b.unwrap_or(0) == 0).collect::<Result<Vec<_>,_>>());
    let s = try!(String::from_utf8(bytes));
    Ok(s)
}

#[test]
fn it_works() {
  let mut stream = TcpStream::connect("127.0.0.1:28015").unwrap();

  let v4 = 0x400c2d20u32;
  let json = 0x7e6970c7u32;
  stream.write_u32::<LittleEndian>(v4);
  stream.write_u32::<LittleEndian>(0);
  stream.write_u32::<LittleEndian>(json);

  assert!(read_string(&mut stream).unwrap() == "SUCCESS");

  let query_token = 5u64;
  stream.write_u64::<LittleEndian>(query_token);

  // Length
  //
  let query = r#"[1,[39,[[15,[[14,["blog"]],"users"]],{"name":"Michel"}]],{}]"#;
  stream.write_u32::<LittleEndian>(query.as_bytes().len() as u32);
  stream.write_all(query.as_bytes());


  /*
  let b = stream.read_u8().unwrap();
  println!("B: {:?}", b);
  */

  let query_token_resp = stream.read_u64::<LittleEndian>().unwrap();
  println!("TOKEN: {:?}", query_token_resp);

  let resp_len = stream.read_u32::<LittleEndian>().unwrap();
  println!("Response Length: {:?}", resp_len);

  let mut resp = [0; 128];
  let _ = stream.read(&mut resp);
  println!("Response: {:?}", str::from_utf8(&resp).unwrap());



  panic!("print stuff");
}
