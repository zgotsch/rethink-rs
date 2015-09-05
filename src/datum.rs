use std::collections::hash_map::HashMap;

extern crate rustc_serialize;
use self::rustc_serialize::json;

#[derive(Debug, PartialEq, Clone)]
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
    // TODO(zach): Do not expose
    pub fn from_str(json_str: &str) -> Self {
        Datum::from_json(json::Json::from_str(json_str).unwrap())
    }

    // TODO(zach): Do not expose
    pub fn from_json(json: json::Json) -> Self {
        match json {
            json::Json::Null => Datum::Null,
            json::Json::Boolean(b) => Datum::Bool(b),
            json::Json::String(s) => Datum::String(s),
            json::Json::U64(n) => Datum::Number(n as f64),
            json::Json::I64(n) => Datum::Number(n as f64),
            json::Json::F64(n) => Datum::Number(n),
            json::Json::Array(json_array) => {
                Datum::Array(json_array.into_iter().map(|json| {
                    Datum::from_json(json)
                }).collect())
            },
            json::Json::Object(json_object) => {
                Datum::Object(json_object.into_iter().map(|(k, json)| {
                    (k, Datum::from_json(json))
                }).collect())
            }
        }
    }

    // TODO(zach): Do not expose
    pub fn serialize(&self) -> String {
        match self {
            &Datum::Null => "null".to_string(),
            &Datum::Bool(b) => (if b { "true" } else { "false" }).to_string(),
            &Datum::String(ref s) => format!("\"{}\"", s),
            &Datum::Number(n) => n.to_string(),
            &Datum::Object(ref m) => {
                format!("{{{}}}", m.iter().map(|(k, datum)| {
                    format!("\"{}\":{}", k, datum.serialize())
                }).collect::<Vec<_>>().connect(","))
            },
            x => format!("unimplemented: {:?}", x)
        }
    }
}
