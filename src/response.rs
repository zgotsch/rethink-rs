extern crate rustc_serialize;
use self::rustc_serialize::json;

use datum::Datum;

use ql2::Response_ResponseType;


pub type ResponseParseError = String;

#[derive(Debug)]
pub struct RethinkResponse {
    pub response_type: Response_ResponseType,
    pub result: Vec<Datum>,
    pub backtrace: Option<Vec<String>>,
    // profile: ???,
    // notes: Vec<notes>,
}

impl RethinkResponse {
    // TODO(zach): Do not expose
    pub fn from_json(json: json::Json) -> Result<RethinkResponse, ResponseParseError> {
        // TODO(zach): this is so unreadable
        if let json::Json::Object(mut o) = json {
            return Ok(RethinkResponse {
                response_type: match ::protobuf::ProtobufEnum::from_i32(match o.remove("t") {
                    Some(json::Json::U64(n)) => n as i32,
                    Some(json::Json::I64(n)) => n as i32,
                    Some(json::Json::F64(n)) => n.floor() as i32,
                    _ => return Err("Parse error: rethink response type was non-numeric".to_string())
                }) {
                    Some(response_type) => response_type,
                    None => return Err("Parse error: unrecognized rethink response type".to_string())
                },
                result: match o.remove("r") {
                    Some(json::Json::Array(json_array)) => json_array.into_iter().map(|json| {
                        Datum::from_json(json)
                    }).collect(),
                    Some(..) => return Err("Parse error: \"r\" field of rethink response should be an array".to_string()),
                    None => return Err("Parse error: rethink response didn't contain a response field".to_string())
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
            return Err("Parse error: expected the rethink json response to be an object".to_string())
        }
    }
}

#[test]
fn deserialize_response() {
    let resp = RethinkResponse::from_json(json::Json::from_str(r###"{"t":1,"r":["foo"],"n":[]}"###).unwrap()).unwrap();
    assert!(resp.response_type == Response_ResponseType::SUCCESS_ATOM);
    assert!(resp.result.first().unwrap() == &Datum::String("foo".to_string()));
    assert!(resp.backtrace == Option::None)
}
