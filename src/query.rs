use std::collections::hash_map::HashMap;

use datum::Datum;
use connection::{Connection, SendError};
use response::{ResponseParseError, RethinkResponse};

use ql2::Term_TermType;


wrapped_enum!{
    #[derive(Debug)]
    /// An error which can occur when running a query
    pub enum RunQueryError {
        /// An error sending the query
        SendError(SendError),
        /// An error decoding the query
        ParseError(ResponseParseError)
    }
}

#[derive(Debug, Clone)]
pub enum ReQL {
    Term {
        command: Term_TermType,
        arguments: Vec<ReQL>,
        optional_arguments: HashMap<String, Datum>
    },
    Datum(Datum)
}

pub mod internal {
    use super::*;
    use datum::Datum;

    impl ReQL {
        pub fn string(string: &str) -> Self {
            ReQL::Datum(Datum::String(string.to_string()))
        }
    }
}

impl ReQL {
    pub fn run(&self, connection: &mut Connection) -> Result<RethinkResponse, RunQueryError> {
        let string_reql = self.serialize_query_for_connection(connection);
        let json = try!(connection.send(&string_reql));
        Ok(try!(RethinkResponse::from_json(json)))
    }

    // TODO(zach): Do not expose
    pub fn serialize_query_for_connection(&self, connection: &Connection) -> String {
        format!("[1,{},{}]", self.serialize(), connection.serialize_params())
    }

    // TODO(zach): Do not expose
    pub fn serialize(&self) -> String {
        match self {
            &ReQL::Term{ref command,
                        ref arguments,
                        ref optional_arguments} => {
                            let command_string = (*command as u32).to_string();
                            let arguments_string = format!("[{}]", arguments.iter().map(|a| {
                                a.serialize()
                            }).collect::<Vec<_>>().connect(","));

                            let mut parts = vec!(command_string, arguments_string);

                            // Only send optional arguments if they exist (to match official
                            // driver behavior)
                            if !optional_arguments.is_empty() {
                                let optional_arguments_string = format!(
                                    "{{{}}}",
                                    optional_arguments.iter().map(|(option_name, option_val)| {
                                        format!("\"{}\":{}", option_name, option_val.serialize())
                                    }).collect::<Vec<_>>().connect(",")
                                );

                                parts.push(optional_arguments_string);
                            }
                            format!("[{}]", parts.connect(","))
                        },
            &ReQL::Datum(ref d) => d.serialize()
        }
    }

    pub fn table(&self, table_name: &str) -> ReQL {
        ReQL::Term {
            command: Term_TermType::TABLE,
            arguments: vec![self.clone(), ReQL::string(table_name)],
            optional_arguments: HashMap::new()
        }
    }

    pub fn get(&self, key: &str) -> ReQL {
        ReQL::Term {
            command: Term_TermType::GET,
            arguments: vec![self.clone(), ReQL::string(key)],
            optional_arguments: HashMap::new()
        }
    }

    pub fn insert(&self, document: &Datum, durability: Option<Durability>) -> ReQL {
        let mut optional_arguments = HashMap::new();
        if let Some(d) = durability {
            optional_arguments.insert("durability".to_string(), Datum::String(d.serialize()));
        }

        ReQL::Term {
            command: Term_TermType::INSERT,
            arguments: vec![self.clone(), ReQL::Datum(document.clone())],
            optional_arguments: optional_arguments
        }
    }
}

pub enum Durability {
    Hard,
    Soft
}

impl Durability {
    fn serialize(&self) -> String {
        match *self {
            Durability::Hard => "hard",
            Durability::Soft => "soft"
        }.to_string()
    }
}
