pub mod rethink {
    use std::collections::hash_map::HashMap;

    use ql2::Term_TermType;

    use connection::{Connection, ConnectionError};
    use query::ReQL;
    use datum::Datum;

    pub fn connect_default() -> Result<Connection, ConnectionError> {
        connect("localhost", 28015, None, None, 20)
    }

    pub fn connect(host: &str, port: u16, default_db: Option<&str>, auth_key: Option<&str>, timeout_secs: u32) -> Result<Connection, ConnectionError> {
        let mut c = Connection::new(host, port, default_db, auth_key, timeout_secs);
        try!(c.connect());
        Ok(c)
    }

    pub fn db_create(db_name: &str) -> ReQL {
        ReQL::Term {
            command: Term_TermType::DB_CREATE,
            arguments: vec![ReQL::string(db_name)],
            optional_arguments: HashMap::new()
        }
    }

    pub fn db_drop(db_name: &str) -> ReQL {
        ReQL::Term {
            command: Term_TermType::DB_DROP,
            arguments: vec![ReQL::string(db_name)],
            optional_arguments: HashMap::new()
        }
    }

    pub fn db_list() -> ReQL {
        ReQL::Term {
            command: Term_TermType::DB_LIST,
            arguments: Vec::new(),
            optional_arguments: HashMap::new()
        }
    }

    pub fn expr(expression: Datum) -> ReQL {
        ReQL::Datum(expression)
    }

    pub fn db(db_name: &str) -> ReQL {
        ReQL::Term {
            command: Term_TermType::DB,
            arguments: vec![ReQL::string(db_name)],
            optional_arguments: HashMap::new()
        }
    }

    pub fn table(table_name: &str) -> ReQL {
        ReQL::Term {
            command: Term_TermType::TABLE,
            arguments: vec![ReQL::string(table_name)],
            optional_arguments: HashMap::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::rethink;

    use std::collections::hash_map::HashMap;

    use ql2::Term_TermType;
    use ql2::Response_ResponseType;

    use datum::Datum;
    use query::ReQL;

    extern crate rand;
    use self::rand::Rng;

    #[test]
    fn serialize_reql() {
        let mut options = HashMap::new();
        options.insert("bar".to_string(), Datum::String("hello".to_string()));
        options.insert("baz".to_string(), Datum::Bool(true));

        let reql = ReQL::Term {
            command: Term_TermType::DATUM,
            arguments: vec![ReQL::string("foo")],
            optional_arguments: options
        };

        let serialized = reql.serialize();
        println!("{}", serialized);
        assert!(serialized == r##"[1,["foo"],{"bar":"hello","baz":true}]"## ||
                serialized == r##"[1,["foo"],{"baz":true,"bar":"hello"}]"##)
    }

    #[test]
    fn it_works() {
      let mut conn = rethink::connect_default().unwrap();
      println!("{}", conn.send(r#"[1,[39,[[15,[[14,["blog"]],"users"]],{"name":"Michel"}]],{}]"#).unwrap());
      // println!("{}", conn.send(r#"[1,"foo",{}]"#).unwrap());
      // panic!("ASDF");
    }

    #[test]
    fn connect_disconnect() {
        let mut conn = rethink::connect_default().unwrap();
        assert!(conn.is_open());

        conn.close();
        assert!(!conn.is_open());

        conn.connect().unwrap();
        assert!(conn.is_open());

        conn.reconnect().unwrap();
        assert!(conn.is_open());
    }

    #[test]
    fn test_expr() {
        let conn = rethink::connect_default().unwrap();
        assert_eq!(rethink::expr(Datum::String("foo".to_string())).serialize_query_for_connection(&conn),
                   r##"[1,"foo",{}]"##)
    }

    #[test]
    fn test_table() {
        let conn = rethink::connect_default().unwrap();

        let tablename = "__test_tablename";
        let dbname = "__test_dbname";

        assert_eq!(rethink::table(tablename).serialize_query_for_connection(&conn),
                   format!("[1,[15,[\"{}\"]],{{}}]", tablename));

        assert_eq!(rethink::db(dbname).table(tablename).serialize_query_for_connection(&conn),
                   format!("[1,[15,[[14,[\"{}\"]],\"{}\"]],{{}}]", dbname, tablename))
    }

    #[test]
    fn use_default_db() {
        let mut conn = rethink::connect_default().unwrap();
        assert!(matches!(conn.default_db(), &None));

        conn.use_(Some("other_name"));
        assert!(matches!(conn.default_db(), &Some(ref db_name) if db_name == "other_name"));

        conn.use_(None);
        assert!(matches!(conn.default_db(), &None));
    }

    #[test]
    fn sends_default_db() {
        let mut conn = rethink::connect_default().unwrap();
        conn.use_(Some("default_db_name"));
        assert_eq!(rethink::expr(Datum::String("foo".to_string())).serialize_query_for_connection(&conn),
                   r##"[1,"foo",{"db":[14,["default_db_name"]]}]"##)
    }

    #[test]
    fn test_db() {
        let mut conn = rethink::connect_default().unwrap();
        let resp = rethink::db("test").run(&mut conn).unwrap();
        assert_eq!(resp.response_type, Response_ResponseType::RUNTIME_ERROR);
        assert_eq!(resp.result.first().unwrap(), &Datum::String("Query result must be of type DATUM, GROUPED_DATA, or STREAM (got DATABASE).".to_string()));
    }

    #[test]
    fn create_db() {
        let mut conn = rethink::connect_default().unwrap();
        rethink::db_drop("db_create_test").run(&mut conn).unwrap();
        let res = rethink::db_create("db_create_test").run(&mut conn).unwrap();
        match res.response_type {
            Response_ResponseType::SUCCESS_ATOM => {
                match res.result.first().unwrap() {
                    &Datum::Object(ref o) => {
                        let json_create_count = o.get("dbs_created").unwrap();
                        match json_create_count {
                            &Datum::Number(n) => assert!(n.floor() as u64 == 1),
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
        let mut conn = rethink::connect_default().unwrap();
        rethink::db_create("db_drop_test").run(&mut conn).unwrap();
        let res = rethink::db_drop("db_drop_test").run(&mut conn).unwrap();
        match res.response_type {
            Response_ResponseType::SUCCESS_ATOM => {
                match res.result.first().unwrap() {
                    &Datum::Object(ref o) => {
                        let json_create_count = o.get("dbs_dropped").unwrap();
                        match json_create_count {
                            &Datum::Number(n) => assert!(n.floor() as u64 == 1),
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
    fn list_db() {
        let mut conn = rethink::connect_default().unwrap();
        rethink::db_create("db_list_test1").run(&mut conn).unwrap();
        rethink::db_create("db_list_test2").run(&mut conn).unwrap();
        rethink::db_create("db_list_test3").run(&mut conn).unwrap();
        let res = rethink::db_list().run(&mut conn).unwrap();
        match res.result.first().unwrap() {
            &Datum::Array(ref db_names) => assert!(db_names.contains(&Datum::String("db_list_test1".to_string()))),
            _ => panic!("Expected an array of database names")
        }
    }

    #[test]
    fn test_insert_get() {
        let mut conn = rethink::connect_default().unwrap();

        let mut rng = rand::thread_rng();
        let key = rng.next_u64().to_string();

        let table_query = rethink::db("test").table("test_table");

        let value = Datum::from_str(&format!(r###"{{"id": "{}", "value": 42}}"###, key));
        let insert_result = table_query.insert(&value, None).run(&mut conn).unwrap();
        // println!("serialized: {}", table_query.insert(&value, Some(Durability::Soft)).serialize_query_for_connection(&conn));
        assert!(insert_result.response_type == Response_ResponseType::SUCCESS_ATOM);
        match insert_result.result.first().unwrap() {
            &Datum::Object(ref o) => {
                if let &Datum::Number(n) = o.get("inserted").unwrap() {
                    assert_eq!(n as u64, 1)
                } else {
                    panic!("Unexpected type of \"inserted\" metadata")
                }
            },
            _ => panic!("Unexpected type in response")
        }

        let get_result = table_query.get(&key).run(&mut conn).unwrap();
        assert!(get_result.response_type == Response_ResponseType::SUCCESS_ATOM);
        assert_eq!(*get_result.result.first().unwrap(), value)
    }
}
