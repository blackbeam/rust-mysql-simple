rust-mysql-simple [![Build Status](https://travis-ci.org/blackbeam/rust-mysql-simple.png?branch=master)](https://travis-ci.org/blackbeam/rust-mysql-simple)
=================
Mysql client library implemented in rust **nightly**. Feel free to open a new issues and pull requests.

### Documentation
Latest crate API docs hosted on [rust-ci.org](http://www.rust-ci.org/blackbeam/rust-mysql-simple/doc/mysql/).

### Simple example
Just a copy of example from rust-postgres.
```rust
extern crate mysql;
extern crate time;

use time::Timespec;

use mysql::conn::{MyOpts};
use mysql::conn::pool::{MyPool};
use mysql::value::{from_value};
use std::default::{Default};

struct Person {
    id: i32,
    name: String,
    time_created: Timespec,
    data: Option<Vec<u8>>
}

fn main() {
    let opts = MyOpts{user: Some("root".to_string()), ..Default::default()};
    let pool = MyPool::new(opts).unwrap();

    pool.query("CREATE DATABASE IF NOT EXISTS person");
    pool.query("USE person");
    pool.query("CREATE TABLE person(
                  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  name TEXT,
                  time_created TIMESTAMP,
                  data BLOB
                );");

    // Just for clarification my name is not Steven.
    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        time_created: time::get_time(),
        data: None
    };

    pool.prepare("INSERT INTO person (name, time_created, data)
                  VALUES (?, ?, ?);")
    .and_then(|mut stmt| {
        stmt.execute([&me.name, &me.time_created, &me.data]);
        Ok(())
    });

    let mut stmt = pool.prepare("SELECT id, name, time_created, data
                                 FROM person").unwrap();

    for row in &mut stmt.execute([]) {
        let row = row.unwrap();
        let person = Person {
            id: from_value(row.get(0)),
            name: from_value(row.get(1)),
            time_created: from_value(row.get(2)),
            data: from_value(row.get(3))
        };
        println!("Found person {}", person.name);
    }

    pool.query("DROP DATABASE IF EXISTS person");
}
```
