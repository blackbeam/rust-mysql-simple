//! rust-mysql-simple is a mysql client library implemented in rust nightly.
//!
//! ```rust
//! extern crate mysql;
//! extern crate time;
//!
//! use time::Timespec;
//!
//! use mysql::conn::{MyOpts};
//! use mysql::conn::pool::{MyPool};
//! use mysql::value::{from_value};
//! use std::default::{Default};
//!
//! #[deriving(PartialEq, Eq, Show, Clone)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     time_created: Timespec,
//!     data: Option<Vec<u8>>
//! }
//!
//! #[allow(unused_must_use)]
//! fn main() {
//!     let opts = MyOpts{user: Some("root".to_string()), ..Default::default()};
//!     let pool = MyPool::new(opts).unwrap();
//!
//!     pool.query("CREATE DATABASE IF NOT EXISTS person");
//!     pool.query("USE person");
//!     pool.query("CREATE TABLE person(
//!                   id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
//!                   name TEXT,
//!                   time_created TIMESTAMP,
//!                   data BLOB
//!                 );");
//!
//!     // Just for clarification my name is not Steven.
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".to_string(),
//!         time_created: time::get_time(),
//!         data: None
//!     };
//!
//!     pool.prepare("INSERT INTO person (name, time_created, data)
//!                   VALUES (?, ?, ?);")
//!     .and_then(|mut stmt| {
//!         stmt.execute(&[&me.name, &me.time_created, &me.data]).and(Ok(()))
//!     });
//!
//!     let mut stmt = pool.prepare("SELECT id, name, time_created, data
//!                                  FROM person").unwrap();
//!     pool.prepare("SELECT id, name, time_created, data FROM person")
//!     .and_then(|mut stmt| {
//!     	for row in &mut stmt.execute([]) {
//!	            let row = row.unwrap();
//!             let person = Person {
//!	                id: from_value(&row[0]),
//!                 name: from_value(&row[1]),
//!                 time_created: from_value(&row[2]),
//!                 data: from_value(&row[3])
//!             };
//!             assert_eq!(person, Person{id: 1, ..me.clone()});
//!         }
//!         Ok(())
//!     });
//!
//!     pool.query("DROP DATABASE IF EXISTS person");
//! }
//! ```
#![crate_name="mysql"]	
#![comment="Mysql client library writen in rust"]
#![license="MIT"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![feature(unsafe_destructor)]
#![feature(phase)]


#![allow(dead_code)]
#![feature(macro_rules)]

#[cfg(test)]
extern crate test;
extern crate sync;
extern crate core;
extern crate debug;
extern crate time;

#[phase(plugin)]
extern crate lazy_static;

mod scramble;
pub mod consts;
pub mod error;
mod packet;
mod io;
pub mod value;
pub mod conn;
