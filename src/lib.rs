//! ### rust-mysql-simple
//! Mysql client library implemented in rust nightly.
//!
//! #### Install
//! Please use *mysql* crate:
//!
//! ```toml
//! [dependencies]
//! mysql = "*"
//! ```
//!
//! rust-mysql-simple offer support of SSL via `ssl` cargo feature which is enabled by default.
//! If you have no plans to use SSL, then you should disable that feature to not to depend on rust-openssl:
//!
//! ```toml
//! [dependencies.mysql]
//! mysql = "*"
//! default-features = false
//! ```
//!
//! #### Use
//! You should start by creating [`MyOpts`](conn/struct.MyOpts.html) struct.
//!
//! Then you can create [`MyPool`](conn/pool/struct.MyPool.html) which should be
//! enough to work with mysql server.
//!
//! ##### Example
//!
//! ```rust
//! use std::default::Default;
//!
//! use mysql::conn::MyOpts;
//! use mysql::conn::pool::MyPool;
//! use mysql::value::from_row;
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Payment {
//!     customer_id: i32,
//!     amount: i32,
//!     account_name: Option<String>,
//! }
//!
//! fn main() {
//!     let opts = MyOpts {
//!           user: Some("root".to_string()),
//!           pass: Some("password".to_string()),
//! #         tcp_addr: Some("127.0.0.1".to_string()),
//! #         tcp_port: 3307,
//!           ..Default::default()
//!     };
//!     let pool = MyPool::new(opts).unwrap();
//!
//!     // Let's create payment table.
//!     // It is temporary so we do not need `tmp` database to exist.
//!     // Unwap just to make sure no error happened.
//!     pool.prep_exec(r"CREATE TEMPORARY TABLE tmp.payment (
//!                          customer_id int not null,
//!                          amount int not null,
//!                          account_name text
//!                      )", ()).unwrap();
//!
//!     let payments = vec![
//!         Payment { customer_id: 1, amount: 2, account_name: None },
//!         Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
//!         Payment { customer_id: 5, amount: 6, account_name: None },
//!         Payment { customer_id: 7, amount: 8, account_name: None },
//!         Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
//!     ];
//!
//!     // Let's insert payments to the database
//!     // We will use into_iter() because we do not need to map Stmt to anything else.
//!     // Also we assume that no error happened in `prepare`.
//!     for mut stmt in pool.prepare(r"INSERT INTO tmp.payment
//!                                        (customer_id, amount, account_name)
//!                                    VALUES
//!                                        (?, ?, ?)").into_iter() {
//!         for p in payments.iter() {
//!             // `execute` takes ownership of `params` so we pass account name by reference.
//!             // Unwrap each result just to make sure no errors happended.
//!             stmt.execute((p.customer_id, p.amount, &p.account_name)).unwrap();
//!         }
//!     }
//!
//!     // Let's select payments from database
//!     let selected_payments: Vec<Payment> =
//!     pool.prep_exec("SELECT customer_id, amount, account_name from tmp.payment", ())
//!     .map(|result| { // In this closure we sill map `QueryResult` to `Vec<Payment>`
//!         // `QueryResult` is iterator over `MyResult<row, err>` so first call to `map`
//!         // will map each `MyResult` to contained `row` (no proper error handling)
//!         // and second call to `map` will map each `row` to `Payment`
//!         result.map(|x| x.unwrap()).map(|row| {
//!             let (customer_id, amount, account_name) = from_row(row);
//!             Payment {
//!                 customer_id: customer_id,
//!                 amount: amount,
//!                 account_name: account_name,
//!             }
//!         }).collect() // Collect payments so now `QueryResult` is mapped to `Vec<Payment>`
//!     }).unwrap(); // Unwrap `Vec<Payment>`
//!
//!     // Now make shure that `payments` equals to `selected_payments`.
//!     // Mysql gives no guaranties on order of returned rows without `ORDER BY`
//!     // so assume we are lukky.
//!     assert_eq!(payments, selected_payments);
//!     println!("Yay!");
//! }
//! ```

#![crate_name="mysql"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![cfg_attr(feature = "nightly", feature(test))]
#[cfg(feature = "nightly")]
extern crate test;

extern crate time;
#[cfg(feature = "openssl")]
extern crate openssl;
extern crate regex;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bitflags;
extern crate byteorder;
#[cfg(feature = "socket")]
extern crate unix_socket;

mod scramble;
pub mod consts;
pub mod error;
mod packet;
mod io;
pub mod value;
pub mod conn;
