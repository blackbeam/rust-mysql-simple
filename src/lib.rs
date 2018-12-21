//! ### rust-mysql-simple
//! Mysql client library implemented in rust.
//!
//! #### Install
//! Please use *mysql* crate:
//!
//! ```toml
//! [dependencies]
//! mysql = "*"
//! ```
//!
//! rust-mysql-simple offers support of SSL via `ssl` cargo feature which is disabled by default.
//! Add `ssl` feature to enable:
//!
//! ```toml
//! [dependencies.mysql]
//! version = "*"
//! features = ["ssl"]
//! ```
//!
//! #### Windows support (since 0.18.0)
//!
//! Windows is supported but currently rust-mysql-simple has no support of SSL on Windows.
//!
//! #### Example
//!
//! ```rust
//! #[macro_use]
//! extern crate mysql;
//! // ...
//!
//! # fn main() {
//! use mysql as my;
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Payment {
//!     customer_id: i32,
//!     amount: i32,
//!     account_name: Option<String>,
//! }
//!
//! fn main() {
//! #   let user = "root";
//! #   let addr = "127.0.0.1";
//! #   let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
//! #                              .map(|my_port| my_port.parse::<u16>().ok().unwrap_or(3307))
//! #                              .unwrap_or(3307);
//! #   let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
//! #   let pool = if port == 3307 && pwd == "password" {
//!     // See docs on the `OptsBuilder`'s methods for the list of options available via URL.
//!     let pool = my::Pool::new("mysql://root:password@localhost:3307/mysql").unwrap();
//! #       drop(pool);
//! #       my::Pool::new_manual(1, 1, "mysql://root:password@localhost:3307/mysql").unwrap()
//! #   } else {
//! #       let mut builder = my::OptsBuilder::default();
//! #       builder.user(Some(user))
//! #              .pass(Some(pwd))
//! #              .ip_or_hostname(Some(addr))
//! #              .db_name("mysql".into())
//! #              .tcp_port(port);
//! #       my::Pool::new_manual(1, 1, builder).unwrap()
//! #   };
//!
//!     // Let's create payment table.
//!     // Unwrap just to make sure no error happened.
//!     pool.prep_exec(r"CREATE TEMPORARY TABLE payment (
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
//!     for mut stmt in pool.prepare(r"INSERT INTO payment
//!                                        (customer_id, amount, account_name)
//!                                    VALUES
//!                                        (:customer_id, :amount, :account_name)").into_iter() {
//!         for p in payments.iter() {
//!             // `execute` takes ownership of `params` so we pass account name by reference.
//!             // Unwrap each result just to make sure no errors happened.
//!             stmt.execute(params!{
//!                 "customer_id" => p.customer_id,
//!                 "amount" => p.amount,
//!                 "account_name" => &p.account_name,
//!             }).unwrap();
//!         }
//!     }
//!
//!     // Let's select payments from database
//!     let selected_payments: Vec<Payment> =
//!     pool.prep_exec("SELECT customer_id, amount, account_name from payment", ())
//!     .map(|result| { // In this closure we will map `QueryResult` to `Vec<Payment>`
//!         // `QueryResult` is iterator over `MyResult<row, err>` so first call to `map`
//!         // will map each `MyResult` to contained `row` (no proper error handling)
//!         // and second call to `map` will map each `row` to `Payment`
//!         result.map(|x| x.unwrap()).map(|row| {
//!             // ⚠️ Note that from_row will panic if you don't follow your schema
//!             let (customer_id, amount, account_name) = my::from_row(row);
//!             Payment {
//!                 customer_id: customer_id,
//!                 amount: amount,
//!                 account_name: account_name,
//!             }
//!         }).collect() // Collect payments so now `QueryResult` is mapped to `Vec<Payment>`
//!     }).unwrap(); // Unwrap `Vec<Payment>`
//!
//!     // Now make sure that `payments` equals to `selected_payments`.
//!     // Mysql gives no guaranties on order of returned rows without `ORDER BY`
//!     // so assume we are lukky.
//!     assert_eq!(payments, selected_payments);
//!     println!("Yay!");
//! }
//! # main();
//! # }
//! ```

#![crate_name = "mysql"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]
#![cfg_attr(feature = "nightly", feature(test, const_fn, drop_types_in_const))]
#[cfg(feature = "nightly")]
extern crate test;

use mysql_common as myc;
#[cfg(feature = "rustc_serialize")]
pub extern crate rustc_serialize;
#[cfg(not(feature = "rustc_serialize"))]
pub extern crate serde;
#[cfg(not(feature = "rustc_serialize"))]
pub extern crate serde_json;
#[cfg(all(test, not(feature = "rustc_serialize")))]
#[macro_use]
extern crate serde_derive;

/// Reexport of `chrono` crate.
pub use crate::myc::chrono;
/// Reexport of `time` crate.
pub use crate::myc::time;
/// Reexport of `uuid` crate.
pub use crate::myc::uuid;

// Until `macro_reexport` stabilisation.
/// This macro is a convenient way to pass named parameters to a statement.
///
/// ```ignore
/// let foo = 42;
/// conn.prep_exec("SELECT :foo, :foo2x", params! {
///     foo,
///     "foo2x" => foo * 2,
/// });
/// ```
#[macro_export]
macro_rules! params {
    () => {};
    (@to_pair $name:expr => $value:expr) => (
        (std::string::String::from($name), $crate::Value::from($value))
    );
    (@to_pair $name:ident) => (
        (std::string::String::from(stringify!($name)), $crate::Value::from($name))
    );
    (@expand $vec:expr;) => {};
    (@expand $vec:expr; $name:expr => $value:expr, $($tail:tt)*) => {
        $vec.push(params!(@to_pair $name => $value));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:expr => $value:expr $(, $tail:tt)*) => {
        $vec.push(params!(@to_pair $name => $value));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:ident, $($tail:tt)*) => {
        $vec.push(params!(@to_pair $name));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:ident $(, $tail:tt)*) => {
        $vec.push(params!(@to_pair $name));
        params!(@expand $vec; $($tail)*);
    };
    ($i:ident, $($tail:tt)*) => {
        {
            let mut output = std::vec::Vec::new();
            params!(@expand output; $i, $($tail)*);
            output
        }
    };
    ($i:expr => $($tail:tt)*) => {
        {
            let mut output = std::vec::Vec::new();
            params!(@expand output; $i => $($tail)*);
            output
        }
    };
    ($i:ident) => {
        {
            let mut output = std::vec::Vec::new();
            params!(@expand output; $i);
            output
        }
    }
}

mod conn;
pub mod error;
mod io;
mod packet;

#[doc(inline)]
pub use crate::myc::constants as consts;

#[doc(inline)]
pub use crate::conn::pool::Pool;
#[doc(inline)]
pub use crate::conn::pool::PooledConn;
#[doc(inline)]
pub use crate::conn::Conn;
#[doc(inline)]
pub use crate::conn::IsolationLevel;
#[doc(inline)]
pub use crate::conn::LocalInfile;
#[doc(inline)]
pub use crate::conn::LocalInfileHandler;
#[doc(inline)]
pub use crate::conn::Opts;
#[doc(inline)]
pub use crate::conn::OptsBuilder;
#[doc(inline)]
pub use crate::conn::QueryResult;
#[doc(inline)]
pub use crate::conn::Stmt;
#[doc(inline)]
pub use crate::conn::Transaction;
#[doc(inline)]
pub use crate::error::DriverError;
#[doc(inline)]
pub use crate::error::Error;
#[doc(inline)]
pub use crate::error::MySqlError;
#[doc(inline)]
pub use crate::error::Result;
#[doc(inline)]
pub use crate::error::ServerError;
#[doc(inline)]
pub use crate::error::UrlError;
#[doc(inline)]
pub use crate::myc::packets::Column;
#[doc(inline)]
pub use crate::myc::params::Params;
#[doc(inline)]
pub use crate::myc::row::convert::{from_row, from_row_opt, FromRowError};
#[doc(inline)]
pub use crate::myc::row::Row;
#[doc(inline)]
pub use crate::myc::value::convert::{from_value, from_value_opt, FromValueError};
#[doc(inline)]
pub use crate::myc::value::json::Deserialized;
#[doc(inline)]
pub use crate::myc::value::json::Serialized;
#[doc(inline)]
pub use crate::myc::value::Value;

pub mod prelude {
    #[doc(inline)]
    pub use crate::conn::GenericConnection;
    #[doc(inline)]
    pub use crate::myc::row::convert::FromRow;
    #[doc(inline)]
    pub use crate::myc::value::convert::{ConvIr, FromValue, ToValue};
}

#[cfg(test)]
#[test]
fn params_macro_test() {
    let foo = 42;
    let bar = "bar";
    assert_eq!(vec![(String::from("foo"), Value::Int(42))], params! { foo });
    assert_eq!(
        vec![(String::from("foo"), Value::Int(42))],
        params! { foo, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { foo, bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { foo, bar, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { "foo" => foo, "bar" => bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { "foo" => foo, "bar" => bar, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { foo, "bar" => bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { "foo" => foo, bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { foo, "bar" => bar, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ],
        params! { "foo" => foo, bar, }
    );
}
