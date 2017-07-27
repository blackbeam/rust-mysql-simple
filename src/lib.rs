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
//!     let pool = my::Pool::new("mysql://root:password@localhost:3307").unwrap();
//! #       drop(pool);
//! #       my::Pool::new_manual(1, 1, "mysql://root:password@localhost:3307").unwrap()
//! #   } else {
//! #       let mut builder = my::OptsBuilder::default();
//! #       builder.user(Some(user))
//! #              .pass(Some(pwd))
//! #              .ip_or_hostname(Some(addr))
//! #              .tcp_port(port);
//! #       my::Pool::new_manual(1, 1, builder).unwrap()
//! #   };
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
//!     pool.prep_exec("SELECT customer_id, amount, account_name from tmp.payment", ())
//!     .map(|result| { // In this closure we will map `QueryResult` to `Vec<Payment>`
//!         // `QueryResult` is iterator over `MyResult<row, err>` so first call to `map`
//!         // will map each `MyResult` to contained `row` (no proper error handling)
//!         // and second call to `map` will map each `row` to `Payment`
//!         result.map(|x| x.unwrap()).map(|row| {
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

#![crate_name="mysql"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![cfg_attr(feature = "nightly", feature(test, const_fn, drop_types_in_const))]
#[cfg(feature = "nightly")]
extern crate test;

extern crate bit_vec;
#[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
extern crate openssl;
#[cfg(all(feature = "ssl", target_os = "macos"))]
extern crate security_framework;
extern crate regex;
#[cfg(unix)]
extern crate libc;
#[cfg(unix)]
extern crate nix;
extern crate net2;
extern crate byteorder;
extern crate mysql_common as myc;
#[cfg(windows)]
extern crate named_pipe;
extern crate url;
extern crate bufstream;
extern crate fnv;
extern crate twox_hash;
extern crate smallvec;
#[cfg(target_os = "windows")]
extern crate winapi;
#[cfg(target_os = "windows")]
extern crate ws2_32;

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
pub use myc::chrono;
/// Reexport of `time` crate.
pub use myc::time;
/// Reexport of `uuid` crate.
pub use myc::uuid;

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
        (::std::string::String::from($name), $crate::Value::from($value))
    );
    (@to_pair $name:ident) => (
        (::std::string::String::from(stringify!($name)), $crate::Value::from($name))
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
    ($($tail:tt)*) => {
        {
            let mut output = vec![];
            params!(@expand output; $($tail)*);
            output
        }
    };
}

mod scramble;
pub mod error;
mod packet;
mod io;
mod conn;


#[doc(inline)]
pub use myc::constants as consts;

#[doc(inline)]
pub use conn::Conn;
#[doc(inline)]
pub use conn::LocalInfile;
#[doc(inline)]
pub use conn::LocalInfileHandler;
#[doc(inline)]
pub use conn::IsolationLevel;
#[doc(inline)]
pub use conn::Opts;
#[doc(inline)]
pub use conn::OptsBuilder;
#[doc(inline)]
pub use conn::QueryResult;
#[doc(inline)]
pub use conn::Stmt;
#[doc(inline)]
pub use conn::Transaction;
#[doc(inline)]
pub use conn::pool::Pool;
#[doc(inline)]
pub use conn::pool::PooledConn;
#[doc(inline)]
pub use error::DriverError;
#[doc(inline)]
pub use error::Error;
#[doc(inline)]
pub use error::MySqlError;
#[doc(inline)]
pub use error::Result;
#[doc(inline)]
pub use error::ServerError;
#[doc(inline)]
pub use error::UrlError;
#[doc(inline)]
pub use myc::packets::Column;
#[doc(inline)]
pub use myc::row::Row;
#[doc(inline)]
pub use myc::row::convert::{
    FromRowError,
    from_row,
    from_row_opt
};
#[doc(inline)]
pub use myc::params::Params;
#[doc(inline)]
pub use myc::value::Value;
#[doc(inline)]
pub use myc::value::convert::{
    FromValueError,
    from_value,
    from_value_opt,
};
#[doc(inline)]
pub use myc::value::json::Serialized;
#[doc(inline)]
pub use myc::value::json::Deserialized;

pub mod prelude {
    #[doc(inline)]
    pub use myc::value::convert::{
        ConvIr,
        FromValue,
        ToValue,
    };
    #[doc(inline)]
    pub use myc::row::convert::{
        FromRow,
    };
    #[doc(inline)]
    pub use conn::GenericConnection;
}

#[cfg(test)]
#[test]
fn params_macro_test() {
    let foo = 42;
    let bar = "bar";
    assert_eq!(
        vec![(String::from("foo"), Value::Int(42))],
        params! { foo }
    );
    assert_eq!(
        vec![(String::from("foo"), Value::Int(42))],
        params! { foo, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { foo, bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { foo, bar, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { "foo" => foo, "bar" => bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { "foo" => foo, "bar" => bar, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { foo, "bar" => bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { "foo" => foo, bar }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { foo, "bar" => bar, }
    );
    assert_eq!(
        vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into()))
        ],
        params! { "foo" => foo, bar, }
    );
}
