//! # rust-mysql-simple
//!
//! **Crate name:** mysql
//!
//! This create offers:
//!
//! *   MySql database driver in pure rust;
//! *   connection pool.
//!
//! Features:
//!
//! *   macOS, Windows and Linux support;
//! *   TLS support via **nativetls** create;
//! *   MySql text protocol support, i.e. support of simple text queries and text result sets;
//! *   MySql binary protocol support, i.e. support of prepared statements and binary result sets;
//! *   support of multi-result sets;
//! *   support of named parameters for prepared statements;
//! *   optional per-connection cache of prepared statements;
//! *   support of MySql packets larger than 2^24;
//! *   support of Unix sockets and Windows named pipes;
//! *   support of custom LOCAL INFILE handlers;
//! *   support of MySql protocol compression;
//! *   support of auth plugins:
//!     *   **mysql_native_password** - for MySql prior to v8;
//!     *   **caching_sha2_password** - for MySql v8 and higher.
//!
//! ## Installation
//!
//! Put the desired version of the crate into the `dependencies` section of your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! mysql = "*"
//! ```
//!
//! ## Example
//!
//! ```rust
//! # mysql::doctest_wrapper!(__result, {
//! use mysql::*;
//! use mysql::prelude::*;
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Payment {
//!     customer_id: i32,
//!     amount: i32,
//!     account_name: Option<String>,
//! }
//!
//! let url = "mysql://root:password@localhost:3307/db_name";
//! # let url = get_opts();
//!
//! let pool = Pool::new(url)?;
//!
//! let mut conn = pool.get_conn()?;
//!
//! // Let's create a table for payments.
//! conn.query_drop(
//!     r"CREATE TEMPORARY TABLE payment (
//!         customer_id int not null,
//!         amount int not null,
//!         account_name text
//!     )")?;
//!
//! let payments = vec![
//!     Payment { customer_id: 1, amount: 2, account_name: None },
//!     Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
//!     Payment { customer_id: 5, amount: 6, account_name: None },
//!     Payment { customer_id: 7, amount: 8, account_name: None },
//!     Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
//! ];
//!
//! // Now let's insert payments to the database
//! conn.exec_batch(
//!     r"INSERT INTO payment (customer_id, amount, account_name)
//!       VALUES (:customer_id, :amount, :account_name)",
//!     payments.iter().map(|p| params! {
//!         "customer_id" => p.customer_id,
//!         "amount" => p.amount,
//!         "account_name" => &p.account_name,
//!     })
//! )?;
//!
//! // Let's select payments from database. Type inference should do the trick here.
//! let selected_payments = conn
//!     .query_map(
//!         "SELECT customer_id, amount, account_name from payment",
//!         |(customer_id, amount, account_name)| {
//!             Payment { customer_id, amount, account_name }
//!         },
//!     )?;
//!
//! // Let's make sure, that `payments` equals to `selected_payments`.
//! // Mysql gives no guaranties on order of returned rows
//! // without `ORDER BY`, so assume we are lucky.
//! assert_eq!(payments, selected_payments);
//! println!("Yay!");
//! # });
//! ```
//!
//! ## API Documentation
//!
//! Please refer to the [crate docs].
//!
//! ## Basic structures
//!
//! ### `Opts`
//!
//! This structure holds server host name, client username/password and other settings,
//! that controls client behavior.
//!
//! #### URL-based connection string
//!
//! Note, that you can use URL-based connection string as a source of an `Opts` instance.
//! URL schema must be `mysql`. Host, port and credentials, as well as query parameters,
//! should be given in accordance with the RFC 3986.
//!
//! Examples:
//!
//! ```rust
//! # mysql::doctest_wrapper!(__result, {
//! # use mysql::Opts;
//! let _ = Opts::from_url("mysql://localhost/some_db")?;
//! let _ = Opts::from_url("mysql://[::1]/some_db")?;
//! let _ = Opts::from_url("mysql://user:pass%20word@127.0.0.1:3307/some_db?")?;
//! # });
//! ```
//!
//! Supported URL parameters (for the meaning of each field please refer to the docs on `Opts`
//! structure in the create API docs):
//!
//! *   `prefer_socket: true | false` - defines the value of the same field in the `Opts` structure;
//! *   `tcp_keepalive_time_ms: u32` - defines the value (in milliseconds)
//!     of the `tcp_keepalive_time` field in the `Opts` structure;
//! *   `tcp_connect_timeout_ms: u64` - defines the value (in milliseconds)
//!     of the `tcp_connect_timeout` field in the `Opts` structure;
//! *   `stmt_cache_size: u32` - defines the value of the same field in the `Opts` structure;
//! *   `compress` - defines the value of the same field in the `Opts` structure.
//!     Supported value are:
//!     *  `true` - enables compression with the default compression level;
//!     *  `fast` - enables compression with "fast" compression level;
//!     *  `best` - enables compression with "best" compression level;
//!     *  `1`..`9` - enables compression with the given compression level.
//! *   `socket` - socket path on UNIX, or pipe name on Windows.
//!
//! ### `OptsBuilder`
//!
//! It's a convenient builder for the `Opts` structure. It defines setters for fields
//! of the `Opts` structure.
//!
//! ```no_run
//! # mysql::doctest_wrapper!(__result, {
//! # use mysql::*;
//! let opts = OptsBuilder::new()
//!     .user(Some("foo"))
//!     .db_name(Some("bar"));
//! let _ = Conn::new(opts)?;
//! # });
//! ```
//!
//! ### `Conn`
//!
//! This structure represents an active MySql connection. It also holds statement cache
//! and metadata for the last result set.
//!
//! ### `Transaction`
//!
//! It's a simple wrapper on top of a routine, that starts with `START TRANSACTION`
//! and ends with `COMMIT` or `ROLBACK`.
//!
//! ### `Pool`
//!
//! It's a reference to a connection pool, that can be cloned and shared between threads.
//!
//! ### `Statement`
//!
//! Statement, actually, is just an identifier coupled with statement metadata, i.e an information
//! about its parameters and columns. Internally the `Statement` structure also holds additional
//! data required to support named parameters (see bellow).
//!
//! ### `Value`
//!
//! This enumeration represents the raw value of a MySql cell. Library offers conversion between
//! `Value` and different rust types via `FromValue` trait described below.
//!
//! #### `FromValue` trait
//!
//! This trait is reexported from **mysql_common** create. Please refer to its
//! [crate docs][mysql_common docs] for the list of supported conversions.
//!
//! Trait offers conversion in two flavours:
//!
//! *   `from_value(Value) -> T` - convenient, but panicking conversion.
//!
//!     Note, that for any variant of `Value` there exist a type, that fully covers its domain,
//!     i.e. for any variant of `Value` there exist `T: FromValue` such that `from_value` will never
//!     panic. This means, that if your database schema is known, than it's possible to write your
//!     application using only `from_value` with no fear of runtime panic.
//!
//! *   `from_value_opt(Value) -> Option<T>` - non-panicking, but less convenient conversion.
//!
//!     This function is useful to probe conversion in cases, where source database schema
//!     is unknown.
//!
//! ### `Row`
//!
//! Internally `Row` is a vector of `Value`s, that also allows indexing by a column name/offset,
//! and stores row metadata. Library offers conversion between `Row` and sequences of Rust types
//! via `FromRow` trait described below.
//!
//! #### `FromRow` trait
//!
//! This trait is reexported from **mysql_common** create. Please refer to its
//! [crate docs][mysql_common docs] for the list of supported conversions.
//!
//! This conversion is based on the `FromValue` and so comes in two similar flavours:
//!
//! *   `from_row(Row) -> T` - same as `from_value`, but for rows;
//! *   `from_row_opt(Row) -> Option<T>` - same as `from_value_opt`, but for rows.
//!
//! ### `Params`
//!
//! Represents parameters of a prepared statement, but this type won't appear directly in your code
//! because binary protocol API will ask for `T: Into<Params>`, where `Into<Params>` is implemented:
//!
//! *    for tuples of `Into<Value>` types up to arity 12, where empty tuple is for statements
//!      without parameters, and unary tuple requires extra comma (e.g. `(val,)`);
//! *    for `Vec<T: Into<Value>>` for cases, when your statement takes more than 12 parameters;
//! *    for named parameters representation (the value of the `params!` macro, described below).
//!
//! **Note:** Please refer to the [**mysql_common** crate docs][mysql_common docs] for the list
//! of types, that implements `Into<Value>`.
//!
//! #### `Serialized`, `Deserialized`
//!
//! Wrapper structures for cases, when you need to provide a value for a JSON cell,
//! or when you need to parse JSON cell as a struct.
//!
//! ```rust
//! # #[macro_use] extern crate serde_derive;
//! # mysql::doctest_wrapper!(__result, {
//! # use mysql::*;
//! #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! struct Example {
//!     foo: u32,
//! }
//!
//! let value = Value::from(Serialized(Example { foo: 42 }));
//! assert_eq!(value, Value::from(r#"{"foo":42}"#));
//!
//! let structure: Deserialized<Example> = from_value(value);
//! assert_eq!(structure, Deserialized(Example { foo: 42 }));
//! # });
//! ```
//!
//! ### `QueryResult`
//!
//! It's an iterator over rows of a query result with support of multi-result sets. It's intended
//! for cases when you need full control during result set iteration. For other cases `Conn`
//! provides a set of methods that will immediately costume the first result set and drop everything
//! else.
//!
//! This iterator is lazy so it won't read the result from server until you iterate over it.
//! MySql protocol is strictly sequential, so `Conn` will be mutably borrowed until the result
//! is fully consumed.
//!
//! ## Text protocol
//!
//! MySql text protocol is implemented in the set of `Conn::query*` methods. It's useful when your
//! query doesn't have parameters.
//!
//! **Note:** All values of a text protocol result set will be encoded as strings by the server,
//! so `from_value` conversion may lead to additional parsing costs.
//!
//! Examples:
//!
//! ```rust
//! # mysql::doctest_wrapper!(__result, {
//! # use mysql::*;
//! # use mysql::prelude::*;
//! let pool = Pool::new(get_opts())?;
//! let val = pool.get_conn()?.query_first("SELECT POW(2, 16)")?;
//!
//! assert_eq!(val, Some(Value::Bytes("65536".as_bytes().to_vec())));
//! # });
//! ```
//!
//! ## Binary protocol
//!
//! MySql binary protocol is implemented in the set of `Conn::exec*` methods. It's useful when your
//! query have parameters. MySql uses `?` symbol as a parameter placeholder and it's only possible
//! to use parameters where a single MySql value is expected. For example:
//!
//! ```rust
//! # mysql::doctest_wrapper!(__result, {
//! # use mysql::*;
//! # use mysql::prelude::*;
//! let pool = Pool::new(get_opts())?;
//! let val = pool.get_conn()?.exec_first("SELECT POW(?, ?)", (2, 16))?;
//!
//! assert_eq!(val, Some(Value::UInt(65536)));
//! # });
//! ```
//!
//! ### Statements
//!
//! In mysql each prepared statement belongs to a particular connection and can't be executed
//! on another connection. Trying to do so will lead to an error. Library won't tie statement
//! to a connection in any way, but one can look on to the connection id, that is stored
//! in the `Statement` structure.
//!
//! ```rust
//! # mysql::doctest_wrapper!(__result, {
//! # use mysql::*;
//! # use mysql::prelude::*;
//! let pool = Pool::new(get_opts())?;
//!
//! let mut conn_1 = pool.get_conn()?;
//! let mut conn_2 = pool.get_conn()?;
//!
//! let stmt_1 = conn_1.prep("SELECT ?")?;
//!
//! assert!(stmt_1.connection_id() == conn_1.connection_id());
//! assert!(stmt_1.connection_id() != conn_2.connection_id());
//!
//! assert!(conn_1.exec_drop(&stmt_1, ("foo",)).is_ok());
//! assert!(conn_2.exec_drop(&stmt_1, ("foo",)).is_err());
//! # });
//! ```
//!
//! ### Statement cache
//!
//! `Conn` will manage the cache of prepared statements on the client side, so subsequent calls
//! to prepare with the same statement won't lead to a client-server roundtrip. Cache size
//! for each connection is determined by the `stmt_cache_size` field of the `Opts` structure.
//! Statements, that are out of this boundary will be closed in LRU order.
//!
//! Statement cache is completely disabled if `stmt_cache_size` is zero.
//!
//! **Caveats:**
//!
//! *   disabled statement cache means, that you have to close statements yourself using
//!     `Conn::close`, or they'll exhaust server limits/resources;
//!
//! *   you should be aware of the [`max_prepared_stmt_count`][max_prepared_stmt_count]
//!     option of the MySql server. If the number of active connections times the value
//!     of `stmt_cache_size` is greater, than you could receive an error while prepareing
//!     another statement.
//!
//! ### Named parameters
//!
//! MySql itself doesn't have named parameters support, so it's implemented on the client side.
//! One should use `:name` as a placeholder syntax for a named parameter.
//!
//! Named parameters may be repeated within the statement, e.g `SELECT :foo :foo` will require
//! a single named parameter `foo` that will be repeated on the corresponding positions during
//! statement execution.
//!
//! One should use the `params!` macro to build a parameters for execution.
//!
//! **Note:** Positional and named parameters can't be mixed within the single statement.
//!
//! Examples:
//!
//! ```rust
//! # mysql::doctest_wrapper!(__result, {
//! # use mysql::*;
//! # use mysql::prelude::*;
//! let pool = Pool::new(get_opts())?;
//!
//! let mut conn = pool.get_conn()?;
//! let stmt = conn.prep("SELECT :foo, :bar, :foo")?;
//!
//! let foo = 42;
//!
//! let val_13 = conn.exec_first(&stmt, params! { "foo" => 13, "bar" => foo })?.unwrap();
//! // Short syntax is available when param name is the same as variable name:
//! let val_42 = conn.exec_first(&stmt, params! { foo, "bar" => 13 })?.unwrap();
//!
//! assert_eq!((foo, 13, foo), val_42);
//! assert_eq!((13, foo, 13), val_13);
//! # });
//! ```
//!
//! [crate docs]: https://docs.rs/mysql
//! [mysql_common docs]: https://docs.rs/mysql_common
//! [max_prepared_stmt_count]: https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_prepared_stmt_count
//!

#![crate_name = "mysql"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]
#![cfg_attr(feature = "nightly", feature(test, const_fn))]
#[cfg(feature = "nightly")]
extern crate test;

use mysql_common as myc;
pub extern crate serde;
pub extern crate serde_json;
#[cfg(test)]
#[macro_use]
extern crate serde_derive;

/// Reexport of `chrono` crate.
pub use crate::myc::chrono;
/// Reexport of `time` crate.
pub use crate::myc::time;
/// Reexport of `uuid` crate.
pub use crate::myc::uuid;

mod conn;
pub mod error;
mod io;
mod queryable;

#[doc(inline)]
pub use crate::myc::constants as consts;

#[doc(inline)]
pub use crate::conn::local_infile::{LocalInfile, LocalInfileHandler};
#[doc(inline)]
pub use crate::conn::opts::SslOpts;
#[doc(inline)]
pub use crate::conn::opts::{Opts, OptsBuilder, DEFAULT_STMT_CACHE_SIZE};
#[doc(inline)]
pub use crate::conn::pool::{Pool, PooledConn};
#[doc(inline)]
pub use crate::conn::query_result::QueryResult;
#[doc(inline)]
pub use crate::conn::stmt::Statement;
#[doc(inline)]
pub use crate::conn::transaction::{IsolationLevel, Transaction};
#[doc(inline)]
pub use crate::conn::Conn;
#[doc(inline)]
pub use crate::error::{DriverError, Error, MySqlError, Result, ServerError, UrlError};
#[doc(inline)]
pub use crate::myc::packets::Column;
#[doc(inline)]
pub use crate::myc::params::Params;
#[doc(inline)]
pub use crate::myc::proto::codec::Compression;
#[doc(inline)]
pub use crate::myc::row::convert::{from_row, from_row_opt, FromRowError};
#[doc(inline)]
pub use crate::myc::row::Row;
#[doc(inline)]
pub use crate::myc::value::convert::{from_value, from_value_opt, FromValueError};
#[doc(inline)]
pub use crate::myc::value::json::{Deserialized, Serialized};
#[doc(inline)]
pub use crate::myc::value::Value;

pub mod prelude {
    #[doc(inline)]
    pub use crate::myc::row::convert::FromRow;
    #[doc(inline)]
    pub use crate::myc::row::ColumnIndex;
    #[doc(inline)]
    pub use crate::myc::value::convert::{ConvIr, FromValue, ToValue};
    #[doc(inline)]
    pub use crate::queryable::{AsStatement, Queryable};
}

#[doc(inline)]
pub use crate::myc::params;

#[doc(hidden)]
#[macro_export]
macro_rules! def_database_url {
    () => {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            let opts = $crate::Opts::from_url(&url).expect("DATABASE_URL invalid");
            if opts
                .get_db_name()
                .expect("a database name is required")
                .is_empty()
            {
                panic!("database name is empty");
            }
            url
        } else {
            "mysql://root:password@127.0.0.1:3307/mysql".into()
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! def_get_opts {
    () => {
        pub fn test_ssl() -> bool {
            let ssl = std::env::var("SSL").ok().unwrap_or("false".into());
            ssl == "true" || ssl == "1"
        }

        pub fn test_compression() -> bool {
            let compress = std::env::var("COMPRESS").ok().unwrap_or("false".into());
            compress == "true" || compress == "1"
        }

        pub fn get_opts() -> $crate::OptsBuilder {
            let database_url = $crate::def_database_url!();
            let mut builder = $crate::OptsBuilder::from_opts(&*database_url)
                .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
            if test_compression() {
                builder = builder.compress(Some(Default::default()));
            }
            if test_ssl() {
                let ssl_opts = $crate::SslOpts::default()
                    .with_danger_skip_domain_validation(true)
                    .with_danger_accept_invalid_certs(true);
                builder = builder
                    .prefer_socket(false)
                    .ssl_opts(ssl_opts);
            }
            builder
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! doctest_wrapper {
    ($body:block) => {
        fn main() {
            $crate::def_get_opts!();
            $body;
        }
    };
    (__result, $body:block) => {
        fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
            $crate::def_get_opts!();
            Ok($body)
        }
    };
}

#[cfg(test)]
mod test_misc {
    use lazy_static::lazy_static;

    use crate::{def_database_url, def_get_opts};

    #[allow(dead_code)]
    fn error_should_implement_send_and_sync() {
        fn _dummy<T: Send + Sync>(_: T) {}
        _dummy(crate::error::Error::FromValueError(crate::Value::NULL));
    }

    lazy_static! {
        pub static ref DATABASE_URL: String = def_database_url!();
    }

    def_get_opts!();
}
