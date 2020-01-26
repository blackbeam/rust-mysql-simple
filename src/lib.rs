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
//! #### Example
//!
//! ```rust
//! #[macro_use]
//! extern crate mysql;
//! // ...
//!
//! # fn main() {
//! use mysql as my;
//! use my::prelude::*;
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Payment {
//!     customer_id: i32,
//!     amount: i32,
//!     account_name: Option<String>,
//! }
//!
//! # fn get_opts() -> my::Opts {
//! #     let url = if let Ok(url) = std::env::var("DATABASE_URL") {
//! #         let opts = my::Opts::from_url(&url).expect("DATABASE_URL invalid");
//! #         if opts.get_db_name().expect("a database name is required").is_empty() {
//! #             panic!("database name is empty");
//! #         }
//! #         url
//! #     } else {
//! #         "mysql://root:password@127.0.0.1:3307/mysql".to_string()
//! #     };
//! #     my::Opts::from_url(&*url).unwrap()
//! # }
//!
//! fn main() {
//! #   let opts = get_opts();
//! #   if opts.get_tcp_port() == 3307 && opts.get_pass() == Some("password") {
//!     // See docs on the `OptsBuilder`'s methods for the list of options available via URL.
//!     let pool = my::Pool::new("mysql://root:password@localhost:3307/mysql").unwrap();
//! #   }
//! #   let pool = my::Pool::new_manual(1, 1, opts).unwrap();
//!
//!     let mut conn = pool.get_conn().unwrap();
//!     // Let's create payment table.
//!     // Unwrap just to make sure no error happened.
//!     conn.query_drop(r"CREATE TEMPORARY TABLE payment (
//!                          customer_id int not null,
//!                          amount int not null,
//!                          account_name text
//!                      )").unwrap();
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
//!     // We also assume that no error happened in `prep`.
//!     let stmt = conn.prep(r"INSERT INTO
//!                            payment (customer_id, amount, account_name)
//!                            VALUES (:customer_id, :amount, :account_name)")
//!         .unwrap();
//!     for p in payments.iter() {
//!         // `execute` takes ownership of `params`, so we'll pass account name by reference.
//!         // Unwrap each result just to make sure no errors happened.
//!         conn.exec_drop(&stmt, params! {
//!             "customer_id" => p.customer_id,
//!             "amount" => p.amount,
//!             "account_name" => &p.account_name,
//!         }).unwrap();
//!     }
//!
//!     // Let's select payments from database
//!     let selected_payments: Vec<Payment> = conn
//!         .query_map("SELECT customer_id, amount, account_name from payment", |row| {
//!             // ⚠️ Note that from_row will panic if you don't follow your schema
//!             let (customer_id, amount, account_name) = my::from_row(row);
//!             Payment { customer_id, amount, account_name }
//!         })
//!         .unwrap();
//!
//!     // Let's make sure, that `payments` equals to `selected_payments`.
//!     // Mysql gives no guaranties on order of returned rows
//!     // without `ORDER BY`, so assume we are lucky.
//!     assert_eq!(payments, selected_payments);
//!     println!("Yay!");
//! }
//! # main();
//! # }
//! ```

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
            let mut builder = $crate::OptsBuilder::from_opts(&*database_url);
            builder.init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
            if test_compression() {
                builder.compress(Some(Default::default()));
            }
            if test_ssl() {
                builder.prefer_socket(false);
                let mut ssl_opts = $crate::SslOpts::default();
                ssl_opts.set_danger_skip_domain_validation(true);
                ssl_opts.set_danger_accept_invalid_certs(true);
                builder.ssl_opts(ssl_opts);
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
