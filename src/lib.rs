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

// Until `macro_reexport` stabilisation.

mod conn;
pub mod error;
mod io;

#[doc(inline)]
pub use crate::myc::constants as consts;

#[doc(inline)]
pub use crate::conn::local_infile::{LocalInfile, LocalInfileHandler};
#[doc(inline)]
pub use crate::conn::opts::SslOpts;
#[doc(inline)]
pub use crate::conn::opts::{Opts, OptsBuilder};
#[doc(inline)]
pub use crate::conn::pool::{Pool, PooledConn};
#[doc(inline)]
pub use crate::conn::query_result::QueryResult;
#[doc(inline)]
pub use crate::conn::stmt::Stmt;
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
    pub use crate::conn::GenericConnection;
    #[doc(inline)]
    pub use crate::myc::row::convert::FromRow;
    #[doc(inline)]
    pub use crate::myc::value::convert::{ConvIr, FromValue, ToValue};
}

#[doc(inline)]
pub use crate::myc::params;

#[cfg(test)]
mod test_misc {
    use lazy_static::lazy_static;

    use std::env;

    use crate::conn::opts::{Opts, OptsBuilder};
    use crate::SslOpts;

    #[allow(dead_code)]
    fn error_should_implement_send_and_sync() {
        fn _dummy<T: Send + Sync>(_: T) {}
        _dummy(crate::error::Error::FromValueError(crate::Value::NULL));
    }

    lazy_static! {
        pub static ref DATABASE_URL: String = {
            if let Ok(url) = env::var("DATABASE_URL") {
                let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
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

    pub fn get_opts() -> OptsBuilder {
        let mut builder = OptsBuilder::from_opts(&**DATABASE_URL);
        builder.init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
        if test_compression() {
            builder.compress(Default::default());
        }
        if test_ssl() {
            builder.prefer_socket(false);
            let mut ssl_opts = SslOpts::default();
            ssl_opts.set_danger_skip_domain_validation(true);
            ssl_opts.set_danger_accept_invalid_certs(true);
            builder.ssl_opts(ssl_opts);
        }
        builder
    }

    pub fn test_ssl() -> bool {
        let ssl = env::var("SSL").ok().unwrap_or("false".into());
        ssl == "true" || ssl == "1"
    }

    pub fn test_compression() -> bool {
        let compress = env::var("COMPRESS").ok().unwrap_or("false".into());
        compress == "true" || compress == "1"
    }
}
