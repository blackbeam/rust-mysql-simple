//! ### rust-mysql-simple
//! Mysql client library implemented in rust nightly.
//!
//! #### Install
//! Just include another `[dependencies.*]` section into your Cargo.toml:
//!
//! ```toml
//! [dependencies.mysql]
//! git = "https://github.com/blackbeam/rust-mysql-simple"
//! ```
//!
//! rust-mysql-simple offer support of SSL via `ssl` cargo feature which is enabled by default. If you have no plans to use SSL, then you should disable that feature to not to depend on rust-openssl:
//!
//! ```toml
//! [dependencies.mysql]
//! git = "https://github.com/blackbeam/rust-mysql-simple"
//! default-features = false
//! ```
//!
//! #### Use
//! You should start by creating [`MyOpts`](conn/struct.MyOpts.html) struct.
//!
//! Then you can create [`MyPool`](conn/pool/struct.MyPool.html) which should be
//! enough to work with mysql server.
#![crate_name="mysql"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![feature(unsafe_destructor)]
#![feature(phase)]
#![feature(slicing_syntax)]


#![allow(dead_code)]
#![feature(macro_rules)]

#[cfg(test)]
extern crate test;
extern crate core;
extern crate time;
#[cfg(feature = "openssl")]
extern crate openssl;
extern crate regex;
#[phase(plugin)]
extern crate regex_macros;

#[phase(plugin)]
extern crate lazy_static;

mod scramble;
pub mod consts;
pub mod error;
mod packet;
mod io;
pub mod value;
pub mod conn;
