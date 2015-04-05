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
#![feature(plugin)]
#![feature(core)]
#![feature(ip)]
#![feature(tcp)]
#![feature(collections)]
#![feature(std_misc)]
#![feature(slice_patterns)]
#![cfg_attr(test, feature(test))]

#[cfg(test)]
extern crate test;

extern crate core;
extern crate time;
#[cfg(feature = "openssl")]
extern crate openssl;
extern crate regex;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate unix_socket;

mod scramble;
pub mod consts;
pub mod error;
mod packet;
mod io;
pub mod value;
pub mod conn;
