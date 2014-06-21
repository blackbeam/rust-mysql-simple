#![crate_id="rust-mysql-simple#0.9.0.0"]	
#![comment="Mysql client library writen in rust"]
#![license="MIT"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![feature(unsafe_destructor)]

#![allow(dead_code)]
#![feature(macro_rules)]

#[cfg(test)]
extern crate test;
extern crate sync;
extern crate core;
extern crate debug;

mod scramble;
pub mod consts;
pub mod error;
pub mod packet;
pub mod io;
pub mod value;
pub mod conn;
pub mod pool;