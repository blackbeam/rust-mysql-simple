#![crate_id="rust-mysql-simple#0.9.0.0"]	
#![comment="Mysql client library writen in rust"]
#![license="MIT"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![allow(dead_code)]
#![feature(macro_rules)]

#![cfg(test)]
extern crate test;
extern crate sync;

pub mod pool;
pub mod consts;
pub mod scramble;
pub mod io;
pub mod conn;