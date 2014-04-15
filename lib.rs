#![crate_id="rust-mysql-simple#0.9.0.0"]	
#![comment="Mysql client library writen in rust"]
#![license="MIT"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![allow(dead_code)]
#![feature(macro_rules)]

#![cfg(test)]
extern crate test;

pub mod consts;
pub mod sha1;
pub mod scramble;
pub mod conn;