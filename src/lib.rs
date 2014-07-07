#![crate_name="mysql"]	
#![comment="Mysql client library writen in rust"]
#![license="MIT"]
#![crate_type="rlib"]
#![crate_type="dylib"]

#![feature(unsafe_destructor)]
#![feature(phase)]


#![allow(dead_code)]
#![feature(macro_rules)]

#[cfg(test)]
extern crate test;
extern crate sync;
extern crate core;
extern crate debug;
extern crate time;

#[phase(plugin)]
extern crate lazy_static;

mod scramble;
pub mod consts;
pub mod error;
mod packet;
mod io;
pub mod value;
pub mod conn;
