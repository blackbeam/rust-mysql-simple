// Copyright (c) 2020 rust-mysql-common contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::env;

fn main() {
    let names = ["CARGO_CFG_TARGET_OS", "CARGO_CFG_TARGET_ARCH"];
    for name in &names {
        let value =
            env::var(name).expect(&format!("Could not get the environment variable {}", name));
        println!("cargo:rustc-env={}={}", name, value);
    }
}
