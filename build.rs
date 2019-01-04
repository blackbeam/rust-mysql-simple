use std::env;

fn main() {
    let names = ["CARGO_CFG_TARGET_OS", "CARGO_CFG_TARGET_ARCH"];
    for name in &names {
        let value =
            env::var(name).expect(&format!("Could not get the environment variable {}", name));
        println!("cargo:rustc-env={}={}", name, value);
    }
}
