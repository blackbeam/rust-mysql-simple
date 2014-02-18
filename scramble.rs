use std::vec;
use sha1::{Sha1, Digest};

pub fn scramble(scr: &[u8], password: &[u8]) -> ~[u8] {
    if password.len() == 0 {
        return ~[];
    }

    let mut sh = Sha1::new();

    let mut sha_pass = [0u8, ..20];
    sh.input(password);
    sh.result(sha_pass);
    sh.reset();

    let mut double_sha_pass = [0u8, ..20];
    sh.input(sha_pass);
    sh.result(double_sha_pass);
    sh.reset();

    let mut hash = [0u8, ..20];
    sh.input(scr);
    sh.input(double_sha_pass);
    sh.result(hash);
    sh.reset();

    let mut output = vec::from_elem(20, 0u8);
    let mut i = 0;
    while i < 20 {
        output[i] = sha_pass[i] ^ hash[i];
        i += 1;
    }

    output
}
