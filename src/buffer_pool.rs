// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    mem::replace,
    ops::Deref,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct BufferPool {
    pool_cap: usize,
    buffer_cap: usize,
    pool: Mutex<Vec<Vec<u8>>>,
}

impl BufferPool {
    pub fn new() -> Self {
        let pool_cap = std::env::var("MYSQL_BUFFER_POOL_CAP")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or(128_usize);

        let buffer_cap = std::env::var("MYSQL_BUFFER_SIZE_CAP")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or(4 * 1024 * 1024);

        Self {
            pool: Default::default(),
            pool_cap,
            buffer_cap,
        }
    }

    pub fn get(self: &Arc<Self>) -> PooledBuf {
        let mut buf = self.pool.lock().unwrap().pop().unwrap_or_default();

        // SAFETY:
        // 1. OK – 0 is always within capacity
        // 2. OK - nothing to initialize
        unsafe { buf.set_len(0) }

        PooledBuf(buf, self.clone())
    }

    fn put(self: &Arc<Self>, mut buf: Vec<u8>) {
        if buf.len() > self.buffer_cap {
            // TODO: until `Vec::shrink_to` stabilization

            // SAFETY:
            // 1. OK – new_len <= capacity
            // 2. OK - 0..new_len is initialized
            unsafe { buf.set_len(self.buffer_cap) }
            buf.shrink_to_fit();
        }

        let mut pool = self.pool.lock().unwrap();
        if pool.len() < self.pool_cap {
            pool.push(buf);
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct PooledBuf(Vec<u8>, Arc<BufferPool>);

impl AsMut<Vec<u8>> for PooledBuf {
    fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }
}

impl Deref for PooledBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        self.1.put(replace(&mut self.0, vec![]))
    }
}
