// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use lru::LruCache;
use twox_hash::XxHash;

use std::{
    borrow::Borrow,
    collections::HashMap,
    hash::{BuildHasherDefault, Hash},
    sync::Arc,
};

use crate::conn::stmt::InnerStmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryString(pub Arc<Vec<u8>>);

impl Borrow<[u8]> for QueryString {
    fn borrow(&self) -> &[u8] {
        &**self.0.as_ref()
    }
}

impl PartialEq<[u8]> for QueryString {
    fn eq(&self, other: &[u8]) -> bool {
        &**self.0.as_ref() == other
    }
}

pub struct Entry {
    pub stmt: Arc<InnerStmt>,
    pub query: QueryString,
}

#[derive(Debug)]
pub struct StmtCache {
    cap: usize,
    cache: LruCache<u32, Entry>,
    query_map: HashMap<QueryString, u32, BuildHasherDefault<XxHash>>,
}

impl StmtCache {
    pub fn new(cap: usize) -> StmtCache {
        StmtCache {
            cap,
            cache: LruCache::unbounded(),
            query_map: Default::default(),
        }
    }

    pub fn contains_query<T>(&self, key: &T) -> bool
    where
        QueryString: Borrow<T>,
        T: Hash + Eq,
        T: ?Sized,
    {
        self.query_map.contains_key(key)
    }

    pub fn by_query<T>(&mut self, query: &T) -> Option<&Entry>
    where
        QueryString: Borrow<T>,
        QueryString: PartialEq<T>,
        T: Hash + Eq,
        T: ?Sized,
    {
        let id = self.query_map.get(query).cloned();
        match id {
            Some(id) => self.cache.get(&id),
            None => None,
        }
    }

    pub fn put(&mut self, query: Arc<Vec<u8>>, stmt: Arc<InnerStmt>) -> Option<Arc<InnerStmt>> {
        if self.cap == 0 {
            return None;
        }

        let query = QueryString(query);

        self.query_map.insert(query.clone(), stmt.id());
        self.cache.put(stmt.id(), Entry { stmt, query });

        if self.cache.len() > self.cap {
            if let Some((_, entry)) = self.cache.pop_lru() {
                self.query_map.remove(&**entry.query.0.as_ref());
                return Some(entry.stmt);
            }
        }

        None
    }

    pub fn clear(&mut self) {
        self.query_map.clear();
        self.cache.clear();
    }

    pub fn remove(&mut self, id: u32) {
        if let Some(entry) = self.cache.pop(&id) {
            self.query_map.remove::<[u8]>(entry.query.borrow());
        }
    }

    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &Entry)> {
        self.cache.iter()
    }

    pub fn into_iter(mut self) -> impl Iterator<Item = (u32, Entry)> {
        std::iter::from_fn(move || self.cache.pop_lru())
    }
}
