use twox_hash::XxHash;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash};

use crate::conn::stmt::InnerStmt;

#[derive(Debug)]
pub struct StmtCache {
    cap: usize,
    map: HashMap<String, (u64, InnerStmt), BuildHasherDefault<XxHash>>,
    iter: u64,
}

impl StmtCache {
    pub fn new(cap: usize) -> StmtCache {
        StmtCache {
            cap,
            map: Default::default(),
            iter: 0,
        }
    }

    pub fn contains<T>(&self, key: &T) -> bool
    where
        String: Borrow<T>,
        T: Hash + Eq,
        T: ?Sized,
    {
        self.map.contains_key(key)
    }

    pub fn get<T>(&mut self, key: &T) -> Option<&InnerStmt>
    where
        String: Borrow<T>,
        String: PartialEq<T>,
        T: Hash + Eq,
        T: ?Sized,
    {
        if let Some(&mut (ref mut last, ref st)) = self.map.get_mut(key) {
            *last = self.iter;
            self.iter += 1;
            Some(st)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: String, value: InnerStmt) -> Option<InnerStmt> {
        if self.cap == 0 {
            return None;
        }

        self.map.insert(key, (self.iter, value));
        self.iter += 1;

        if self.map.len() > self.cap {
            if let Some(evict) = self
                .map
                .iter()
                .map(|(key, &(last, _))| (last, key))
                .min()
                .map(|(_, key)| key.to_string())
            {
                return self.map.remove(&evict).map(|(_, st)| st);
            }
        }
        None
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }

    #[cfg(test)]
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a String, u64)> + 'a> {
        Box::new(self.map.iter().map(|(stmt, &(i, _))| (stmt, i)))
    }

    pub fn into_iter(self) -> Box<dyn Iterator<Item = (String, InnerStmt)>> {
        Box::new(self.map.into_iter().map(|(k, (_, v))| (k, v)))
    }

    pub fn get_cap(&self) -> usize {
        self.cap
    }
}
