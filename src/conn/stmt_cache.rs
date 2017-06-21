use packet::InnerStmt;
use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::IntoIter;
use std::hash::{BuildHasherDefault, Hash};
use twox_hash::XxHash;

#[derive(Debug)]
pub struct StmtCache {
    cap: usize,
    map: HashMap<String, InnerStmt, BuildHasherDefault<XxHash>>,
    order: VecDeque<String>,
}

impl StmtCache {
    pub fn new(cap: usize) -> StmtCache {
        StmtCache {
            cap,
            map: Default::default(),
            order: VecDeque::with_capacity(cap),
        }
    }

    pub fn contains<T>(&self, key: &T) -> bool
        where String: Borrow<T>,
              T: Hash + Eq,
              T: ?Sized
    {
        self.map.contains_key(key)
    }

    pub fn get<T>(&mut self, key: &T) -> Option<&InnerStmt>
        where String: Borrow<T>,
              String: PartialEq<T>,
              T: Hash + Eq,
              T: ?Sized
    {
        if self.map.contains_key(key) {
            if let Some(mut pos) = self.order.iter().position(|x| x == key) {
                while pos < self.order.len() - 1 {
                    self.order.swap(pos, pos + 1);
                    pos += 1;
                }
            }
            self.map.get(key)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: String, value: InnerStmt) -> Option<InnerStmt> {
        self.map.insert(key.clone(), value);
        self.order.push_back(key);
        if self.order.len() > self.cap {
            self.order.pop_front().and_then(|stmt| self.map.remove(&stmt))
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    pub fn into_iter(self) -> IntoIter<String, InnerStmt> {
        self.map.into_iter()
    }

    pub fn get_cap(&self) -> usize {
        self.cap
    }
}