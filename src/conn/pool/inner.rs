use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Condvar, Mutex,
    },
};

use crate::{Conn, Opts, PoolOpts};

#[derive(Debug)]
pub struct Protected {
    opts: Opts,
    connections: VecDeque<Conn>,
}

impl Protected {
    fn new(opts: Opts) -> crate::Result<Protected> {
        let constraints = opts.get_pool_opts().constraints();

        let mut this = Protected {
            connections: VecDeque::with_capacity(constraints.max()),
            opts,
        };

        for _ in 0..constraints.min() {
            this.new_conn()?;
        }

        Ok(this)
    }

    pub fn new_conn(&mut self) -> crate::Result<()> {
        match Conn::new(self.opts.clone()) {
            Ok(conn) => {
                self.connections.push_back(conn);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub fn take_by_query(&mut self, query: &[u8]) -> Option<Conn> {
        match self
            .connections
            .iter()
            .position(|conn| conn.has_stmt(query))
        {
            Some(position) => self.connections.swap_remove_back(position),
            None => None,
        }
    }

    pub fn pop_front(&mut self) -> Option<Conn> {
        self.connections.pop_front()
    }

    pub fn push_back(&mut self, conn: Conn) {
        self.connections.push_back(conn)
    }
}

pub struct Inner {
    protected: (Mutex<Protected>, Condvar),
    pool_opts: PoolOpts,
    count: AtomicUsize,
}

impl Inner {
    pub fn increase(&self) {
        let prev = self.count.fetch_add(1, Ordering::SeqCst);
        debug_assert!(prev < self.max_constraint());
    }

    pub fn decrease(&self) {
        let prev = self.count.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(prev > 0);
    }

    pub fn count(&self) -> usize {
        let value = self.count.load(Ordering::SeqCst);
        debug_assert!(value <= self.max_constraint());
        value
    }

    pub fn is_full(&self) -> bool {
        self.count() == self.max_constraint()
    }

    pub fn opts(&self) -> &PoolOpts {
        &self.pool_opts
    }

    pub fn max_constraint(&self) -> usize {
        self.pool_opts.constraints().max()
    }

    pub fn protected(&self) -> &(Mutex<Protected>, Condvar) {
        &self.protected
    }

    pub fn new(opts: Opts) -> crate::Result<Self> {
        Ok(Self {
            count: AtomicUsize::new(opts.get_pool_opts().constraints().min()),
            pool_opts: opts.get_pool_opts().clone(),
            protected: (Mutex::new(Protected::new(opts)?), Condvar::new()),
        })
    }
}
