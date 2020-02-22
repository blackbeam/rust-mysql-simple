// Copyright (c) 2020 rust-mysql-common contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::fmt;

use crate::{
    conn::ConnRef, prelude::*, Conn, LocalInfileHandler, Params, PooledConn, QueryResult, Result,
    Statement,
};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            IsolationLevel::ReadUncommitted => write!(f, "READ UNCOMMITTED"),
            IsolationLevel::ReadCommitted => write!(f, "READ COMMITTED"),
            IsolationLevel::RepeatableRead => write!(f, "REPEATABLE READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    conn: ConnRef<'a>,
    committed: bool,
    rolled_back: bool,
    restore_local_infile_handler: Option<LocalInfileHandler>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(conn: &'a mut Conn) -> Transaction<'a> {
        let handler = conn.local_infile_handler.clone();
        Transaction {
            conn: ConnRef::ViaConnRef(conn),
            committed: false,
            rolled_back: false,
            restore_local_infile_handler: handler,
        }
    }

    pub(crate) fn new_pooled(conn: PooledConn) -> Transaction<'a> {
        let handler = conn.as_ref().local_infile_handler.clone();
        Transaction {
            conn: ConnRef::ViaPooledConn(conn),
            committed: false,
            rolled_back: false,
            restore_local_infile_handler: handler,
        }
    }

    /// Will consume and commit transaction.
    pub fn commit(mut self) -> Result<()> {
        self.conn.query_drop("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    /// Will consume and rollback transaction. You also can rely on `Drop` implementation but it
    /// will swallow errors.
    pub fn rollback(mut self) -> Result<()> {
        self.conn.query_drop("ROLLBACK")?;
        self.rolled_back = true;
        Ok(())
    }

    /// A way to override local infile handler for this transaction.
    /// Destructor of transaction will restore original handler.
    pub fn set_local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) {
        self.conn.set_local_infile_handler(handler);
    }
}

impl<'a> Queryable for Transaction<'a> {
    fn query_iter<T: AsRef<str>>(&mut self, query: T) -> Result<QueryResult<'_>> {
        self.conn.query_iter(query)
    }

    fn prep<T: AsRef<str>>(&mut self, query: T) -> Result<Statement> {
        self.conn.prep(query)
    }

    fn close(&mut self, stmt: Statement) -> Result<()> {
        self.conn.close(stmt)
    }

    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_>>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        self.conn.exec_iter(stmt, params)
    }
}

impl<'a> Drop for Transaction<'a> {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            let _ = self.conn.query_drop("ROLLBACK");
        }
        self.conn.local_infile_handler = self.restore_local_infile_handler.take();
    }
}
