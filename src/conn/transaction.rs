use std::fmt;

use crate::{
    conn::ConnRef, prelude::*, queryable::ConnMut, Binary, Conn, LocalInfileHandler, Params,
    PooledConn, QueryResponse, Result, Statement, Text,
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
    conn: ConnMut<'a>,
    committed: bool,
    rolled_back: bool,
    restore_local_infile_handler: Option<LocalInfileHandler>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(conn: ConnMut<'a>) -> Transaction<'a> {
        let handler = conn.local_infile_handler.clone();
        Transaction {
            conn: conn,
            committed: false,
            rolled_back: false,
            restore_local_infile_handler: handler,
        }
    }

    /// Will consume and commit transaction.
    pub fn commit(mut self) -> Result<()> {
        (&mut *self.conn).query("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    /// Will consume and rollback transaction. You also can rely on `Drop` implementation but it
    /// will swallow errors.
    pub fn rollback(mut self) -> Result<()> {
        (&mut *self.conn).query("ROLLBACK")?;
        self.rolled_back = true;
        Ok(())
    }

    /// A way to override local infile handler for this transaction.
    /// Destructor of transaction will restore original handler.
    pub fn set_local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) {
        self.conn.set_local_infile_handler(handler);
    }
}

impl<'a> From<&'a mut Transaction<'_>> for ConnMut<'a> {
    fn from(tx: &'a mut Transaction<'_>) -> Self {
        ConnMut::Mut(&mut tx.conn)
    }
}

impl<'a> Drop for Transaction<'a> {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            let _ = (&mut *self.conn).query("ROLLBACK");
        }
        self.conn.local_infile_handler = self.restore_local_infile_handler.take();
    }
}
