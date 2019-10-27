use std::fmt;

use crate::conn::ConnRef;
use crate::prelude::{FromRow, GenericConnection};
use crate::{from_row, Conn, LocalInfileHandler, Params, PooledConn, QueryResult, Result, Stmt};

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

    /// See [`Conn::query`](struct.Conn.html#method.query).
    pub fn query<T: AsRef<str>>(&mut self, query: T) -> Result<QueryResult<'_>> {
        self.conn.query(query)
    }

    /// See [`Conn::first`](struct.Conn.html#method.first).
    pub fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> Result<Option<U>> {
        self.query(query).and_then(|mut result| {
            if let Some(row) = result.next() {
                row.map(|x| Some(from_row(x)))
            } else {
                Ok(None)
            }
        })
    }

    /// See [`Conn::prepare`](struct.Conn.html#method.prepare).
    pub fn prepare<T: AsRef<str>>(&mut self, query: T) -> Result<Stmt<'_>> {
        self.conn.prepare(query)
    }

    /// See [`Conn::prep_exec`](struct.Conn.html#method.prep_exec).
    pub fn prep_exec<A: AsRef<str>, T: Into<Params>>(
        &mut self,
        query: A,
        params: T,
    ) -> Result<QueryResult<'_>> {
        self.conn.prep_exec(query, params)
    }

    /// See [`Conn::first_exec`](struct.Conn.html#method.first_exec).
    pub fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> Result<Option<T>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        T: FromRow,
    {
        self.prep_exec(query, params).and_then(|mut result| {
            if let Some(row) = result.next() {
                row.map(|x| Some(from_row(x)))
            } else {
                Ok(None)
            }
        })
    }

    /// Will consume and commit transaction.
    pub fn commit(mut self) -> Result<()> {
        self.conn.query("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    /// Will consume and rollback transaction. You also can rely on `Drop` implementation but it
    /// will swallow errors.
    pub fn rollback(mut self) -> Result<()> {
        self.conn.query("ROLLBACK")?;
        self.rolled_back = true;
        Ok(())
    }

    /// A way to override local infile handler for this transaction.
    /// Destructor of transaction will restore original handler.
    pub fn set_local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) {
        self.conn.set_local_infile_handler(handler);
    }
}

impl<'a> GenericConnection for Transaction<'a> {
    fn query<T: AsRef<str>>(&mut self, query: T) -> Result<QueryResult<'_>> {
        self.query(query)
    }

    fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> Result<Option<U>> {
        self.first(query)
    }

    fn prepare<T: AsRef<str>>(&mut self, query: T) -> Result<Stmt<'_>> {
        self.prepare(query)
    }

    fn prep_exec<A, T>(&mut self, query: A, params: T) -> Result<QueryResult<'_>>
    where
        A: AsRef<str>,
        T: Into<Params>,
    {
        self.prep_exec(query, params)
    }

    fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> Result<Option<T>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        T: FromRow,
    {
        self.first_exec(query, params)
    }
}

impl<'a> Drop for Transaction<'a> {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            let _ = self.conn.query("ROLLBACK");
        }
        self.conn.local_infile_handler = self.restore_local_infile_handler.take();
    }
}
