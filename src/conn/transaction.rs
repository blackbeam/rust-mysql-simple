// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::packets::OkPacket;

use std::{borrow::Cow, fmt};

use crate::{
    conn::{
        query_result::{Binary, Text},
        ConnMut,
    },
    prelude::*,
    LocalInfileHandler, Params, QueryResult, Result, Statement,
};

/// MySql transaction options.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct TxOpts {
    with_consistent_snapshot: bool,
    isolation_level: Option<IsolationLevel>,
    access_mode: Option<AccessMode>,
}

impl TxOpts {
    /// Returns the value of the characteristic.
    pub fn with_consistent_snapshot(&self) -> bool {
        self.with_consistent_snapshot
    }

    /// Returns the access mode value.
    pub fn access_mode(&self) -> Option<AccessMode> {
        self.access_mode
    }

    /// Returns the isolation level value.
    pub fn isolation_level(&self) -> Option<IsolationLevel> {
        self.isolation_level
    }

    /// Turns on/off the `WITH CONSISTENT SNAPSHOT` tx characteristic (defaults to `false`).
    pub fn set_with_consistent_snapshot(mut self, val: bool) -> Self {
        self.with_consistent_snapshot = val;
        self
    }

    /// Defines the transaction access mode (defaults to `None`, i.e unspecified).
    pub fn set_access_mode(mut self, access_mode: Option<AccessMode>) -> Self {
        self.access_mode = access_mode;
        self
    }

    /// Defines the transaction isolation level (defaults to `None`, i.e. unspecified).
    pub fn set_isolation_level(mut self, level: Option<IsolationLevel>) -> Self {
        self.isolation_level = level;
        self
    }
}

/// MySql transaction access mode.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
#[repr(u8)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}

/// MySql transaction isolation level.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
#[repr(u8)]
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
    pub(crate) conn: ConnMut<'a, 'static, 'static>,
    committed: bool,
    rolled_back: bool,
    restore_local_infile_handler: Option<LocalInfileHandler>,
}

impl Transaction<'_> {
    pub(crate) fn new<'a>(conn: ConnMut<'a, 'static, 'static>) -> Transaction<'a> {
        let handler = conn.0.local_infile_handler.clone();
        Transaction {
            conn,
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

    /// Returns the number of affected rows, reported by the server.
    pub fn affected_rows(&self) -> u64 {
        self.conn.affected_rows()
    }

    /// Returns the last insert id of the last query, if any.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.conn
            .0
            .ok_packet
            .as_ref()
            .and_then(OkPacket::last_insert_id)
    }

    /// Returns the warnings count, reported by the server.
    pub fn warnings(&self) -> u16 {
        self.conn.warnings()
    }

    /// [Info], reported by the server.
    ///
    /// Will be empty if not defined.
    ///
    /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    pub fn info_ref(&self) -> &[u8] {
        self.conn.info_ref()
    }

    /// [Info], reported by the server.
    ///
    /// Will be empty if not defined.
    ///
    /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    pub fn info_str(&self) -> Cow<str> {
        self.conn.info_str()
    }
}

impl<'a> Queryable for Transaction<'a> {
    fn query_iter<T: AsRef<str>>(&mut self, query: T) -> Result<QueryResult<'_, '_, '_, Text>> {
        self.conn.query_iter(query)
    }

    fn prep<T: AsRef<str>>(&mut self, query: T) -> Result<Statement> {
        self.conn.prep(query)
    }

    fn close(&mut self, stmt: Statement) -> Result<()> {
        self.conn.close(stmt)
    }

    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_, '_, '_, Binary>>
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
        self.conn.0.local_infile_handler = self.restore_local_infile_handler.take();
    }
}
