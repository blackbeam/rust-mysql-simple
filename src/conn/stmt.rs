use mysql_common::packets::{parse_stmt_packet, StmtPacket};

use std::{borrow::Cow, io, sync::Arc};

use crate::{prelude::*, Column, Result};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InnerStmt {
    columns: Option<Vec<Column>>,
    params: Option<Vec<Column>>,
    stmt_packet: StmtPacket,
    connection_id: u32,
}

impl InnerStmt {
    pub fn from_payload(pld: &[u8], connection_id: u32) -> io::Result<InnerStmt> {
        let stmt_packet = parse_stmt_packet(pld)?;

        Ok(InnerStmt {
            columns: None,
            params: None,
            stmt_packet,
            connection_id,
        })
    }

    pub fn with_params(mut self, params: Option<Vec<Column>>) -> Self {
        self.params = params;
        self
    }

    pub fn with_columns(mut self, columns: Option<Vec<Column>>) -> Self {
        self.columns = columns;
        self
    }

    pub fn columns(&self) -> Option<&[Column]> {
        self.columns.as_ref().map(AsRef::as_ref)
    }

    pub fn params(&self) -> Option<&[Column]> {
        self.params.as_ref().map(AsRef::as_ref)
    }

    pub fn id(&self) -> u32 {
        self.stmt_packet.statement_id()
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection_id
    }

    pub fn num_params(&self) -> u16 {
        self.stmt_packet.num_params()
    }

    pub fn num_columns(&self) -> u16 {
        self.stmt_packet.num_columns()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Statement {
    pub(crate) inner: Arc<InnerStmt>,
    pub(crate) named_params: Option<Vec<String>>,
}

impl Statement {
    pub(crate) fn new(inner: Arc<InnerStmt>, named_params: Option<Vec<String>>) -> Self {
        Self {
            inner,
            named_params,
        }
    }

    pub fn columns(&self) -> Option<&[Column]> {
        self.inner.columns()
    }

    pub fn params(&self) -> Option<&[Column]> {
        self.inner.params()
    }

    pub fn id(&self) -> u32 {
        self.inner.id()
    }

    pub fn connection_id(&self) -> u32 {
        self.inner.connection_id()
    }

    pub fn num_params(&self) -> u16 {
        self.inner.num_params()
    }

    pub fn num_columns(&self) -> u16 {
        self.inner.num_columns()
    }
}

impl AsStatement for Statement {
    fn as_statement<Q: Queryable>(&self, _queryable: &mut Q) -> Result<Cow<'_, Statement>> {
        Ok(Cow::Borrowed(self))
    }
}

impl<'a> AsStatement for &'a Statement {
    fn as_statement<Q: Queryable>(&self, _queryable: &mut Q) -> Result<Cow<'_, Statement>> {
        Ok(Cow::Borrowed(self))
    }
}

impl<T: AsRef<str>> AsStatement for T {
    fn as_statement<Q: Queryable>(&self, queryable: &mut Q) -> Result<Cow<'static, Statement>> {
        let statement = queryable.prep(self.as_ref())?;
        Ok(Cow::Owned(statement))
    }
}
