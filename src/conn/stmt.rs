// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use arc_swap::ArcSwapOption;
use mysql_common::{io::ParseBuf, packets::StmtPacket, proto::MyDeserialize};

use std::{borrow::Cow, fmt, io, sync::Arc};

use crate::{prelude::*, Column, Result};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct InnerStmt {
    columns: Option<Arc<[Column]>>,
    /// This cached value overrides the column metadata stored in the `inner` field.
    ///
    /// See MARIADB_CLIENT_CACHE_METADATA capability.
    columns_cache: ColumnCache,
    params: Option<Arc<[Column]>>,
    stmt_packet: StmtPacket,
    connection_id: u32,
}

impl<'de> MyDeserialize<'de> for InnerStmt {
    const SIZE: Option<usize> = StmtPacket::SIZE;
    type Ctx = u32;

    fn deserialize(connection_id: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let stmt_packet = buf.parse(())?;

        Ok(InnerStmt {
            columns: None,
            columns_cache: ColumnCache::new(),
            params: None,
            stmt_packet,
            connection_id,
        })
    }
}

impl InnerStmt {
    pub fn with_params(mut self, params: Option<Vec<Column>>) -> Self {
        self.params = params.map(Into::into);
        self
    }

    pub fn with_columns(mut self, columns: Option<Vec<Column>>) -> Self {
        self.columns = columns.map(|x| x.into());
        self
    }

    pub fn columns(&self) -> Arc<[Column]> {
        self.columns_cache
            .get_columns()
            .or_else(|| self.columns.clone())
            .unwrap_or_default()
    }

    pub fn update_columns_metadata(&self, columns: Vec<Column>) {
        self.columns_cache.set_columns(columns);
    }

    pub fn params(&self) -> &[Column] {
        self.params.as_ref().map(AsRef::as_ref).unwrap_or(&[])
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
    pub(crate) named_params: Option<Vec<Vec<u8>>>,
}

impl Statement {
    pub(crate) fn new(inner: Arc<InnerStmt>, named_params: Option<Vec<Vec<u8>>>) -> Self {
        Self {
            inner,
            named_params,
        }
    }

    pub fn columns(&self) -> Arc<[Column]> {
        self.inner.columns()
    }

    /// Overrides columns metadata for this statement.
    ///
    /// See MARIADB_CLIENT_CACHE_METADATA capability.
    pub(crate) fn update_columns_metadata(&self, columns: Vec<Column>) {
        self.inner.update_columns_metadata(columns);
    }

    pub fn params(&self) -> &[Column] {
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

impl AsStatement for &'_ Statement {
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

struct ColumnCache {
    columns: ArcSwapOption<Arc<[Column]>>,
}

impl ColumnCache {
    const fn new() -> Self {
        Self {
            columns: ArcSwapOption::const_empty(),
        }
    }

    fn get_columns(&self) -> Option<Arc<[Column]>> {
        self.columns.load_full().map(|x| (*x).clone())
    }

    fn set_columns(&self, new_columns: Vec<Column>) {
        let new_columns: Arc<[Column]> = new_columns.into();
        self.columns.store(Some(Arc::new(new_columns)));
    }
}

impl fmt::Debug for ColumnCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ColumnCache")
            .field("columns", &self.get_columns())
            .finish()
    }
}

impl PartialEq for ColumnCache {
    fn eq(&self, other: &Self) -> bool {
        self.get_columns() == other.get_columns()
    }
}

impl Eq for ColumnCache {}

#[cfg(test)]
mod tests {
    use super::ColumnCache;
    use crate::Column;
    use mysql_common::constants::ColumnType;
    use std::{sync::Arc, thread};

    const ROUNDS: usize = if cfg!(miri) { 8 } else { 10_000 };

    fn columns(table: &str, len: usize) -> Vec<Column> {
        (0..len)
            .map(|_| Column::new(ColumnType::MYSQL_TYPE_LONG).with_table(table.as_bytes()))
            .collect()
    }

    #[test]
    fn reader_keeps_columns_alive_while_writer_replaces_them() {
        let cache = Arc::new(ColumnCache::new());
        cache.set_columns(columns("t", 42));

        let writer = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for _ in 0..ROUNDS {
                    cache.set_columns(columns("t", 46));
                    cache.set_columns(columns("t", 42));
                }
            })
        };

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for _ in 0..ROUNDS {
                        let Some(columns) = cache.get_columns() else {
                            continue;
                        };
                        for column in columns.iter() {
                            assert_eq!(column.table_str(), "t");
                        }
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
    }
}
