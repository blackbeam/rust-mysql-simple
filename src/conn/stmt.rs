// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crossbeam_utils::atomic::AtomicCell;
use mysql_common::{io::ParseBuf, packets::StmtPacket, proto::MyDeserialize};

use std::{borrow::Cow, fmt, io, ptr::NonNull, sync::Arc};

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

/// This is to make raw Arc pointer Send and Sync
///
/// This splits fat `*const [Column]` pointer to its components
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
struct ColumnsArcPtr((NonNull<Column>, usize));

impl ColumnsArcPtr {
    fn from_arc(arc: Arc<[Column]>) -> Self {
        let len = arc.len();
        let ptr = Arc::into_raw(arc);
        // SAFETY: the `Arc` structure itself contains NonNull so this is either safe
        // or someone created a broken `Arc` using unsafe code.
        let ptr = unsafe { NonNull::new_unchecked(ptr as *const Column as *mut Column) };
        Self((ptr, len))
    }

    fn to_arc(self) -> Arc<[Column]> {
        let columns = self.into_arc();
        let clone = columns.clone();
        // ignore the pointer because it is already stored in self
        let _ = Arc::into_raw(columns);
        clone
    }

    fn into_arc(self) -> Arc<[Column]> {
        let fat_pointer = NonNull::slice_from_raw_parts(self.0 .0, self.0 .1);
        // SAFETY: non-null pointer always points to a valid Arc
        unsafe { Arc::from_raw(fat_pointer.as_ptr()) }
    }
}

unsafe impl Send for ColumnsArcPtr {}
unsafe impl Sync for ColumnsArcPtr {}

struct ColumnCache {
    columns: AtomicCell<Option<ColumnsArcPtr>>,
}

impl ColumnCache {
    fn new() -> Self {
        Self {
            columns: AtomicCell::new(None),
        }
    }

    fn get_columns(&self) -> Option<Arc<[Column]>> {
        self.columns.load().map(|x| x.to_arc())
    }

    fn set_columns(&self, new_columns: Vec<Column>) {
        let new_columns: Arc<[Column]> = new_columns.into();
        let new_ptr = ColumnsArcPtr::from_arc(new_columns);

        let Some(old_ptr) = self.columns.swap(Some(new_ptr)) else {
            return;
        };

        // drop the old `Arc`
        old_ptr.into_arc();
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

impl Drop for ColumnCache {
    fn drop(&mut self) {
        // drop `Arc` if any
        self.columns.load().map(|x| x.into_arc());
    }
}
