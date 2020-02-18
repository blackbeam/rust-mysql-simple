use mysql_common::{packets::OkPacket, row::new_row};

use std::{collections::hash_map::HashMap, marker::PhantomData, sync::Arc};

use crate::{
    from_row,
    prelude::{FromRow, IntoFallibleIterator},
    queryable::ConnMut,
    Column, Conn, Error, Result, Row, Value,
};

pub enum Or<A, B> {
    A(A),
    B(B),
}

/// Result set kind.
pub trait Protocol: 'static + Send + Sync {
    fn next(conn: &mut Conn, columns: &Arc<Vec<Column>>) -> Result<Option<Vec<Value>>>;
}

/// Text result set marker.
#[derive(Debug)]
pub struct Text;

impl Protocol for Text {
    fn next(conn: &mut Conn, columns: &Arc<Vec<Column>>) -> Result<Option<Vec<Value>>> {
        conn.next_text(columns.len())
    }
}

/// Binary result set marker.
#[derive(Debug)]
pub struct Binary;

impl Protocol for Binary {
    fn next(conn: &mut Conn, columns: &Arc<Vec<Column>>) -> Result<Option<Vec<Value>>> {
        conn.next_bin(columns)
    }
}

/// State of a result set iterator.
#[derive(Debug)]
enum SetIteratorState {
    /// Iterator is in a non-empty set.
    InSet(Arc<Vec<Column>>),
    /// Iterator is in an empty set.
    InEmptySet(OkPacket<'static>),
    /// Iterator is in an errored result set.
    Errored(Error),
    /// Next result set isn't handled.
    OnBoundary,
    /// No more result sets.
    Done,
}

impl SetIteratorState {
    fn ok_packet(&self) -> Option<&OkPacket<'_>> {
        if let Self::InEmptySet(ref ok) = self {
            Some(ok)
        } else {
            None
        }
    }

    fn columns(&self) -> Option<&Arc<Vec<Column>>> {
        if let Self::InSet(ref cols) = self {
            Some(cols)
        } else {
            None
        }
    }
}

impl From<Vec<Column>> for SetIteratorState {
    fn from(columns: Vec<Column>) -> Self {
        Self::InSet(columns.into())
    }
}

impl From<OkPacket<'static>> for SetIteratorState {
    fn from(ok_packet: OkPacket<'static>) -> Self {
        Self::InEmptySet(ok_packet)
    }
}

impl From<Error> for SetIteratorState {
    fn from(err: Error) -> Self {
        Self::Errored(err)
    }
}

impl From<Or<Vec<Column>, OkPacket<'static>>> for SetIteratorState {
    fn from(or: Or<Vec<Column>, OkPacket<'static>>) -> Self {
        match or {
            Or::A(cols) => Self::from(cols),
            Or::B(ok) => Self::from(ok),
        }
    }
}

/// Response to a query or statement execution.
///
/// It is an iterator:
/// *   over result sets (via `Self::next_set`)
/// *   over rows of a current result set (via `Iterator` impl)
#[derive(Debug)]
pub struct QueryResponse<'a, T: Protocol> {
    conn: ConnMut<'a>,
    state: SetIteratorState,
    set_index: usize,
    protocol: PhantomData<T>,
}

impl<'a, T: Protocol> QueryResponse<'a, T> {
    fn from_state<'b>(conn: ConnMut<'b>, state: SetIteratorState) -> QueryResponse<'b, T> {
        QueryResponse {
            conn,
            state,
            set_index: 0,
            protocol: PhantomData,
        }
    }

    pub(crate) fn new<'b>(
        conn: ConnMut<'b>,
        meta: Or<Vec<Column>, OkPacket<'static>>,
    ) -> QueryResponse<'b, T> {
        Self::from_state(conn, meta.into())
    }

    pub(crate) fn in_set<'b>(conn: ConnMut<'b>, columns: Vec<Column>) -> QueryResponse<'b, T> {
        Self::from_state(conn, columns.into())
    }

    pub(crate) fn in_empty_set<'b>(
        conn: ConnMut<'b>,
        ok_packet: OkPacket<'static>,
    ) -> QueryResponse<'b, T> {
        Self::from_state(conn, ok_packet.into())
    }

    /// Updates state with the next result set, if any.
    ///
    /// Returns `false` if there is no next result set.
    ///
    /// **Requires:** `self.state == OnBoundary`
    fn handle_next(&mut self) {
        if cfg!(debug_assertions) {
            if let SetIteratorState::OnBoundary = self.state {
            } else {
                panic!("self.state == OnBoundary");
            }
        }

        if self.conn.more_results_exists() {
            match self.conn.handle_result_set() {
                Ok(meta) => self.state = meta.into(),
                Err(err) => self.state = err.into(),
            }
            self.set_index += 1;
        } else {
            self.state = SetIteratorState::Done;
        }
    }

    /// Returns an iterator over the current result set.
    pub fn next_set<'b>(&'b mut self) -> Option<Result<ResultSet<'a, 'b, T>>> {
        use SetIteratorState::*;

        if let OnBoundary | Done = &self.state {
            debug_assert!(
                self.conn.more_results_exists() == false,
                "the next state must be handled by the Iterator::next"
            );

            None
        } else {
            Some(Ok(ResultSet {
                set_index: self.set_index,
                inner: self,
            }))
        }
    }

    /// Returns the number of affected rows for the current result set.
    pub fn affected_rows(&self) -> u64 {
        self.state
            .ok_packet()
            .map(|ok| ok.affected_rows())
            .unwrap_or_default()
    }

    /// Returns the last insert id for the current result set.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.state
            .ok_packet()
            .map(|ok| ok.last_insert_id())
            .unwrap_or_default()
    }

    /// Returns the warnings count for the current result set.
    pub fn warnings(&self) -> u16 {
        self.state
            .ok_packet()
            .map(|ok| ok.warnings())
            .unwrap_or_default()
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// info for the current result set.
    pub fn info(&self) -> &[u8] {
        self.state
            .ok_packet()
            .and_then(|ok| ok.info_ref())
            .unwrap_or_default()
    }

    /// Returns columns of the current result rest.
    pub fn columns(&self) -> SetColumns {
        SetColumns {
            inner: self.state.columns().map(Into::into),
        }
    }
}

impl<'a, T: Protocol> Drop for QueryResponse<'a, T> {
    fn drop(&mut self) {
        while self.next_set().is_some() {}
    }
}

#[derive(Debug)]
pub struct ResultSet<'a, 'b, T: Protocol> {
    set_index: usize,
    inner: &'b mut QueryResponse<'a, T>,
}

impl<'a, 'b, T: Protocol> std::ops::Deref for ResultSet<'a, 'b, T> {
    type Target = QueryResponse<'a, T>;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl<'a, 'b, T: Protocol> Iterator for ResultSet<'a, 'b, T> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.set_index == self.inner.set_index {
            self.inner.next()
        } else {
            None
        }
    }
}

impl<'a, T: Protocol> Iterator for QueryResponse<'a, T> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        use SetIteratorState::*;

        let state = std::mem::replace(&mut self.state, OnBoundary);

        match state {
            InSet(cols) => match T::next(&mut *self.conn, &cols) {
                Ok(Some(vals)) => {
                    self.state = InSet(cols.clone());
                    Some(Ok(new_row(vals, cols)))
                }
                Ok(None) => {
                    self.handle_next();
                    None
                }
                Err(e) => {
                    self.handle_next();
                    Some(Err(e))
                }
            },
            InEmptySet(_) => {
                self.handle_next();
                None
            }
            Errored(err) => {
                self.handle_next();
                Some(Err(err))
            }
            OnBoundary => None,
            Done => {
                self.state = Done;
                None
            }
        }
    }
}

impl<'a, P: Protocol> IntoFallibleIterator for QueryResponse<'a, P> {
    type Item = Row;
    type Error = Error;
    type IntoFallibleIter = fallible_iterator::Convert<Self>;

    fn into_fallible_iter(self) -> Self::IntoFallibleIter {
        fallible_iterator::convert(self)
    }
}

impl<'a, 'b, P: Protocol> IntoFallibleIterator for ResultSet<'a, 'b, P> {
    type Item = Row;
    type Error = Error;
    type IntoFallibleIter = fallible_iterator::Convert<Self>;

    fn into_fallible_iter(self) -> Self::IntoFallibleIter {
        fallible_iterator::convert(self)
    }
}

impl<'a, 'b, T: Protocol> Drop for ResultSet<'a, 'b, T> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

#[derive(Debug)]
pub struct SetColumns<'a> {
    inner: Option<&'a Arc<Vec<Column>>>,
}

impl<'a> SetColumns<'a> {
    /// Returns an index of a column by its name.
    pub fn column_index<U: AsRef<str>>(&self, name: U) -> Option<usize> {
        let name = name.as_ref().as_bytes();
        self.inner
            .as_ref()
            .and_then(|cols| cols.iter().position(|col| col.name_ref() == name))
    }

    pub fn as_ref(&self) -> &[Column] {
        self.inner
            .as_ref()
            .map(|cols| &(*cols)[..])
            .unwrap_or(&[][..])
    }
}
