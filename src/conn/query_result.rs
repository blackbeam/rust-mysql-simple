use mysql_common::{packets::OkPacket, row::new_row};

use std::{collections::hash_map::HashMap, convert::identity, marker::PhantomData, sync::Arc};

use crate::{from_row, prelude::FromRow, Column, Conn, Result, Row, Value};

pub enum Or<A, B> {
    A(A),
    B(B),
}

pub trait Protocol: 'static + Send + Sync {
    fn next(conn: &mut Conn, columns: &Arc<Vec<Column>>) -> Result<Option<Vec<Value>>>;
}

pub struct Text;

impl Protocol for Text {
    fn next(conn: &mut Conn, columns: &Arc<Vec<Column>>) -> Result<Option<Vec<Value>>> {
        conn.next_text(columns.len())
    }
}

pub struct Binary;

impl Protocol for Binary {
    fn next(conn: &mut Conn, columns: &Arc<Vec<Column>>) -> Result<Option<Vec<Value>>> {
        conn.next_bin(columns)
    }
}

enum SetIteratorState {
    InSet(Arc<Vec<Column>>),
    InEmptySet(OkPacket<'static>),
    OnBoundary,
}

impl SetIteratorState {
    fn ok_packet(&self) -> Option<&OkPacket<'_>> {
        if let Self::InEmptySet(ref ok) = self {
            Some(ok)
        } else {
            None
        }
    }

    fn columns(&self) -> Option<&[Column]> {
        if let Self::InSet(ref cols) = self {
            Some(&**cols)
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

pub struct QueryResponse<'a, T: Protocol> {
    conn: &'a mut Conn,
    state: SetIteratorState,
    protocol: PhantomData<T>,
}

impl<'a, T: Protocol> QueryResponse<'a, T> {
    fn from_state(conn: &'a mut Conn, state: SetIteratorState) -> Self {
        QueryResponse {
            conn,
            state,
            protocol: PhantomData,
        }
    }

    pub(crate) fn in_set(conn: &'a mut Conn, columns: Vec<Column>) -> Self {
        Self::from_state(conn, columns.into())
    }

    pub(crate) fn in_empty_set(conn: &'a mut Conn, ok_packet: OkPacket<'static>) -> Self {
        Self::from_state(conn, ok_packet.into())
    }

    pub fn next_set<'b>(&'b mut self) -> Option<Result<ResultSet<'b, T>>> {
        use Or::*;
        use SetIteratorState::*;

        if let OnBoundary = &self.state {
            if self.conn.more_results_exists() {
                match self.conn.handle_result_set() {
                    Ok(A(cols)) => self.state = cols.into(),
                    Ok(B(ok)) => self.state = ok.into(),
                    Err(err) => return Some(Err(err)),
                }
                self.next_set()
            } else {
                None
            }
        } else {
            Some(Ok(ResultSet {
                conn: &mut *self.conn,
                state: &mut self.state,
                phantom: PhantomData,
            }))
        }
    }
}

impl<'a, T: Protocol> Drop for QueryResponse<'a, T> {
    fn drop(&mut self) {
        while self.next_set().map(drop).is_some() {}
    }
}

pub struct ResultSet<'a, T: Protocol> {
    conn: &'a mut Conn,
    state: &'a mut SetIteratorState,
    phantom: PhantomData<T>,
}

impl<'a, T: Protocol> ResultSet<'a, T> {
    /// Returns the number of affected rows.
    pub fn affected_rows(&self) -> u64 {
        self.state
            .ok_packet()
            .map(|ok| ok.affected_rows())
            .unwrap_or_default()
    }

    /// Returns the last insert id.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.state
            .ok_packet()
            .map(|ok| ok.last_insert_id())
            .unwrap_or_default()
    }

    /// Returns the warnings count.
    pub fn warnings(&self) -> u16 {
        self.state
            .ok_packet()
            .map(|ok| ok.warnings())
            .unwrap_or_default()
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// info.
    pub fn info(&self) -> &[u8] {
        self.state
            .ok_packet()
            .and_then(|ok| ok.info_ref())
            .unwrap_or_default()
    }

    /// Returns an index of a column by its name.
    pub fn column_index<U: AsRef<str>>(&self, name: U) -> Option<usize> {
        let name = name.as_ref().as_bytes();
        self.state
            .columns()
            .and_then(|cols| cols.iter().position(|col| col.name_ref() == name))
    }
}

impl<'a, T: Protocol> Iterator for ResultSet<'a, T> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        use SetIteratorState::*;

        match self.state {
            InSet(ref cols) => match T::next(self.conn, cols) {
                Ok(Some(vals)) => Some(Ok(new_row(vals, cols.clone()))),
                Ok(None) => {
                    *self.state = OnBoundary;
                    None
                }
                Err(e) => Some(Err(e)),
            },
            InEmptySet(_) | OnBoundary => None,
        }
    }
}

impl<'a, T: Protocol> Drop for ResultSet<'a, T> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

pub trait MyIterator {
    fn my_fold<T, U, F>(self, init: U, step: F) -> Result<U>
    where
        Self: Sized,
        T: FromRow,
        F: FnMut(U, T) -> U;

    fn my_map_with<T, U, F>(self, mut fun: F) -> Result<Vec<U>>
    where
        Self: Sized,
        T: FromRow,
        F: FnMut(T) -> U,
    {
        self.my_fold(Vec::new(), |mut acc, row: T| {
            acc.push(fun(row));
            acc
        })
    }

    fn my_map<T>(self) -> Result<Vec<T>>
    where
        Self: Sized,
        T: FromRow,
    {
        self.my_map_with(identity)
    }

    fn my_first<T>(self) -> Result<Option<T>>
    where
        Self: Sized,
        T: FromRow,
    {
        self.my_fold(None, |acc, row: T| acc.or(Some(row)))
    }
}

impl<I> MyIterator for I
where
    I: Iterator<Item = Result<Row>>,
{
    fn my_fold<T, U, F>(mut self, mut init: U, mut step: F) -> Result<U>
    where
        Self: Sized,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        while let Some(row) = self.next() {
            init = step(init, from_row(row?));
        }
        Ok(init)
    }
}

impl<'a, P> MyIterator for QueryResponse<'a, P>
where
    P: Protocol,
{
    fn my_fold<T, U, F>(mut self, init: U, step: F) -> Result<U>
    where
        Self: Sized,
        T: FromRow,
        F: FnMut(U, T) -> U,
    {
        match self.next_set().transpose()? {
            Some(result_set) => result_set.my_fold(init, step),
            None => Ok(init),
        }
    }
}

/// Mysql result set for text and binary protocols.
///
/// If you want to get rows from `QueryResult` you should rely on implementation
/// of `Iterator` over `Result<Row>` on `QueryResult`.
///
/// [`Row`](struct.Row.html) is the current row representation. To get something useful from
/// [`Row`](struct.Row.html) you should rely on `FromRow` trait implemented for tuples of
/// `FromValue` implementors up to arity 12, or on `FromValue` trait for rows with higher arity.
///
/// For more info on how to work with values please look at
/// [`Value`](../value/enum.Value.html) documentation.
#[derive(Debug)]
pub struct QueryResult<'a> {
    conn: &'a mut Conn,
    columns: Arc<Vec<Column>>,
    is_bin: bool,
}

impl<'a> QueryResult<'a> {
    pub(crate) fn new(conn: &'a mut Conn, columns: Vec<Column>, is_bin: bool) -> QueryResult<'a> {
        QueryResult {
            conn,
            columns: Arc::new(columns),
            is_bin,
        }
    }

    fn handle_if_more_results(&mut self) -> Option<Result<Row>> {
        if self.conn.more_results_exists() {
            match self.conn.handle_result_set() {
                Ok(Or::A(cols)) => {
                    self.columns = Arc::new(cols);
                    None
                }
                Ok(_) => {
                    self.columns = Arc::new(Vec::new());
                    None
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            None
        }
    }

    /// Returns number affected rows for the current result set.
    pub fn affected_rows(&self) -> u64 {
        self.conn.affected_rows()
    }

    /// Returns the last insert id for the current result set.
    pub fn last_insert_id(&self) -> u64 {
        self.conn.last_insert_id()
    }

    /// Returns warnings count for the current result set.
    pub fn warnings(&self) -> u16 {
        self.conn
            .last_ok_packet
            .as_ref()
            .map(|ok| ok.warnings())
            .unwrap_or_default()
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// info.
    pub fn info(&self) -> &[u8] {
        self.conn
            .last_ok_packet
            .as_ref()
            .and_then(|ok| ok.info_ref())
            .unwrap_or_default()
    }

    /// Returns index of a `QueryResult`'s column by name.
    pub fn column_index<T: AsRef<str>>(&self, name: T) -> Option<usize> {
        let name = name.as_ref().as_bytes();
        for (i, c) in self.columns.iter().enumerate() {
            if c.name_ref() == name {
                return Some(i);
            }
        }
        None
    }

    /// Returns a `HashMap` which maps column names to column indexes.
    pub fn column_indexes(&self) -> HashMap<String, usize> {
        let mut indexes = HashMap::default();
        for (i, column) in self.columns.iter().enumerate() {
            indexes.insert(column.name_str().into_owned(), i);
        }
        indexes
    }

    /// Returns a list of columns in the current result set.
    pub fn columns_ref(&self) -> &[Column] {
        self.columns.as_ref()
    }

    /// Returns `true` if this query result contains another result set,
    /// or if last result set isn't fully consumed.
    ///
    /// # Note
    ///
    /// Note, that the metadata of a query result (number of affected rows, last insert id etc.)
    /// will change on the result set boundary, i.e:
    ///
    /// ```rust
    /// # mysql::doctest_wrapper!(__result, {
    /// # use mysql::*;
    /// # use mysql::prelude::*;
    /// # let mut conn = Conn::new(get_opts())?;
    /// let mut result = conn.query_iter("SELECT 1; SELECT 'foo', 'bar';")?;
    ///
    /// // First result set contains one column.
    /// assert_eq!(result.columns_ref().len(), 1);
    /// for row in result.by_ref() {} // first result set was consumed
    ///
    /// // More results exists
    /// assert!(result.more_results_exists());
    ///
    /// // Second result set contains two columns.
    /// assert_eq!(result.columns_ref().len(), 2);
    /// for row in result.by_ref() {} // second result set was consumed
    ///
    /// // No more results
    /// assert!(result.more_results_exists() == false);
    /// # });
    /// ```
    pub fn more_results_exists(&self) -> bool {
        self.conn.more_results_exists() || !self.consumed()
    }

    /// Returns true if current result set is consumed.
    ///
    /// See also [`QueryResult::more_results_exists`].
    pub fn consumed(&self) -> bool {
        !self.conn.has_results
    }
}

impl<'a> Iterator for QueryResult<'a> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Result<Row>> {
        let values = if self.columns.len() > 0 {
            if self.is_bin {
                self.conn.next_bin(&self.columns)
            } else {
                self.conn.next_text(self.columns.len())
            }
        } else {
            Ok(None)
        };
        match values {
            Ok(values) => match values {
                Some(values) => Some(Ok(new_row(values, self.columns.clone()))),
                None => self.handle_if_more_results(),
            },
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> Drop for QueryResult<'a> {
    fn drop(&mut self) {
        while self.more_results_exists() {
            while let Some(_) = self.next() {}
        }
    }
}
