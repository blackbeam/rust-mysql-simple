use mysql_common::row::new_row;

use std::{collections::hash_map::HashMap, sync::Arc};

use crate::{Column, Conn, Result as MyResult, Row};

/// Mysql result set for text and binary protocols.
///
/// If you want to get rows from `QueryResult` you should rely on implementation
/// of `Iterator` over `MyResult<Row>` on `QueryResult`.
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

    fn handle_if_more_results(&mut self) -> Option<MyResult<Row>> {
        if self.conn.more_results_exists() {
            match self.conn.handle_result_set() {
                Ok(cols) => {
                    self.columns = Arc::new(cols);
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
        self.conn.affected_rows
    }

    /// Returns the last insert id for the current result set.
    pub fn last_insert_id(&self) -> u64 {
        self.conn.last_insert_id
    }

    /// Returns warnings count for the current result set.
    pub fn warnings(&self) -> u16 {
        self.conn.warnings
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// info.
    pub fn info(&self) -> Vec<u8> {
        self.conn
            .info
            .as_ref()
            .map(Clone::clone)
            .unwrap_or_else(Vec::new)
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
    type Item = MyResult<Row>;

    fn next(&mut self) -> Option<MyResult<Row>> {
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
