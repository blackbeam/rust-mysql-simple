use fnv::FnvHasher;
use mysql_common::row::new_row;

use std::collections::hash_map::HashMap;
use std::hash::BuildHasherDefault as BldHshrDflt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::{Column, Conn, Result as MyResult, Row, Stmt};

/// Possible ways to pass conn to a query result
#[derive(Debug)]
pub enum ResultConnRef<'a> {
    ViaConnRef(&'a mut Conn),
    ViaStmt(Stmt<'a>),
}

impl<'a> Deref for ResultConnRef<'a> {
    type Target = Conn;

    fn deref(&self) -> &Conn {
        match *self {
            ResultConnRef::ViaConnRef(ref conn_ref) => conn_ref,
            ResultConnRef::ViaStmt(ref stmt) => stmt.conn.deref(),
        }
    }
}

impl<'a> DerefMut for ResultConnRef<'a> {
    fn deref_mut(&mut self) -> &mut Conn {
        match *self {
            ResultConnRef::ViaConnRef(ref mut conn_ref) => conn_ref,
            ResultConnRef::ViaStmt(ref mut stmt) => stmt.conn.deref_mut(),
        }
    }
}

/// Mysql result set for text and binary protocols.
///
/// If you want to get rows from `QueryResult` you should rely on implementation
/// of `Iterator` over `MyResult<Row>` on `QueryResult`.
///
/// [`Row`](struct.Row.html) is the current row representation. To get something useful from
/// [`Row`](struct.Row.html) you should rely on `FromRow` trait implemented for tuples of
/// `FromValue` implementors up to arity 12, or on `FromValue` trait for rows with higher arity.
///
/// ```rust
/// # use mysql::Pool;
/// # use mysql::{Opts, OptsBuilder, from_row};
/// # fn get_opts() -> Opts {
/// #     let url = if let Ok(url) = std::env::var("DATABASE_URL") {
/// #         let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
/// #         if opts.get_db_name().expect("a database name is required").is_empty() {
/// #             panic!("database name is empty");
/// #         }
/// #         url
/// #     } else {
/// #         "mysql://root:password@127.0.0.1:3307/mysql".to_string()
/// #     };
/// #     Opts::from_url(&*url).unwrap()
/// # }
/// # let opts = get_opts();
/// # let pool = Pool::new(opts).unwrap();
/// let mut conn = pool.get_conn().unwrap();
///
/// for row in conn.prep_exec("SELECT ?, ?", (42, 2.5)).unwrap() {
///     let (a, b) = from_row(row.unwrap());
///     assert_eq!((a, b), (42u8, 2.5_f32));
/// }
/// ```
///
/// For more info on how to work with values please look at
/// [`Value`](../value/enum.Value.html) documentation.
#[derive(Debug)]
pub struct QueryResult<'a> {
    conn: ResultConnRef<'a>,
    columns: Arc<Vec<Column>>,
    is_bin: bool,
}

impl<'a> QueryResult<'a> {
    pub(crate) fn new(
        conn: ResultConnRef<'a>,
        columns: Vec<Column>,
        is_bin: bool,
    ) -> QueryResult<'a> {
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

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// affected rows.
    pub fn affected_rows(&self) -> u64 {
        self.conn.affected_rows
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// last insert id.
    pub fn last_insert_id(&self) -> u64 {
        self.conn.last_insert_id
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// warnings count.
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

    /// Returns HashMap which maps column names to column indexes.
    pub fn column_indexes(&self) -> HashMap<String, usize, BldHshrDflt<FnvHasher>> {
        let mut indexes = HashMap::default();
        for (i, column) in self.columns.iter().enumerate() {
            indexes.insert(column.name_str().into_owned(), i);
        }
        indexes
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `QueryResult`'s columns if any.
    pub fn columns_ref(&self) -> &[Column] {
        self.columns.as_ref()
    }

    /// This predicate will help you to consume multiple result sets.
    ///
    /// # Note
    ///
    /// Note that it'll also return `true` for unconsumed singe-result set.
    ///
    /// # Example
    ///
    /// ```ignore
    /// conn.query(r#"
    ///            CREATE PROCEDURE multi() BEGIN
    ///                SELECT 1;
    ///                SELECT 2;
    ///            END
    ///            "#);
    /// let mut result = conn.query("CALL multi()").unwrap();
    /// while result.more_results_exists() {
    ///     for x in result.by_ref() {
    ///         // On first iteration of `while` you will get result set from
    ///         // SELECT 1 and from SELECT 2 on second.
    ///     }
    /// }
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
