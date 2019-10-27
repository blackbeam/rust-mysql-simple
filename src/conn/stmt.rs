use mysql_common::packets::ComStmtClose;

use std::io;

use crate::conn::query_result::ResultConnRef;
use crate::conn::ConnRef;
use crate::{from_row, prelude::FromRow, Column, Conn, Params, PooledConn, QueryResult, Result};

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct InnerStmt {
    /// Positions and names of named parameters
    named_params: Option<Vec<String>>,
    params: Option<Vec<Column>>,
    columns: Option<Vec<Column>>,
    statement_id: u32,
    num_columns: u16,
    num_params: u16,
    warning_count: u16,
}

impl InnerStmt {
    pub fn from_payload(pld: &[u8], named_params: Option<Vec<String>>) -> io::Result<InnerStmt> {
        let stmt_packet = mysql_common::packets::parse_stmt_packet(pld)?;

        Ok(InnerStmt {
            named_params,
            statement_id: stmt_packet.statement_id(),
            num_columns: stmt_packet.num_columns(),
            num_params: stmt_packet.num_params(),
            warning_count: stmt_packet.warning_count(),
            params: None,
            columns: None,
        })
    }

    pub fn set_named_params(&mut self, named_params: Option<Vec<String>>) {
        self.named_params = named_params;
    }

    pub fn set_params(&mut self, params: Option<Vec<Column>>) {
        self.params = params;
    }

    pub fn set_columns(&mut self, columns: Option<Vec<Column>>) {
        self.columns = columns
    }

    pub fn columns(&self) -> Option<&[Column]> {
        self.columns.as_ref().map(AsRef::as_ref)
    }

    pub fn params(&self) -> Option<&[Column]> {
        self.params.as_ref().map(AsRef::as_ref)
    }

    pub fn id(&self) -> u32 {
        self.statement_id
    }

    pub fn num_params(&self) -> u16 {
        self.num_params
    }

    pub fn num_columns(&self) -> u16 {
        self.num_columns
    }

    pub fn named_params(&self) -> Option<&Vec<String>> {
        self.named_params.as_ref()
    }
}

/// Mysql [prepared statement][1].
///
/// [1]: http://dev.mysql.com/doc/internals/en/prepared-statements.html
#[derive(Debug)]
pub struct Stmt<'a> {
    stmt: InnerStmt,
    pub(crate) conn: ConnRef<'a>,
}

impl<'a> Stmt<'a> {
    pub(crate) fn new(stmt: InnerStmt, conn: &'a mut Conn) -> Stmt<'a> {
        Stmt {
            stmt,
            conn: ConnRef::ViaConnRef(conn),
        }
    }

    pub(crate) fn new_pooled(stmt: InnerStmt, pooled_conn: PooledConn) -> Stmt<'a> {
        Stmt {
            stmt,
            conn: ConnRef::ViaPooledConn(pooled_conn),
        }
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s params if any.
    pub fn params_ref(&self) -> Option<&[Column]> {
        self.stmt.params()
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s columns if any.
    pub fn columns_ref(&self) -> Option<&[Column]> {
        self.stmt.columns()
    }

    /// Returns index of a `Stmt`'s column by name.
    pub fn column_index<T: AsRef<str>>(&self, name: T) -> Option<usize> {
        match self.stmt.columns() {
            None => None,
            Some(columns) => {
                let name = name.as_ref().as_bytes();
                for (i, c) in columns.iter().enumerate() {
                    if c.name_ref() == name {
                        return Some(i);
                    }
                }
                None
            }
        }
    }

    /// Executes prepared statement with parameters passed as a [`Into<Params>`] implementor.
    ///
    /// ```rust
    /// # use mysql::{Pool, Opts, OptsBuilder, from_value, from_row, Value};
    /// # use mysql::prelude::ToValue;
    /// # use std::default::Default;
    /// # use std::iter::repeat;
    /// # fn get_opts() -> Opts {
    /// #     let user = "root";
    /// #     let addr = "127.0.0.1";
    /// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
    /// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
    /// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
    /// #                                .unwrap_or(3307);
    /// #     let mut builder = OptsBuilder::default();
    /// #     builder.user(Some(user.to_string()))
    /// #            .pass(Some(pwd))
    /// #            .ip_or_hostname(Some(addr.to_string()))
    /// #            .tcp_port(port)
    /// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'".to_owned()]);
    /// #     builder.into()
    /// # }
    /// # let opts = get_opts();
    /// # let pool = Pool::new(opts).unwrap();
    /// let mut stmt0 = pool.prepare("SELECT 42").unwrap();
    /// let mut stmt1 = pool.prepare("SELECT ?").unwrap();
    /// let mut stmt2 = pool.prepare("SELECT ?, ?").unwrap();
    /// let mut stmt13 = pool.prepare("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?").unwrap();
    ///
    /// // It is better to pass params as a tuple when executing statements of arity <= 12
    /// for row in stmt0.execute(()).unwrap() {
    ///     let cell = from_row::<u8>(row.unwrap());
    ///     assert_eq!(cell, 42u8);
    /// }
    /// // just do not forget about trailing comma in case of arity = 1
    /// for row in stmt1.execute((42,)).unwrap() {
    ///     let cell = from_row::<u8>(row.unwrap());
    ///     assert_eq!(cell, 42u8);
    /// }
    ///
    /// // If you don't want to lose ownership of param, then you should pass it by reference
    /// let word = "hello".to_string();
    /// for row in stmt2.execute((&word, &word)).unwrap() {
    ///     let (cell1, cell2) = from_row::<(String, String)>(row.unwrap());
    ///     assert_eq!(cell1, "hello");
    ///     assert_eq!(cell2, "hello");
    /// }
    ///
    /// // If you want to execute statement of arity > 12, then you can pass params as &[&ToValue].
    /// let params: &[&dyn ToValue] = &[&1, &2, &3, &4, &5, &6, &7, &8, &9, &10, &11, &12, &13];
    /// for row in stmt13.execute(params).unwrap() {
    ///     let row: Vec<u8> = row.unwrap().unwrap().into_iter().map(from_value::<u8>).collect();
    ///     assert_eq!(row, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
    /// }
    /// // but be aware of implicit copying, so if you have huge params and do not care
    /// // about ownership, then better to use plain Vec<Value>.
    /// let mut params: Vec<Value> = Vec::with_capacity(13);
    /// for i in 1..14 {
    ///     params.push(repeat('A').take(i * 1000).collect::<String>().into());
    /// }
    /// for row in stmt13.execute(params).unwrap() {
    ///     let row = row.unwrap();
    ///     let row: Vec<String> = row.unwrap().into_iter().map(from_value::<String>).collect();
    ///     for i in 1..14 {
    ///         assert_eq!(row[i-1], repeat('A').take(i * 1000).collect::<String>());
    ///     }
    /// }
    /// ```
    pub fn execute<T: Into<Params>>(&mut self, params: T) -> Result<QueryResult> {
        self.conn.execute(&self.stmt, params)
    }

    /// See [`Conn::first_exec`](struct.Conn.html#method.first_exec).
    pub fn first_exec<P, T>(&mut self, params: P) -> Result<Option<T>>
    where
        P: Into<Params>,
        T: FromRow,
    {
        self.execute(params).and_then(|mut result| {
            if let Some(row) = result.next() {
                row.map(|x| Some(from_row(x)))
            } else {
                Ok(None)
            }
        })
    }

    pub(crate) fn prep_exec<T: Into<Params>>(mut self, params: T) -> Result<QueryResult<'a>> {
        let columns = self.conn._execute(&self.stmt, params.into())?;
        Ok(QueryResult::new(
            ResultConnRef::ViaStmt(self),
            columns,
            true,
        ))
    }
}

impl<'a> Drop for Stmt<'a> {
    fn drop(&mut self) {
        if self.conn.stmt_cache.get_cap() == 0 {
            let com_stmt_close = ComStmtClose::new(self.stmt.id());
            let _ = self.conn.write_command_raw(com_stmt_close);
        }
    }
}
