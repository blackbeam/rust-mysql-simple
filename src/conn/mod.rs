use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs;
use std::fmt;
use std::hash::BuildHasherDefault as BldHshrDflt;
use std::io;
use std::io::Read;
use std::io::Write as NewWrite;
use std::net;
use std::ops::{
    Deref,
    DerefMut,
    Index,
};
use std::path;
use std::str::from_utf8;
use std::sync::Arc;

use super::consts;
use super::consts::Command;
use super::consts::ColumnType;
use super::io::Read as MyRead;
use super::io::Write;
use super::io::Stream;
use super::io::TcpStream::Insecure;
use super::error::Error::{
    IoError,
    MySqlError,
    DriverError
};
use super::error::DriverError::{
    CouldNotConnect,
    UnsupportedProtocol,
    Protocol41NotSet,
    UnexpectedPacket,
    MismatchedStmtParams,
    NamedParamsForPositionalQuery,
    SetupError,
    ReadOnlyTransNotSupported,
};
use super::error::Result as MyResult;
#[cfg(feature = "ssl")]
use super::error::DriverError::SslNotSupported;
use named_params::parse_named_params;
use super::scramble::scramble;
use super::packet::{OkPacket, EOFPacket, ErrPacket, HandshakePacket, ServerVersion};
use super::parser::column_def;
use super::value::{
    Params,
    Value,
    from_value_opt,
    from_value,
    FromValue,
};
use super::value::Value::{NULL, Int, UInt, Float, Bytes, Date, Time};

use bufstream::BufStream;
use byteorder::LittleEndian as LE;
use byteorder::{ReadBytesExt, WriteBytesExt};
use fnv::FnvHasher;
use twox_hash::XxHash;
#[cfg(feature = "socket")]
use unix_socket as us;
#[cfg(feature = "pipe")]
use named_pipe as np;

pub mod pool;
mod opts;
pub use self::opts::Opts;
pub use self::opts::OptsBuilder;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
}

impl<'a> Transaction<'a> {
    fn new(conn: &'a mut Conn) -> Transaction<'a> {
        Transaction {
            conn: ConnRef::ViaConnRef(conn),
            committed: false,
            rolled_back: false,
        }
    }

    fn new_pooled(conn: pool::PooledConn) -> Transaction<'a> {
        Transaction {
            conn: ConnRef::ViaPooledConn(conn),
            committed: false,
            rolled_back: false,
        }
    }

    /// See [`Conn::query`](struct.Conn.html#method.query).
    pub fn query<'c, T: AsRef<str> + 'c>(&'c mut self, query: T) -> MyResult<QueryResult<'c>> {
        self.conn.query(query)
    }

    /// See [`Conn::first`](struct.Conn.html#method.first).
    pub fn first<T: AsRef<str>>(&mut self, query: T) -> MyResult<Option<Row>> {
        self.query(query).and_then(|result| {
            for row in result {
                return row.map(Some);
            }
            return Ok(None)
        })
    }

    /// See [`Conn::prepare`](struct.Conn.html#method.prepare).
    pub fn prepare<'c, T: AsRef<str> + 'c>(&'c mut self, query: T) -> MyResult<Stmt<'c>> {
        self.conn.prepare(query)
    }

    /// See [`Conn::prep_exec`](struct.Conn.html#method.prep_exec).
    pub fn prep_exec<'c, A: AsRef<str> + 'c, T: Into<Params>>(&'c mut self, query: A, params: T) -> MyResult<QueryResult<'c>> {
        self.conn.prep_exec(query, params)
    }

    /// See [`Conn::first_exec`](struct.Conn.html#method.first_exec).
    pub fn first_exec<Q, P>(&mut self, query: Q, params: P) -> MyResult<Option<Row>>
    where Q: AsRef<str>,
          P: Into<Params>,
    {
        self.prep_exec(query, params).and_then(|result| {
            for row in result {
                return row.map(Some);
            }
            return Ok(None)
        })
    }

    /// Will consume and commit transaction.
    pub fn commit(mut self) -> MyResult<()> {
        try!(self.conn.query("COMMIT"));
        self.committed = true;
        Ok(())
    }

    /// Will consume and rollback transaction. You also can rely on `Drop` implementation but it
    /// will swallow errors.
    pub fn rollback(mut self) -> MyResult<()> {
        try!(self.conn.query("ROLLBACK"));
        self.rolled_back = true;
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if ! self.committed && ! self.rolled_back {
            let _ = self.conn.query("ROLLBACK");
        }
    }
}

/***
 *     .d8888b.  888                  888
 *    d88P  Y88b 888                  888
 *    Y88b.      888                  888
 *     "Y888b.   888888 88888b.d88b.  888888
 *        "Y88b. 888    888 "888 "88b 888
 *          "888 888    888  888  888 888
 *    Y88b  d88P Y88b.  888  888  888 Y88b.
 *     "Y8888P"   "Y888 888  888  888  "Y888
 *
 *
 *
 */
#[derive(Eq, PartialEq, Clone, Debug)]
struct InnerStmt {
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
    fn from_payload(pld: &[u8], named_params: Option<Vec<String>>) -> io::Result<InnerStmt> {
        let mut reader = &pld[1..];
        let statement_id = try!(reader.read_u32::<LE>());
        let num_columns = try!(reader.read_u16::<LE>());
        let num_params = try!(reader.read_u16::<LE>());
        let warning_count = try!(reader.read_u16::<LE>());
        Ok(InnerStmt {
            named_params: named_params,
            statement_id: statement_id,
            num_columns: num_columns,
            num_params: num_params,
            warning_count: warning_count,
            params: None,
            columns: None,
        })
    }
}

/// Possible ways to pass conn to a statement or transaction
#[derive(Debug)]
enum ConnRef<'a> {
    ViaConnRef(&'a mut Conn),
    ViaPooledConn(pool::PooledConn),
}

impl<'a> Deref for ConnRef<'a> {
    type Target = Conn;

    fn deref<'c>(&'c self) -> &'c Conn {
        match *self {
            ConnRef::ViaConnRef(ref conn_ref) => conn_ref,
            ConnRef::ViaPooledConn(ref conn) => conn.as_ref(),
        }
    }
}

impl<'a> DerefMut for ConnRef<'a> {
    fn deref_mut<'c>(&'c mut self) -> &'c mut Conn {
        match *self {
            ConnRef::ViaConnRef(ref mut conn_ref) => conn_ref,
            ConnRef::ViaPooledConn(ref mut conn) => conn.as_mut(),
        }
    }
}

/// Mysql
/// [prepared statement](http://dev.mysql.com/doc/internals/en/prepared-statements.html).
#[derive(Debug)]
pub struct Stmt<'a> {
    stmt: InnerStmt,
    conn: ConnRef<'a>,
}

impl<'a> Stmt<'a> {
    fn new(stmt: InnerStmt, conn: &'a mut Conn) -> Stmt<'a> {
        Stmt {
            stmt: stmt,
            conn: ConnRef::ViaConnRef(conn),
        }
    }

    fn new_pooled(stmt: InnerStmt, pooled_conn: pool::PooledConn) -> Stmt<'a> {
        Stmt {
            stmt: stmt,
            conn: ConnRef::ViaPooledConn(pooled_conn),
        }
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s params if any.
    pub fn params_ref(&self) -> Option<&[Column]> {
        match self.stmt.params {
            Some(ref params) => Some(params.as_ref()),
            None => None
        }
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s columns if any.
    pub fn columns_ref(&self) -> Option<&[Column]> {
        match self.stmt.columns {
            Some(ref columns) => Some(columns.as_ref()),
            None => None
        }
    }

    /// Returns index of a `Stmt`'s column by name.
    pub fn column_index<T: AsRef<str>>(&self, name: T) -> Option<usize> {
        match self.stmt.columns {
            None => None,
            Some(ref columns) => {
                let name = name.as_ref().as_bytes();
                for (i, c) in columns.iter().enumerate() {
                    if c.name() == name {
                        return Some(i)
                    }
                }
                None
            }
        }
    }

    /// Executes prepared statement with parameters passed as a [`Into<Params>`] implementor.
    ///
    /// ```rust
    /// # use mysql::conn::pool;
    /// # use mysql::conn::{Opts, OptsBuilder};
    /// # use mysql::value::{from_value, from_row, ToValue, Value};
    /// # use std::thread::Thread;
    /// # use std::default::Default;
    /// # use std::iter::repeat;
    /// # fn get_opts() -> Opts {
    /// #     let USER = "root";
    /// #     let ADDR = "127.0.0.1";
    /// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
    /// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
    /// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
    /// #                                .unwrap_or(3307);
    /// #     let mut builder = OptsBuilder::default();
    /// #     builder.user(Some(USER.to_string()))
    /// #            .pass(Some(pwd))
    /// #            .ip_or_hostname(Some(ADDR.to_string()))
    /// #            .tcp_port(port)
    /// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'".to_owned()]);
    /// #     builder.into()
    /// # }
    /// # let opts = get_opts();
    /// # let pool = pool::Pool::new(opts).unwrap();
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
    /// let params: &[&ToValue] = &[&1, &2, &3, &4, &5, &6, &7, &8, &9, &10, &11, &12, &13];
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
    pub fn execute<'s, T: Into<Params>>(&'s mut self, params: T) -> MyResult<QueryResult<'s>> {
        self.conn.execute(&self.stmt, params)
    }

    /// See [`Conn::first_exec`](struct.Conn.html#method.first_exec).
    pub fn first_exec<P>(&mut self, params: P) -> MyResult<Option<Row>>
    where P: Into<Params>,
    {
        self.execute(params).and_then(|result| {
            for row in result {
                return row.map(Some);
            }
            return Ok(None)
        })
    }

    fn prep_exec<T: Into<Params>>(mut self, params: T) -> MyResult<QueryResult<'a>> {
        let (columns, ok_packet) = try!(self.conn._execute(&self.stmt, params.into()));
        Ok(QueryResult::new(ResultConnRef::ViaStmt(self), columns, ok_packet, true))
    }
}

/***
 *     .d8888b.           888
 *    d88P  Y88b          888
 *    888    888          888
 *    888         .d88b.  888 888  888 88888b.d88b.  88888b.
 *    888        d88""88b 888 888  888 888 "888 "88b 888 "88b
 *    888    888 888  888 888 888  888 888  888  888 888  888
 *    Y88b  d88P Y88..88P 888 Y88b 888 888  888  888 888  888
 *     "Y8888P"   "Y88P"  888  "Y88888 888  888  888 888  888
 *
 *
 *
 */

/// Mysql
/// [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Column {
    payload: Vec<u8>,
    schema: (usize, usize),
    table: (usize, usize),
    org_table: (usize, usize),
    name: (usize, usize),
    org_name: (usize, usize),
    default_values: Option<(usize, usize)>,
    pub column_length: u32,
    pub character_set: u16,
    pub flags: consts::ColumnFlags,
    pub column_type: consts::ColumnType,
    pub decimals: u8,
}

impl Column {
    pub fn from_payload(pld: Vec<u8>) -> io::Result<Column> {
        let (
            schema,
            table,
            org_table,
            name,
            org_name,
            character_set,
            column_length,
            column_type,
            flags,
            decimals,
            default_values
        ) = {
            let (
                schema,
                table,
                org_table,
                name,
                org_name,
                character_set,
                column_length,
                column_type,
                flags,
                decimals,
                default_values
            ) = match column_def(&*pld) {
                ::nom::IResult::Done(_, def) => def,
                _ => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Can't parse column"))
            };
            let schema = (schema.as_ptr() as usize - pld.as_ptr() as usize, schema.len());
            let table = (table.as_ptr() as usize - pld.as_ptr() as usize, table.len());
            let org_table = (org_table.as_ptr() as usize - pld.as_ptr() as usize, org_table.len());
            let name = (name.as_ptr() as usize - pld.as_ptr() as usize, name.len());
            let org_name = (org_name.as_ptr() as usize - pld.as_ptr() as usize, org_name.len());
            let default_values = default_values.map(|v| {
                (v.as_ptr() as usize - pld.as_ptr() as usize, v.len())
            });
            (schema, table, org_table, name, org_name, character_set, column_length, column_type,
             flags, decimals, default_values)
        };

        Ok(Column {
            payload: pld,
            schema: schema,
            table: table,
            org_table: org_table,
            name: name,
            org_name: org_name,
            default_values: default_values,
            column_length: column_length,
            character_set: character_set,
            flags: consts::ColumnFlags::from_bits_truncate(flags),
            column_type: consts::ColumnType::from(column_type),
            decimals: decimals,
        })
    }

    pub fn schema<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.schema.0..self.schema.0+self.schema.1]
    }

    pub fn table<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.table.0..self.table.0+self.table.1]
    }

    pub fn org_table<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.org_table.0..self.org_table.0+self.org_table.1]
    }

    pub fn name<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.name.0..self.name.0+self.name.1]
    }

    pub fn org_name<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.org_name.0..self.org_name.0+self.org_name.1]
    }

    pub fn default_values<'a>(&'a self) -> Option<&'a [u8]> {
        self.default_values.map(|(offset, len)| {
            &self.payload[offset..offset + len]
        })
    }
}

///// Mysql
///// [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition).
//#[derive(Clone, Eq, PartialEq, Debug)]
//pub struct Column {
//    /// Schema name.
//    pub schema: Vec<u8>,
//    /// Virtual table name.
//    pub table: Vec<u8>,
//    /// Phisical table name.
//    pub org_table: Vec<u8>,
//    /// Virtual column name.
//    pub name: Vec<u8>,
//    /// Phisical column name.
//    pub org_name: Vec<u8>,
//    /// Default values.
//    pub default_values: Vec<u8>,
//    /// Maximum length of the field.
//    pub column_length: u32,
//    /// Column character set.
//    pub character_set: u16,
//    /// Flags.
//    pub flags: consts::ColumnFlags,
//    /// Column type.
//    pub column_type: consts::ColumnType,
//    /// Max shown decimal digits
//    pub decimals: u8
//}
//
//impl Column {
//    #[inline]
//    pub fn from_payload(command: u8, pld: &[u8]) -> io::Result<Column> {
//        let mut reader = pld.as_ref();
//        // Skip catalog
//        let _ = try!(reader.read_lenenc_bytes());
//        let schema = try!(reader.read_lenenc_bytes());
//        let table = try!(reader.read_lenenc_bytes());
//        let org_table = try!(reader.read_lenenc_bytes());
//        let name = try!(reader.read_lenenc_bytes());
//        let org_name = try!(reader.read_lenenc_bytes());
//        let _ = try!(reader.read_lenenc_int());
//        let character_set = try!(reader.read_u16::<LE>());
//        let column_length = try!(reader.read_u32::<LE>());
//        let column_type = try!(reader.read_u8());
//        let flags = consts::ColumnFlags::from_bits_truncate(try!(reader.read_u16::<LE>()));
//        let decimals = try!(reader.read_u8());
//        // skip filler
//        try!(reader.read_u16::<LE>());
//        let mut default_values = Vec::with_capacity(reader.len());
//        if command == Command::COM_FIELD_LIST as u8 {
//            let len = try!(reader.read_lenenc_int());
//            try!(reader.take(len).read_to_end(&mut default_values));
//        }
//        Ok(Column{schema: schema,
//                  table: table,
//                  org_table: org_table,
//                  name: name,
//                  org_name: org_name,
//                  character_set: character_set,
//                  column_length: column_length,
//                  column_type: From::from(column_type),
//                  flags: flags,
//                  decimals: decimals,
//                  default_values: default_values})
//    }
//}

/// Mysql row representation.
///
/// It allows you to move column values out of a row with `Row::take` method but note that it
/// makes row incomplete. Calls to `from_row_opt` on incomplete row will return
/// `Error::FromRowError` and also numerical indexing on taken columns will panic.
///
/// ```rust
/// # use mysql::conn::pool;
/// # use mysql::conn::{Opts, OptsBuilder};
/// # use mysql::value::{from_value, from_row, ToValue, Value};
/// # use std::thread::Thread;
/// # use std::default::Default;
/// # use std::iter::repeat;
/// # fn get_opts() -> Opts {
/// #     let USER = "root";
/// #     let ADDR = "127.0.0.1";
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     let mut builder = OptsBuilder::default();
/// #     builder.user(Some(USER))
/// #            .pass(Some(pwd))
/// #            .ip_or_hostname(Some(ADDR))
/// #            .tcp_port(port)
/// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
/// #     builder.into()
/// # }
/// # let opts = get_opts();
/// # let pool = pool::Pool::new_manual(1, 1, opts).unwrap();
/// # pool.prep_exec("CREATE TEMPORARY TABLE tmp.Users (id INT, name TEXT, age INT, email TEXT)", ());
/// # pool.prep_exec("INSERT INTO tmp.Users (id, name, age, email) VALUES (?, ?, ?, ?)",
/// #                (1, "John", 17, "foo@bar.baz"));
/// pool.prep_exec("SELECT * FROM tmp.Users", ()).map(|mut result| {
///     let mut row = result.next().unwrap().unwrap();
///     let id: u32 = row.take("id").unwrap();
///     let name: String = row.take("name").unwrap();
///     let age: u32 = row.take("age").unwrap();
///     let email: String = row.take("email").unwrap();
///
///     assert_eq!(1, id);
///     assert_eq!("John", name);
///     assert_eq!(17, age);
///     assert_eq!("foo@bar.baz", email);
/// });
/// ```
///
#[derive(Clone, PartialEq, Debug)]
pub struct Row {
    values: Vec<Option<Value>>,
    columns: Arc<Vec<Column>>
}

impl Row {
    /// Creates instance of `Row` from raw row representation
    #[doc(hidden)]
    pub fn new(raw_row: Vec<Value>, columns: Arc<Vec<Column>>) -> Row {
        Row {
            values: raw_row.into_iter().map(|value| Some(value)).collect(),
            columns: columns
        }
    }

    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns reference to the value of a column with index `index` if it exists and wasn't taken
    /// by `Row::take` method.
    ///
    /// Non panicking version of `row[usize]`.
    pub fn as_ref(&self, index: usize) -> Option<&Value> {
        self.values.get(index).and_then(|x| x.as_ref())
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` earlier,
    /// then will convert it to `T`.
    pub fn get<T, I>(&mut self, index: I) -> Option<T>
    where T: FromValue,
          I: ColumnIndex {
        index.idx(&*self.columns).and_then(|idx| {
            self.values.get(idx).and_then(|x| x.as_ref()).map(|x| from_value::<T>(x.clone()))
        })
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take<T, I>(&mut self, index: I) -> Option<T>
    where T: FromValue,
          I: ColumnIndex {
        index.idx(&*self.columns).and_then(|idx| {
            self.values.get_mut(idx).and_then(|x| x.take()).map(from_value::<T>)
        })
    }

    /// Unwraps values of a row.
    ///
    /// # Panics
    ///
    /// Panics if any of columns was taken by `take` method.
    pub fn unwrap(self) -> Vec<Value> {
        self.values.into_iter()
        .map(|x| x.expect("Can't unwrap row if some of columns was taken"))
        .collect()
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: Value) {
        self.values[index] = Some(value);
    }
}

impl Index<usize> for Row {
    type Output = Value;

    fn index<'a>(&'a self, index: usize) -> &'a Value {
        self.values[index].as_ref().unwrap()
    }
}

impl<'a> Index<&'a str> for Row {
    type Output = Value;

    fn index<'r>(&'r self, index: &'a str) -> &'r Value {
        for (i, column) in self.columns.iter().enumerate() {
            if column.name() == index.as_bytes() {
                return self.values[i].as_ref().unwrap();
            }
        }
        panic!("No such column: `{}`", index);
    }
}

pub trait ColumnIndex {
    fn idx(&self, columns: &Vec<Column>) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn idx(&self, columns: &Vec<Column>) -> Option<usize> {
        if *self >= columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> ColumnIndex for &'a str {
    fn idx(&self, columns: &Vec<Column>) -> Option<usize> {
        for (i, c) in columns.iter().enumerate() {
            if c.name() == self.as_bytes() {
                return Some(i);
            }
        }
        None
    }
}

/***
 *    888b     d888           .d8888b.
 *    8888b   d8888          d88P  Y88b
 *    88888b.d88888          888    888
 *    888Y88888P888 888  888 888         .d88b.  88888b.  88888b.
 *    888 Y888P 888 888  888 888        d88""88b 888 "88b 888 "88b
 *    888  Y8P  888 888  888 888    888 888  888 888  888 888  888
 *    888   "   888 Y88b 888 Y88b  d88P Y88..88P 888  888 888  888
 *    888       888  "Y88888  "Y8888P"   "Y88P"  888  888 888  888
 *                       888
 *                  Y8b d88P
 *                   "Y88P"
 */

/// Mysql connection.
#[derive(Debug)]
pub struct Conn {
    opts: Opts,
    stream: Option<Stream>,
    stmts: HashMap<String, InnerStmt, BldHshrDflt<XxHash>>,
    server_version: ServerVersion,
    affected_rows: u64,
    last_insert_id: u64,
    max_allowed_packet: usize,
    capability_flags: consts::CapabilityFlags,
    connection_id: u32,
    status_flags: consts::StatusFlags,
    seq_id: u8,
    character_set: u8,
    last_command: u8,
    connected: bool,
    has_results: bool,
}

impl Conn {
    fn empty<T: Into<Opts>>(opts: T) -> Conn {
        Conn {
            opts: opts.into(),
            stream: None,
            stmts: HashMap::default(),
            seq_id: 0u8,
            capability_flags: consts::CapabilityFlags::empty(),
            status_flags: consts::StatusFlags::empty(),
            connection_id: 0u32,
            character_set: 0u8,
            affected_rows: 0u64,
            last_insert_id: 0u64,
            last_command: 0u8,
            max_allowed_packet: consts::MAX_PAYLOAD_LEN,
            connected: false,
            has_results: false,
            server_version: (0, 0, 0),
        }
    }

    #[cfg(all(not(feature = "ssl"), feature = "socket", not(feature = "pipe")))]
    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        try!(conn.connect_stream());
        try!(conn.connect());
        if conn.opts.get_unix_addr().is_none() && conn.opts.get_prefer_socket() {
            if conn.opts.addr_is_loopback() {
                match conn.get_system_var("socket") {
                    Some(path) => {
                        let path = from_value::<String>(path);
                        let mut new_opts = OptsBuilder::from_opts(conn.opts.clone());
                        new_opts.unix_addr(Some(path));
                        return Conn::new(new_opts).or(Ok(conn));
                    },
                    _ => return Ok(conn)
                }
            }
        }
        for cmd in conn.opts.get_init().clone() {
            try!(conn.query(cmd));
        }
        return Ok(conn);
    }

    #[cfg(all(not(feature = "ssl"), not(feature = "socket"), feature = "pipe"))]
    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        try!(conn.connect_stream());
        try!(conn.connect());
        if conn.opts.get_pipe_name().is_none() && conn.opts.get_prefer_socket() {
            if conn.opts.addr_is_loopback() {
                match conn.get_system_var("socket") {
                    Some(name) => {
                        let name = from_value::<String>(name);
                        let mut new_opts = OptsBuilder::from_opts(conn.opts.clone());
                        new_opts.pipe_name(Some(name));
                        return Conn::new(new_opts).or(Ok(conn));
                    },
                    _ => return Ok(conn)
                }
            }
        }
        for cmd in conn.opts.get_init().clone() {
            try!(conn.query(cmd));
        }
        return Ok(conn);
    }

    #[cfg(all(feature = "ssl", feature = "socket"))]
    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        try!(conn.connect_stream());
        try!(conn.connect());
        if let &None = conn.opts.get_ssl_opts() {
            if conn.opts.get_unix_addr().is_none() && conn.opts.get_prefer_socket() {
                if conn.opts.addr_is_loopback() {
                    match conn.get_system_var("socket") {
                        Some(path) => {
                            let path = from_value::<String>(path);
                            let mut new_opts = OptsBuilder::from_opts(conn.opts.clone());
                            new_opts.unix_addr(Some(path));
                            return Conn::new(new_opts).or(Ok(conn));
                        },
                        _ => return Ok(conn)
                    }
                }
            }
        }
        for cmd in conn.opts.get_init().clone() {
            try!(conn.query(cmd));
        }
        return Ok(conn);
    }

    #[cfg(all(feature = "ssl", not(feature = "socket"), feature = "pipe"))]
    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        try!(conn.connect_stream());
        try!(conn.connect());
        if let &None = conn.opts.get_ssl_opts() {
            if conn.opts.get_unix_addr().is_none() && conn.opts.get_prefer_socket() {
                if conn.opts.addr_is_loopback() {
                    match conn.get_system_var("socket") {
                        Some(name) => {
                            let name = from_value::<String>(path);
                            let mut new_opts = OptsBuilder::from_opts(opts);
                            new_opts.pipe_name(Some(name));
                            return Conn::new(new_opts).or(Ok(conn));
                        },
                        _ => return Ok(conn)
                    }
                }
            }
        }
        for cmd in conn.opts.get_init().clone() {
            try!(conn.query(cmd));
        }
        return Ok(conn);
    }

    #[cfg(all(not(feature = "ssl"), not(feature = "socket"), not(feature = "pipe")))]
    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        try!(conn.connect_stream());
        try!(conn.connect());
        for cmd in conn.opts.get_init().clone() {
            try!(conn.query(cmd));
        }
        return Ok(conn);
    }

    #[cfg(all(feature = "ssl", not(feature = "socket"), not(feature = "pipe")))]
    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        try!(conn.connect_stream());
        try!(conn.connect());
        for cmd in conn.opts.get_init().clone() {
            try!(conn.query(cmd));
        }
        return Ok(conn);
    }

    fn soft_reset(&mut self) -> MyResult<()> {
        try!(self.write_command(Command::COM_RESET_CONNECTION));
        self.read_packet().and_then(|pld| {
            match pld[0] {
                0 => {
                    let ok = try!(OkPacket::from_payload(&*pld));
                    self.handle_ok(&ok);
                    self.last_command = 0;
                    self.stmts.clear();
                    Ok(())
                },
                _ => {
                    let err = try!(ErrPacket::from_payload(&*pld, self.capability_flags));
                    Err(MySqlError(err.into()))
                },
            }
        })
    }

    fn hard_reset(&mut self) -> MyResult<()> {
        self.stream = None;
        self.stmts.clear();
        self.seq_id = 0;
        self.capability_flags = consts::CapabilityFlags::empty();
        self.status_flags = consts::StatusFlags::empty();
        self.connection_id = 0;
        self.character_set = 0;
        self.affected_rows = 0;
        self.last_insert_id = 0;
        self.last_command = 0;
        self.max_allowed_packet = consts::MAX_PAYLOAD_LEN;
        self.connected = false;
        self.has_results = false;
        try!(self.connect_stream());
        self.connect()
    }

    /// Resets `MyConn` (drops state then reconnects).
    pub fn reset(&mut self) -> MyResult<()> {
        if self.server_version > (5, 7, 2) {
            match self.soft_reset() {
                Ok(_) => Ok(()),
                _ => self.hard_reset()
            }
        } else {
            self.hard_reset()
        }
    }

    fn get_mut_stream<'a>(&'a mut self) -> &'a mut Stream {
        self.stream.as_mut().unwrap()
    }

    #[cfg(feature = "openssl")]
    fn switch_to_ssl(&mut self) -> MyResult<()> {
        if self.stream.is_some() {
            let stream = self.stream.take().unwrap();
            let stream = try!(stream.make_secure(self.opts.get_verify_peer(),
                                                 self.opts.get_ssl_opts()));
            self.stream = Some(stream);
        }
        Ok(())
    }

    #[cfg(all(not(feature = "socket"), feature = "pipe"))]
    fn connect_stream(&mut self) -> MyResult<()> {
        if let &Some(ref pipe_name) = self.opts.get_pipe_name() {
            let mut full_name: String = r"\\.\pipe\".into();
            full_name.push_str(&**pipe_name);
            match np::PipeClient::connect(full_name) {
                Ok(mut pipe_stream) => {
                    pipe_stream.set_read_timeout(self.opts.get_read_timeout().clone());
                    pipe_stream.set_write_timeout(self.opts.get_write_timeout().clone());
                    self.stream = Some(Stream::PipeStream(BufStream::new(pipe_stream)));
                    Ok(())
                },
                Err(e) => {
                    let addr = format!(r"\\.\pipe\{}", pipe_name);
                    let desc = format!("{}", e);
                    Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
                }
            }
        } else if let &Some(ref ip_or_hostname) = self.opts.get_ip_or_hostname() {
            match net::TcpStream::connect(&(&**ip_or_hostname, self.opts.get_tcp_port())) {
                Ok(stream) => {
                    try!(stream.set_read_timeout(self.opts.get_read_timeout().clone()));
                    try!(stream.set_write_timeout(self.opts.get_write_timeout().clone()));
                    self.stream = Some(Stream::TcpStream(Some(Insecure(BufStream::new(stream)))));
                    Ok(())
                },
                Err(e) => {
                    let addr = format!("{}:{}", ip_or_hostname, self.opts.get_tcp_port());
                    let desc = format!("{}", e);
                    Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
                }
            }
        } else {
            Err(DriverError(CouldNotConnect(None)))
        }
    }

    #[cfg(all(feature = "socket", not(feature = "pipe")))]
    fn connect_stream(&mut self) -> MyResult<()> {
        if let &Some(ref unix_addr) = self.opts.get_unix_addr() {
            match us::UnixStream::connect(unix_addr) {
                Ok(stream) => {
                    try!(stream.set_read_timeout(self.opts.get_read_timeout().clone()));
                    try!(stream.set_write_timeout(self.opts.get_write_timeout().clone()));
                    self.stream = Some(Stream::UnixStream(BufStream::new(stream)));
                    Ok(())
                },
                Err(e) => {
                    let addr = format!("{}", unix_addr.display());
                    let desc = format!("{}", e);
                    Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
                }
            }
        } else if let &Some(ref ip_or_hostname) = self.opts.get_ip_or_hostname() {
            match net::TcpStream::connect(&(&**ip_or_hostname, self.opts.get_tcp_port())) {
                Ok(stream) => {
                    try!(stream.set_read_timeout(self.opts.get_read_timeout().clone()));
                    try!(stream.set_write_timeout(self.opts.get_write_timeout().clone()));
                    self.stream = Some(Stream::TcpStream(Some(Insecure(BufStream::new(stream)))));
                    Ok(())
                },
                Err(e) => {
                    let addr = format!("{}:{}", ip_or_hostname, self.opts.get_tcp_port());
                    let desc = format!("{}", e);
                    Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
                }
            }
        } else {
            Err(DriverError(CouldNotConnect(None)))
        }
    }

    #[cfg(all(not(feature = "socket"), not(feature = "pipe")))]
    fn connect_stream(&mut self) -> MyResult<()> {
        if let &Some(ref ip_or_hostname) = self.opts.get_ip_or_hostname() {
            match net::TcpStream::connect(&(&**ip_or_hostname, self.opts.get_tcp_port())) {
                Ok(stream) => {
                    try!(stream.set_read_timeout(self.opts.get_read_timeout().clone()));
                    try!(stream.set_write_timeout(self.opts.get_write_timeout().clone()));
                    self.stream = Some(Stream::TcpStream(Some(Insecure(BufStream::new(stream)))));
                    Ok(())
                },
                Err(e) => {
                    let addr = format!("{}:{}", ip_or_hostname, self.opts.get_tcp_port());
                    let desc = format!("{}", e);
                    Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
                }
            }
        } else {
            Err(DriverError(CouldNotConnect(None)))
        }
    }

    fn read_packet(&mut self) -> MyResult<Vec<u8>> {
        let old_seq_id = self.seq_id;
        let (data, seq_id) = try!(self.get_mut_stream().read_packet(old_seq_id));
        self.seq_id = seq_id;
        Ok(data)
    }

    fn write_packet(&mut self, data: &[u8]) -> MyResult<()> {
        let seq_id = self.seq_id;
        let max_allowed_packet = self.max_allowed_packet;
        self.seq_id = try!(self.get_mut_stream().write_packet(data, seq_id, max_allowed_packet));
        Ok(())
    }

    fn handle_handshake(&mut self, hp: &HandshakePacket) {
        self.capability_flags = hp.capability_flags;
        self.status_flags = hp.status_flags;
        self.connection_id = hp.connection_id;
        self.character_set = hp.character_set;
        self.server_version = hp.server_version;
    }

    fn handle_ok(&mut self, op: &OkPacket) {
        self.affected_rows = op.affected_rows;
        self.last_insert_id = op.last_insert_id;
        self.status_flags = op.status_flags;
    }

    fn handle_eof(&mut self, eof: &EOFPacket) {
        self.status_flags = eof.status_flags;
    }

    #[cfg(not(feature = "ssl"))]
    fn do_handshake(&mut self) -> MyResult<()> {
        self.read_packet().and_then(|pld| {
            match pld[0] {
                0xFF => {
                    let error_packet = try!(ErrPacket::from_payload(pld.as_ref(),
                                                                    self.capability_flags));
                    Err(MySqlError(error_packet.into()))
                },
                _ => {
                    let handshake = try!(HandshakePacket::from_payload(pld.as_ref()));
                    if handshake.protocol_version != 10u8 {
                        return Err(DriverError(UnsupportedProtocol(handshake.protocol_version)));
                    }
                    if !handshake.capability_flags.contains(consts::CLIENT_PROTOCOL_41) {
                        return Err(DriverError(Protocol41NotSet));
                    }
                    self.handle_handshake(&handshake);
                    self.do_handshake_response(&handshake)
                },
            }
        }).and_then(|_| {
            self.read_packet()
        }).and_then(|pld| {
            match pld[0] {
                0u8 => {
                    let ok = try!(OkPacket::from_payload(pld.as_ref()));
                    self.handle_ok(&ok);
                    Ok(())
                },
                0xffu8 => {
                    let err = try!(ErrPacket::from_payload(pld.as_ref(),
                                                           self.capability_flags));
                    Err(MySqlError(err.into()))
                },
                _ => Err(DriverError(UnexpectedPacket))
            }
        })
    }

    #[cfg(feature = "ssl")]
    fn do_handshake(&mut self) -> MyResult<()> {
        self.read_packet().and_then(|pld| {
            match pld[0] {
                0xFF => {
                    let error_packet = try!(ErrPacket::from_payload(pld.as_ref(),
                                                                    self.capability_flags));
                    Err(MySqlError(error_packet.into()))
                },
                _ => {
                    let handshake = try!(HandshakePacket::from_payload(pld.as_ref()));
                    if handshake.protocol_version != 10u8 {
                        return Err(DriverError(UnsupportedProtocol(handshake.protocol_version)));
                    }
                    if !handshake.capability_flags.contains(consts::CLIENT_PROTOCOL_41) {
                        return Err(DriverError(Protocol41NotSet));
                    }
                    self.handle_handshake(&handshake);
                    if self.opts.get_ssl_opts().is_some() && self.stream.is_some() {
                        if self.stream.as_ref().unwrap().is_insecure() {
                            if !handshake.capability_flags.contains(consts::CLIENT_SSL) {
                                return Err(DriverError(SslNotSupported));
                            } else {
                                try!(self.do_ssl_request());
                                try!(self.switch_to_ssl());
                            }
                        }
                    }
                    self.do_handshake_response(&handshake)
                },
            }
        }).and_then(|_| {
            self.read_packet()
        }).and_then(|pld| {
            match pld[0] {
                0u8 => {
                    let ok = try!(OkPacket::from_payload(pld.as_ref()));
                    self.handle_ok(&ok);
                    Ok(())
                },
                0xffu8 => {
                    let err = try!(ErrPacket::from_payload(pld.as_ref(),
                                                           self.capability_flags));
                    Err(MySqlError(err.into()))
                },
                _ => Err(DriverError(UnexpectedPacket))
            }
        })
    }

    #[cfg(feature = "ssl")]
    fn get_client_flags(&self) -> consts::CapabilityFlags {
        let mut client_flags = consts::CLIENT_PROTOCOL_41 |
                               consts::CLIENT_SECURE_CONNECTION |
                               consts::CLIENT_LONG_PASSWORD |
                               consts::CLIENT_TRANSACTIONS |
                               consts::CLIENT_LOCAL_FILES |
                               consts::CLIENT_MULTI_STATEMENTS |
                               consts::CLIENT_MULTI_RESULTS |
                               consts::CLIENT_PS_MULTI_RESULTS |
                               (self.capability_flags & consts::CLIENT_LONG_FLAG);
        if let &Some(ref db_name) = self.opts.get_db_name() {
            if db_name.len() > 0 {
                client_flags.insert(consts::CLIENT_CONNECT_WITH_DB);
            }
        }
        if self.stream.is_some() && self.stream.as_ref().unwrap().is_insecure() {
            if self.opts.get_ssl_opts().is_some() {
                client_flags.insert(consts::CLIENT_SSL);
            }
        }
        client_flags
    }

    #[cfg(not(feature = "ssl"))]
    fn get_client_flags(&self) -> consts::CapabilityFlags {
        let mut client_flags = consts::CLIENT_PROTOCOL_41 |
                               consts::CLIENT_SECURE_CONNECTION |
                               consts::CLIENT_LONG_PASSWORD |
                               consts::CLIENT_TRANSACTIONS |
                               consts::CLIENT_LOCAL_FILES |
                               consts::CLIENT_MULTI_STATEMENTS |
                               consts::CLIENT_MULTI_RESULTS |
                               consts::CLIENT_PS_MULTI_RESULTS |
                               (self.capability_flags & consts::CLIENT_LONG_FLAG);
        if let &Some(ref db_name) = self.opts.get_db_name() {
            if db_name.len() > 0 {
                client_flags.insert(consts::CLIENT_CONNECT_WITH_DB);
            }
        }
        client_flags
    }

    #[cfg(feature = "ssl")]
    fn do_ssl_request(&mut self) -> MyResult<()> {
        let client_flags = self.get_client_flags();
        let mut buf = [0; 4 + 4 + 1 + 23];
        {
            let mut writer = &mut buf[..];
            try!(writer.write_u32::<LE>(client_flags.bits()));
            try!(writer.write_all(&[0u8; 4]));
            try!(writer.write_u8(consts::UTF8_GENERAL_CI));
            try!(writer.write_all(&[0u8; 23]));
        }
        self.write_packet(&buf[..])
    }

    fn do_handshake_response(&mut self, hp: &HandshakePacket) -> MyResult<()> {
        let client_flags = self.get_client_flags();
        let scramble_buf = if let &Some(ref pass) = self.opts.get_pass() {
            scramble(&*hp.auth_plugin_data, pass.as_bytes())
        } else {
            None
        };
        let user_len = self.opts.get_user().as_ref().map(|x| x.as_bytes().len()).unwrap_or(0);
        let db_name_len = self.opts.get_db_name().as_ref().map(|x| x.as_bytes().len()).unwrap_or(0);
        let scramble_buf_len = if scramble_buf.is_some() { 20 } else { 0 };
        let mut payload_len = 4 + 4 + 1 + 23 + user_len + 1 + 1 + scramble_buf_len;
        if db_name_len > 0 {
            payload_len += db_name_len + 1;
        }
        let mut buf = vec![0u8; payload_len];
        {
            let mut writer = &mut *buf;
            try!(writer.write_u32::<LE>(client_flags.bits()));
            try!(writer.write_all(&[0u8; 4]));
            try!(writer.write_u8(consts::UTF8_GENERAL_CI));
            try!(writer.write_all(&[0u8; 23]));
            if let &Some(ref user) = self.opts.get_user() {
                try!(writer.write_all(user.as_bytes()));
            }
            try!(writer.write_u8(0u8));
            try!(writer.write_u8(scramble_buf_len as u8));
            if let Some(scr) = scramble_buf {
                try!(writer.write_all(scr.as_ref()));
            }
            if db_name_len > 0 {
                let db_name = self.opts.get_db_name().as_ref().unwrap();
                try!(writer.write_all(db_name.as_bytes()));
                try!(writer.write_u8(0u8));
            }
        }
        self.write_packet(&*buf)
    }

    fn write_command(&mut self, cmd: consts::Command) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd as u8;
        self.write_packet(&[cmd as u8])
    }

    fn write_command_data(&mut self, cmd: consts::Command, data: &[u8]) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd as u8;
        let mut buf = vec![0u8; data.len() + 1];
        {
            let mut writer = &mut *buf;
            let _ = writer.write_u8(cmd as u8);
            let _ = writer.write_all(data);
        }
        self.write_packet(&*buf)
    }

    fn send_long_data(&mut self, stmt: &InnerStmt, params: &[Value], ids: Vec<u16>) -> MyResult<()> {
        for &id in ids.iter() {
            match params[id as usize] {
                Bytes(ref x) => {
                    for chunk in x.chunks(self.max_allowed_packet - 7) {
                        let chunk_len = chunk.len() + 7;
                        let mut buf = vec![0u8; chunk_len];
                        {
                            let mut writer = &mut *buf;
                            try!(writer.write_u32::<LE>(stmt.statement_id));
                            try!(writer.write_u16::<LE>(id));
                            try!(writer.write_all(chunk));
                        }
                        try!(self.write_command_data(Command::COM_STMT_SEND_LONG_DATA, &*buf));
                    }
                },
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    fn _execute(&mut self, stmt: &InnerStmt, params: Params) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        let mut buf = [0u8; 4 + 1 + 4];
        let mut data: Vec<u8>;
        let out;
        match params {
            Params::Empty => {
                if stmt.num_params != 0 {
                    return Err(DriverError(MismatchedStmtParams(stmt.num_params, 0)));
                }
                {
                    let mut writer = &mut buf[..];
                    try!(writer.write_u32::<LE>(stmt.statement_id));
                    try!(writer.write_u8(0u8));
                    try!(writer.write_u32::<LE>(1u32));
                }
                out = &buf[..];
            },
            Params::Positional(params) => {
                if stmt.num_params != params.len() as u16 {
                    return Err(DriverError(MismatchedStmtParams(stmt.num_params, params.len())));
                }
                if let Some(ref sparams) = stmt.params {
                    let (bitmap, values, large_ids) =
                        try!(Value::to_bin_payload(sparams.as_ref(),
                                                   &params,
                                                   self.max_allowed_packet));
                    match large_ids {
                        Some(ids) => try!(self.send_long_data(stmt, &params, ids)),
                        _ => ()
                    }
                    data = vec![0u8; 9 + bitmap.len() + 1 + params.len() * 2 + values.len()];
                    {
                        let mut writer = &mut *data;
                        try!(writer.write_u32::<LE>(stmt.statement_id));
                        try!(writer.write_u8(0u8));
                        try!(writer.write_u32::<LE>(1u32));
                        try!(writer.write_all(bitmap.as_ref()));
                        try!(writer.write_u8(1u8));
                        for i in 0..params.len() {
                            match params[i] {
                                NULL => try!(writer.write_all(
                                    &[sparams[i].column_type as u8, 0u8])),
                                Bytes(..) => try!(
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0u8])),
                                Int(..) => try!(
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 0u8])),
                                UInt(..) => try!(
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 128u8])),
                                Float(..) => try!(
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_DOUBLE as u8, 0u8])),
                                Date(..) => try!(
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_DATETIME as u8, 0u8])),
                                Time(..) => try!(
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_TIME as u8, 0u8]))
                            }
                        }
                        try!(writer.write_all(values.as_ref()));
                    }
                    out = &*data;
                } else {
                    unreachable!();
                }
            },
            Params::Named(_) => {
                if let None = stmt.named_params {
                    return Err(DriverError(NamedParamsForPositionalQuery));
                }
                let named_params = stmt.named_params.as_ref().unwrap();
                return self._execute(stmt, try!(params.into_positional(named_params)))
            }
        }
        try!(self.write_command_data(Command::COM_STMT_EXECUTE, out));
        self.handle_result_set()
    }

    fn execute<'a, T: Into<Params>>(&'a mut self, stmt: &InnerStmt, params: T) -> MyResult<QueryResult<'a>> {
        match self._execute(stmt, params.into()) {
            Ok((columns, ok_packet)) => {
                Ok(QueryResult::new(ResultConnRef::ViaConnRef(self), columns, ok_packet, true))
            },
            Err(err) => Err(err)
        }
    }

    fn _start_transaction(&mut self,
                          consistent_snapshot: bool,
                          isolation_level: Option<IsolationLevel>,
                          readonly: Option<bool>) -> MyResult<()> {
        if let Some(i_level) = isolation_level {
            let _ = try!(self.query(format!("SET TRANSACTION ISOLATION LEVEL {}", i_level)));
        }
        if let Some(readonly) = readonly {
            if self.server_version < (5, 6, 5) {
                return Err(DriverError(ReadOnlyTransNotSupported));
            }
            let _ = if readonly {
                try!(self.query("SET TRANSACTION READ ONLY"))
            } else {
                try!(self.query("SET TRANSACTION READ WRITE"))
            };
        }
        let _ = if consistent_snapshot {
            try!(self.query("START TRANSACTION WITH CONSISTENT SNAPSHOT"))
        } else {
            try!(self.query("START TRANSACTION"))
        };
        Ok(())
    }

    fn send_local_infile(&mut self, file_name: &[u8]) -> MyResult<Option<OkPacket>> {
        let path = String::from_utf8_lossy(file_name);
        let path = path.into_owned();
        let path: path::PathBuf = path.into();
        let mut file = try!(fs::File::open(&path));
        let mut chunk = vec![0u8; self.max_allowed_packet];
        let mut r = file.read(&mut chunk[..]);
        loop {
            match r {
                Ok(n) => {
                    if n > 0 {
                        try!(self.write_packet(&chunk[..n]));
                    } else {
                        break;
                    }
                },
                Err(e) => {
                    return Err(IoError(e));
                }
            }
            r = file.read(&mut chunk[..]);
        }
        try!(self.write_packet(&[]));
        let pld = try!(self.read_packet());
        if pld[0] == 0u8 {
            let ok = try!(OkPacket::from_payload(pld.as_ref()));
            self.handle_ok(&ok);
            return Ok(Some(ok));
        }
        Ok(None)
    }

    fn handle_result_set(&mut self) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        let pld = try!(self.read_packet());
        match pld[0] {
            0x00 => {
                let ok = try!(OkPacket::from_payload(pld.as_ref()));
                self.handle_ok(&ok);
                Ok((Vec::new(), Some(ok)))
            },
            0xfb => {
                let mut reader = &pld[1..];
                let mut file_name = Vec::with_capacity(reader.len());
                try!(reader.read_to_end(&mut file_name));
                match self.send_local_infile(file_name.as_ref()) {
                    Ok(x) => Ok((Vec::new(), x)),
                    Err(err) => Err(err)
                }
            },
            0xff => {
                let err = try!(ErrPacket::from_payload(pld.as_ref(), self.capability_flags));
                Err(MySqlError(err.into()))
            },
            _ => {
                let mut reader = &pld[..];
                let column_count = try!(reader.read_lenenc_int());
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as usize);
                for _ in 0..column_count {
                    let pld = try!(self.read_packet());
                    columns.push(try!(Column::from_payload(pld)));
                }
                // skip eof packet
                try!(self.read_packet());
                self.has_results = true;
                Ok((columns, None))
            }
        }
    }

    fn _query(&mut self, query: &str) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        try!(self.write_command_data(Command::COM_QUERY, query.as_bytes()));
        self.handle_result_set()
    }

    /// Executes [`COM_PING`](http://dev.mysql.com/doc/internals/en/com-ping.html)
    /// on `Conn`. Return `true` on success or `false` on error.
    pub fn ping(&mut self) -> bool {
        match self.write_command(Command::COM_PING) {
            Ok(_) => {
                self.read_packet().is_ok()
            },
            _ => false
        }
    }

    /// Starts new transaction with provided options.
    /// `readonly` is only available since MySQL 5.6.5.
    pub fn start_transaction<'a>(&'a mut self,
                                 consistent_snapshot: bool,
                                 isolation_level: Option<IsolationLevel>,
                                 readonly: Option<bool>) -> MyResult<Transaction<'a>> {
        let _ = try!(self._start_transaction(consistent_snapshot, isolation_level, readonly));
        Ok(Transaction::new(self))
    }

    /// Implements text protocol of mysql server.
    ///
    /// Executes mysql query on `Conn`. [`QueryResult`](struct.QueryResult.html)
    /// will borrow `Conn` until the end of its scope.
    pub fn query<'a, T: AsRef<str> + 'a>(&'a mut self, query: T) -> MyResult<QueryResult<'a>> {
        match self._query(query.as_ref()) {
            Ok((columns, ok_packet)) => {
                Ok(QueryResult::new(ResultConnRef::ViaConnRef(self), columns, ok_packet, false))
            },
            Err(err) => Err(err),
        }
    }

    /// Performs query and returns first row.
    pub fn first<T: AsRef<str>>(&mut self, query: T) -> MyResult<Option<Row>> {
        self.query(query).and_then(|result| {
            for row in result {
                return row.map(Some);
            }
            return Ok(None)
        })
    }

    fn _true_prepare(&mut self,
                     query: &str,
                     named_params: Option<Vec<String>>) -> MyResult<InnerStmt> {
        try!(self.write_command_data(Command::COM_STMT_PREPARE, query.as_bytes()));
        let pld = try!(self.read_packet());
        match pld[0] {
            0xff => {
                let err = try!(ErrPacket::from_payload(pld.as_ref(), self.capability_flags));
                Err(MySqlError(err.into()))
            },
            _ => {
                let mut stmt = try!(InnerStmt::from_payload(pld.as_ref(), named_params));
                if stmt.num_params > 0 {
                    let mut params: Vec<Column> = Vec::with_capacity(stmt.num_params as usize);
                    for _ in 0..stmt.num_params {
                        let pld = try!(self.read_packet());
                        params.push(try!(Column::from_payload(pld)));
                    }
                    stmt.params = Some(params);
                    try!(self.read_packet());
                }
                if stmt.num_columns > 0 {
                    let mut columns: Vec<Column> = Vec::with_capacity(stmt.num_columns as usize);
                    for _ in 0..stmt.num_columns {
                        let pld = try!(self.read_packet());
                        columns.push(try!(Column::from_payload(pld)));
                    }
                    stmt.columns = Some(columns);
                    try!(self.read_packet());
                }
                Ok(stmt)
            }
        }
    }

    fn _prepare(&mut self, query: &str, named_params: Option<Vec<String>>) -> MyResult<InnerStmt> {
        if let Some(inner_st) = self.stmts.get_mut(query) {
            inner_st.named_params = named_params;
            return Ok(inner_st.clone());
        }

        let inner_st = try!(self._true_prepare(query, named_params));
        self.stmts.insert(query.to_owned(), inner_st.clone());
        Ok(inner_st)
    }

    /// Implements binary protocol of mysql server.
    ///
    /// Prepares mysql statement on `Conn`. [`Stmt`](struct.Stmt.html) will
    /// borrow `Conn` until the end of its scope.
    ///
    /// This call will take statement from cache if has been prepared on this connection.
    ///
    /// ### Named parameters support
    ///
    /// `prepare` supports named parameters in form of `:named_param_name`. Allowed characters for
    /// parameter name is `[a-z_]`. Named parameters will be converted to positional before actual
    /// call to prepare so `SELECT :a-:b, :a*:b` is actually `SELECT ?-?, ?*?`.
    ///
    /// ```
    /// # #[macro_use] extern crate mysql; fn main() {
    /// # use mysql::conn::pool;
    /// # use mysql::conn::{Opts, OptsBuilder};
    /// # use mysql::value::{from_value, from_row, ToValue, Value};
    /// # use std::thread::Thread;
    /// # use std::default::Default;
    /// # use std::iter::repeat;
    /// # use mysql::Error::DriverError;
    /// # use mysql::DriverError::MixedParams;
    /// # use mysql::DriverError::MissingNamedParameter;
    /// # use mysql::DriverError::NamedParamsForPositionalQuery;
    /// # fn get_opts() -> Opts {
    /// #     let USER = "root";
    /// #     let ADDR = "127.0.0.1";
    /// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
    /// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
    /// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
    /// #                                .unwrap_or(3307);
    /// #     let mut builder = OptsBuilder::default();
    /// #     builder.user(Some(USER.to_string()))
    /// #            .pass(Some(pwd))
    /// #            .ip_or_hostname(Some(ADDR.to_string()))
    /// #            .tcp_port(port)
    /// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'".to_owned()]);
    /// #     builder.into()
    /// # }
    /// # let opts = get_opts();
    /// # let pool = pool::Pool::new(opts).unwrap();
    /// // Names could be repeated
    /// pool.prep_exec("SELECT :a+:b, :a * :b, ':c'", params!{"a" => 2, "b" => 3}).map(|mut result| {
    ///     let row = result.next().unwrap().unwrap();
    ///     assert_eq!((5, 6, String::from(":c")), from_row(row));
    /// }).unwrap();
    ///
    /// // You can call named statement with positional parameters
    /// pool.prep_exec("SELECT :a+:b, :a*:b", (2, 3, 2, 3)).map(|mut result| {
    ///     let row = result.next().unwrap().unwrap();
    ///     assert_eq!((5, 6), from_row(row));
    /// }).unwrap();
    ///
    /// // You must pass all named parameters for statement
    /// let err = pool.prep_exec("SELECT :name", params!{"another_name" => 42}).unwrap_err();
    /// match err {
    ///     DriverError(e) => assert_eq!(MissingNamedParameter(String::from("name")), e),
    ///     _ => unreachable!(),
    /// }
    ///
    /// // You can't call positional statement with named parameters
    /// let err = pool.prep_exec("SELECT ?", params!{"first" => 42}).unwrap_err();
    /// match err {
    ///     DriverError(e) => assert_eq!(NamedParamsForPositionalQuery, e),
    ///     _ => unreachable!(),
    /// }
    ///
    /// // You can't mix named and positional parameters
    /// let err = pool.prepare("SELECT :a, ?").unwrap_err();
    /// match err {
    ///     DriverError(e) => assert_eq!(MixedParams, e),
    ///     _ => unreachable!(),
    /// }
    /// # }
    /// ```
    pub fn prepare<'a, T: AsRef<str> + 'a>(&'a mut self, query: T) -> MyResult<Stmt<'a>> {
        let query = query.as_ref();
        let (named_params, real_query) = try!(parse_named_params(query));
        match self._prepare(real_query.borrow(), named_params) {
            Ok(stmt) => Ok(Stmt::new(stmt, self)),
            Err(err) => Err(err),
        }
    }

    /// Prepares and executes statement in one call. See
    /// ['Conn::prepare'](struct.Conn.html#method.prepare)
    ///
    /// This call will take statement from cache if has been prepared on this connection.
    pub fn prep_exec<'a, A, T>(&'a mut self, query: A, params: T) -> MyResult<QueryResult<'a>>
    where A: AsRef<str> + 'a,
          T: Into<Params> {
        try!(self.prepare(query)).prep_exec(params.into())
    }

    /// Executs statement and returns first row.
    pub fn first_exec<Q, P>(&mut self, query: Q, params: P) -> MyResult<Option<Row>>
    where Q: AsRef<str>,
          P: Into<Params>,
    {
        self.prep_exec(query, params).and_then(|result| {
            for row in result {
                return row.map(Some);
            }
            return Ok(None)
        })
    }

    fn more_results_exists(&self) -> bool {
        self.has_results
    }

    fn connect(&mut self) -> MyResult<()> {
        if self.connected {
            return Ok(());
        }
        self.do_handshake().and_then(|_| {
            Ok(from_value_opt::<usize>(self.get_system_var("max_allowed_packet").unwrap_or(NULL))
               .unwrap_or(0))
        }).and_then(|max_allowed_packet| {
            if max_allowed_packet == 0 {
                Err(DriverError(SetupError))
            } else {
                self.max_allowed_packet = max_allowed_packet;
                self.connected = true;
                Ok(())
            }
        })
    }

    fn get_system_var(&mut self, name: &str) -> Option<Value> {
        for row in self.query(format!("SELECT @@{};", name)).unwrap() {
            match row {
                Ok(mut r) => match r.len() {
                    0 => (),
                    _ => return r.take(0),
                },
                _ => (),
            }
        }
        return None;
    }

    fn next_bin(&mut self, columns: &Vec<Column>) -> MyResult<Option<Vec<Value>>> {
        if ! self.has_results {
            return Ok(None);
        }
        let pld = match self.read_packet() {
            Ok(pld) => pld,
            Err(e) => {
                self.has_results = false;
                return Err(e);
            }
        };
        let x = pld[0];
        if x == 0xfe && pld.len() < 0xfe {
            self.has_results = false;
            let p = try!(EOFPacket::from_payload(pld.as_ref()));
            self.handle_eof(&p);
            return Ok(None);
        }
        let res = Value::from_bin_payload(pld.as_ref(), columns.as_ref());
        match res {
            Ok(p) => Ok(Some(p)),
            Err(e) => {
                self.has_results = false;
                Err(IoError(e))
            }
        }
    }

    fn next_text(&mut self, col_count: usize) -> MyResult<Option<Vec<Value>>> {
        if ! self.has_results {
            return Ok(None);
        }
        let pld = match self.read_packet() {
            Ok(pld) => pld,
            Err(e) => {
                self.has_results = false;
                return Err(e);
            }
        };
        let x = pld[0];
        if (x == 0xfe || x == 0xff) && pld.len() < 0xfe {
            self.has_results = false;
            if x == 0xfe {
                let p = try!(EOFPacket::from_payload(pld.as_ref()));
                self.handle_eof(&p);
                return Ok(None);
            } else /* x == 0xff */ {
                let p = ErrPacket::from_payload(pld.as_ref(), self.capability_flags);
                match p {
                    Ok(p) => return Err(MySqlError(p.into())),
                    Err(err) => return Err(IoError(err))
                }
            }
        }
        let res = Value::from_payload(pld.as_ref(), col_count);
        match res {
            Ok(p) => Ok(Some(p)),
            Err(err) => {
                self.has_results = false;
                Err(IoError(err))
            }
        }
    }

    fn has_stmt(&self, query: &str) -> bool {
        self.stmts.contains_key(query)
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        let keys: Vec<String> = self.stmts.keys().map(Clone::clone).collect();
        for key in keys {
            for stmt in self.stmts.remove(&key) {
                let data: [u8; 4] = [(stmt.statement_id & 0x000000FF) as u8,
                                     ((stmt.statement_id & 0x0000FF00) >> 08) as u8,
                                     ((stmt.statement_id & 0x00FF0000) >> 16) as u8,
                                     ((stmt.statement_id & 0xFF000000) >> 24) as u8,];
                let _ = self.write_command_data(Command::COM_STMT_CLOSE, &data);
            }
        }
    }
}

/***
 *    888b     d888          8888888b.                             888 888
 *    8888b   d8888          888   Y88b                            888 888
 *    88888b.d88888          888    888                            888 888
 *    888Y88888P888 888  888 888   d88P  .d88b.  .d8888b  888  888 888 888888
 *    888 Y888P 888 888  888 8888888P"  d8P  Y8b 88K      888  888 888 888
 *    888  Y8P  888 888  888 888 T88b   88888888 "Y8888b. 888  888 888 888
 *    888   "   888 Y88b 888 888  T88b  Y8b.          X88 Y88b 888 888 Y88b.
 *    888       888  "Y88888 888   T88b  "Y8888   88888P'  "Y88888 888  "Y888
 *                       888
 *                  Y8b d88P
 *                   "Y88P"
 */

/// Possible ways to pass conn to a query result
#[derive(Debug)]
enum ResultConnRef<'a> {
    ViaConnRef(&'a mut Conn),
    ViaStmt(Stmt<'a>)
}

impl<'a> Deref for ResultConnRef<'a> {
    type Target = Conn;

    fn deref<'c>(&'c self) -> &'c Conn {
        match *self {
            ResultConnRef::ViaConnRef(ref conn_ref) => conn_ref,
            ResultConnRef::ViaStmt(ref stmt) => stmt.conn.deref(),
        }
    }
}

impl<'a> DerefMut for ResultConnRef<'a> {
    fn deref_mut<'c>(&'c mut self) -> &'c mut Conn {
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
/// use mysql::value::from_row;
/// # use mysql::conn::pool;
/// # use mysql::conn::{Opts, OptsBuilder};
/// # use mysql::value::Value;
/// # use std::thread::Thread;
/// # use std::default::Default;
/// # fn get_opts() -> Opts {
/// #     let USER = "root";
/// #     let ADDR = "127.0.0.1";
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     let mut builder = OptsBuilder::default();
/// #     builder.user(Some(USER))
/// #            .pass(Some(pwd))
/// #            .ip_or_hostname(Some(ADDR))
/// #            .tcp_port(port)
/// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
/// #     builder.into()
/// # }
/// # let opts = get_opts();
/// # let pool = pool::Pool::new(opts).unwrap();
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
    ok_packet: Option<OkPacket>,
    is_bin: bool,
}

impl<'a> QueryResult<'a> {
    fn new(conn: ResultConnRef<'a>,
           columns: Vec<Column>,
           ok_packet: Option<OkPacket>,
           is_bin: bool) -> QueryResult<'a>
    {
        QueryResult {
            conn: conn,
            columns: Arc::new(columns),
            ok_packet: ok_packet,
            is_bin: is_bin
        }
    }

    fn handle_if_more_results(&mut self) -> Option<MyResult<Row>> {
        if self.conn.status_flags.contains(consts::SERVER_MORE_RESULTS_EXISTS) {
            match self.conn.handle_result_set() {
                Ok((cols, ok_p)) => {
                    self.columns = Arc::new(cols);
                    self.ok_packet = ok_p;
                    None
                },
                Err(e) => return Some(Err(e)),
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
        self.ok_packet.as_ref().map(|ok_p| ok_p.warnings).unwrap_or(0u16)
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// info.
    pub fn info(&self) -> Vec<u8> {
        if self.ok_packet.is_some() {
            self.ok_packet.as_ref().unwrap().info.clone()
        } else {
            Vec::with_capacity(0)
        }
    }

    /// Returns index of a `QueryResult`'s column by name.
    pub fn column_index<T: AsRef<str>>(&self, name: T) -> Option<usize> {
        let name = name.as_ref().as_bytes();
        for (i, c) in self.columns.iter().enumerate() {
            if c.name() == name {
                return Some(i)
            }
        }
        None
    }

    /// Returns HashMap which maps column names to column indexes.
    pub fn column_indexes<'b, 'c>(&'b self) -> HashMap<String, usize, BldHshrDflt<FnvHasher>> {
        let mut indexes = HashMap::default();
        for (i, column) in self.columns.iter().enumerate() {
            indexes.insert(from_utf8(column.name()).unwrap().to_string(), i);
        }
        indexes
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `QueryResult`'s columns if any.
    pub fn columns_ref(&self) -> &[Column] {
        self.columns.as_ref()
    }

    /// This predicate will help you if you are expecting multiple result sets.
    ///
    /// For example:
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
        self.conn.has_results
    }
}

impl<'a> Iterator for QueryResult<'a> {
    type Item = MyResult<Row>;

    fn next(&mut self) -> Option<MyResult<Row>> {
        let values = if self.is_bin {
            self.conn.next_bin(&self.columns)
        } else {
            self.conn.next_text(self.columns.len())
        };
        match values {
            Ok(values) => {
                match values {
                    Some(values) => Some(Ok(Row::new(values, self.columns.clone()))),
                    None => self.handle_if_more_results(),
                }
            },
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> Drop for QueryResult<'a> {
    fn drop(&mut self) {
        while self.conn.more_results_exists() {
            while let Some(_) = self.next() {}
        }
    }
}

/***
 *    88888888888                   888
 *        888                       888
 *        888                       888
 *        888      .d88b.  .d8888b  888888 .d8888b
 *        888     d8P  Y8b 88K      888    88K
 *        888     88888888 "Y8888b. 888    "Y8888b.
 *        888     Y8b.          X88 Y88b.       X88
 *        888      "Y8888   88888P'  "Y888  88888P'
 *
 *
 *
 */

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    use Opts;
    use OptsBuilder;

    static USER: &'static str = "root";
    static PASS: &'static str = "password";
    static ADDR: &'static str = "127.0.0.1";
    static PORT: u16          = 3307;

    #[cfg(feature = "openssl")]
    pub fn get_opts() -> Opts {
        let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
        let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
                                   .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
                                   .unwrap_or(PORT);
        let mut builder = OptsBuilder::default();
        builder.user(Some(USER))
               .pass(Some(pwd))
               .ip_or_hostname(Some(ADDR))
               .tcp_port(port)
               .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"])
               .ssl_opts(Some(("tests/ca-cert.pem", None::<(String, String)>)));
        builder.into()
    }

    #[cfg(not(feature = "ssl"))]
    pub fn get_opts() -> Opts {
        let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or(PASS.to_string());
        let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
                                   .map(|my_port| my_port.parse().ok().unwrap_or(PORT))
                                   .unwrap_or(PORT);
        let mut builder = OptsBuilder::default();
        builder.user(Some(USER))
               .pass(Some(pwd))
               .ip_or_hostname(Some(ADDR))
               .tcp_port(port)
               .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
        builder.into()
    }

    mod my_conn {
        use std::iter;
        use std::borrow::ToOwned;
        use std::fs;
        use std::io::Write;
        use time::{Tm, now};
        use Conn;
        use DriverError::{
            MissingNamedParameter,
            NamedParamsForPositionalQuery,
        };
        use Error::DriverError;
        use OptsBuilder;
        use Params;
        use super::super::super::value::{ToValue, from_value, from_row};
        use super::super::super::value::Value::{NULL, Int, Bytes, Date};
        use super::get_opts;
        use super::super::Column;

        #[test]
        fn should_connect() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mode = conn.query("SELECT @@GLOBAL.sql_mode").unwrap().next().unwrap().unwrap().take(0).unwrap();
            let mode = from_value::<String>(mode);
            assert!(mode.contains("TRADITIONAL"));
            assert!(conn.ping());
        }
        #[test]
        fn should_connect_with_database() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.db_name(Some("mysql"));
            let mut conn = Conn::new(opts).unwrap();
            assert_eq!(conn.query("SELECT DATABASE()").unwrap().next().unwrap().unwrap().unwrap(),
                       vec![Bytes(b"mysql".to_vec())]);
        }
        #[test]
        fn should_connect_by_hostname() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.db_name(Some("mysql"));
            opts.ip_or_hostname(Some("localhost"));
            let mut conn = Conn::new(opts).unwrap();
            assert_eq!(conn.query("SELECT DATABASE()").unwrap().next().unwrap().unwrap().unwrap(),
                       vec![Bytes(b"mysql".to_vec())]);
        }
        #[test]
        fn should_execute_queryes_and_parse_results() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE x.tbl(\
                                    a TEXT,\
                                    b INT,\
                                    c INT UNSIGNED,\
                                    d DATE,\
                                    e FLOAT
                                )").is_ok());
            assert!(conn.query("INSERT INTO x.tbl(a, b, c, d, e) VALUES (\
                                    'hello',\
                                    -123,\
                                    123,\
                                    '2014-05-05',\
                                    123.123\
                                )").is_ok());
            assert!(conn.query("INSERT INTO x.tbl(a, b, c, d, e) VALUES (\
                                    'world',\
                                    -321,\
                                    321,\
                                    '2014-06-06',\
                                    321.321\
                                )").is_ok());
            assert!(conn.query("SELECT * FROM unexisted").is_err());
            assert!(conn.query("SELECT * FROM x.tbl").is_ok());
            // Drop
            assert!(conn.query("UPDATE x.tbl SET a = 'foo'").is_ok());
            assert_eq!(conn.affected_rows, 2);
            assert!(conn.query("SELECT * FROM x.tbl WHERE a = 'bar'").unwrap().next().is_none());
            for (i, row) in conn.query("SELECT * FROM x.tbl")
                                .unwrap().enumerate() {
                let row = row.unwrap();
                if i == 0 {
                    assert_eq!(row[0], Bytes(b"foo".to_vec()));
                    assert_eq!(row[1], Bytes(b"-123".to_vec()));
                    assert_eq!(row[2], Bytes(b"123".to_vec()));
                    assert_eq!(row[3], Bytes(b"2014-05-05".to_vec()));
                    assert_eq!(row[4], Bytes(b"123.123".to_vec()));
                } else if i == 1 {
                    assert_eq!(row[0], Bytes(b"foo".to_vec()));
                    assert_eq!(row[1], Bytes(b"-321".to_vec()));
                    assert_eq!(row[2], Bytes(b"321".to_vec()));
                    assert_eq!(row[3], Bytes(b"2014-06-06".to_vec()));
                    assert_eq!(row[4], Bytes(b"321.321".to_vec()));
                } else {
                    unreachable!();
                }
            }
        }
        #[test]
        fn should_parse_large_text_result() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert_eq!(
                conn.query("SELECT REPEAT('A', 20000000)").unwrap().next().unwrap().unwrap().unwrap(),
                vec![Bytes(iter::repeat(b'A').take(20_000_000).collect())]
            );
        }
        #[test]
        fn should_execute_statements_and_parse_results() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE x.tbl(\
                                    a TEXT,\
                                    b INT,\
                                    c INT UNSIGNED,\
                                    d DATE,\
                                    e DOUBLE\
                                )").is_ok());
            let _ = conn.prepare("INSERT INTO x.tbl(a, b, c, d, e)\
                          VALUES (?, ?, ?, ?, ?)")
            .and_then(|mut stmt| {
                let tm = Tm { tm_year: 114, tm_mon: 4, tm_mday: 5, tm_hour: 0,
                              tm_min: 0, tm_sec: 0, tm_nsec: 0, ..now() };
                let hello = b"hello".to_vec();
                assert!(stmt.execute((&hello, -123, 123, tm.to_timespec(), 123.123f64)).is_ok());
                assert!(stmt.execute(&[
                    &b"world".to_vec() as &ToValue,
                    &NULL as &ToValue,
                    &NULL as &ToValue,
                    &NULL as &ToValue,
                    &321.321f64 as &ToValue
                ][..]).is_ok());
                Ok(())
            }).unwrap();
            let _ = conn.prepare("SELECT * from x.tbl").and_then(|mut stmt| {
                for (i, row) in stmt.execute(()).unwrap().enumerate() {
                    let mut row = row.unwrap();
                    if i == 0 {
                        assert_eq!(row[0], Bytes(b"hello".to_vec()));
                        assert_eq!(row[1], Int(-123i64));
                        assert_eq!(row[2], Int(123i64));
                        assert_eq!(row[3], Date(2014u16, 5u8, 5u8, 0u8, 0u8, 0u8, 0u32));
                        assert_eq!(row.take::<f64, _>(4).unwrap(), 123.123f64);
                    } else if i == 1 {
                        assert_eq!(row[0], Bytes(b"world".to_vec()));
                        assert_eq!(row[1], NULL);
                        assert_eq!(row[2], NULL);
                        assert_eq!(row[3], NULL);
                        assert_eq!(row.take::<f64, _>(4).unwrap(), 321.321f64);
                    } else {
                        unreachable!();
                    }
                }
                Ok(())
            }).unwrap();
            let mut result = conn.prep_exec("SELECT ?, ?, ?", ("hello", 1, 1.1)).unwrap();
            let row = result.next().unwrap();
            let mut row = row.unwrap();
            assert_eq!(row.take::<String, _>(0).unwrap(), "hello".to_string());
            assert_eq!(row.take::<i8, _>(1).unwrap(), 1i8);
            assert_eq!(row.take::<f32, _>(2).unwrap(), 1.1f32);
        }
        #[test]
        fn should_parse_large_binary_result() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT REPEAT('A', 20000000);").unwrap();
            assert_eq!(
                stmt.execute(()).unwrap().next().unwrap().unwrap().unwrap(),
                vec![Bytes(iter::repeat(b'A').take(20_000_000).collect())]
            );
        }
        #[test]
        fn should_start_commit_and_rollback_transactions() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE x.tbl(a INT)").is_ok());
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                assert!(t.commit().is_ok());
                Ok(())
            }).unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from x.tbl").unwrap().next().unwrap().unwrap().unwrap(),
                vec![Bytes(b"2".to_vec())]
            );
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO tbl(a) VALUES(1)").is_err());
                Ok(())
                // implicit rollback
            }).unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from x.tbl").unwrap().next().unwrap().unwrap().unwrap(),
                vec![Bytes(b"2".to_vec())]
            );
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                assert!(t.rollback().is_ok());
                Ok(())
            }).unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from x.tbl").unwrap().next().unwrap().unwrap().unwrap(),
                vec![Bytes(b"2".to_vec())]
            );
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                let _ = t.prepare("INSERT INTO x.tbl(a) VALUES(?)")
                .and_then(|mut stmt| {
                    assert!(stmt.execute((3,)).is_ok());
                    assert!(stmt.execute((4,)).is_ok());
                    Ok(())
                }).unwrap();
                assert!(t.commit().is_ok());
                Ok(())
            }).unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from x.tbl").unwrap().next().unwrap().unwrap().unwrap(),
                vec![Bytes(b"4".to_vec())]
            );
            let _ = conn.start_transaction(false, None, None). and_then(|mut t| {
                t.prep_exec("INSERT INTO x.tbl(a) VALUES(?)", (5,)).unwrap();
                t.prep_exec("INSERT INTO x.tbl(a) VALUES(?)", (6,)).unwrap();
                Ok(())
            }).unwrap();
        }
        #[test]
        fn should_handle_LOCAL_INFILE() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE x.tbl(a TEXT)").is_ok());
            let path = ::std::path::PathBuf::from("local_infile.txt");
            {
                let mut file = fs::File::create(&path).unwrap();
                let _ = file.write(b"AAAAAA\n");
                let _ = file.write(b"BBBBBB\n");
                let _ = file.write(b"CCCCCC\n");
            }
            let query = format!("LOAD DATA LOCAL INFILE '{}' INTO TABLE x.tbl",
                                path.to_str().unwrap().to_owned());
            conn.query(query).unwrap();
            for (i, row) in conn.query("SELECT * FROM x.tbl")
                                .unwrap().enumerate() {
                let row = row.unwrap();
                match i {
                    0 => assert_eq!(row.unwrap(), vec!(Bytes(b"AAAAAA".to_vec()))),
                    1 => assert_eq!(row.unwrap(), vec!(Bytes(b"BBBBBB".to_vec()))),
                    2 => assert_eq!(row.unwrap(), vec!(Bytes(b"CCCCCC".to_vec()))),
                    _ => unreachable!()
                }
            }
            let _ = fs::remove_file(&path);
        }
        #[test]
        fn should_reset_connection() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE `db`.`test` \
                                (`test` VARCHAR(255) NULL);").is_ok());
            assert!(conn.query("SELECT * FROM `db`.`test`;").is_ok());
            assert!(conn.reset().is_ok());
            assert!(conn.query("SELECT * FROM `db`.`test`;").is_err());
        }

        #[test]
        #[cfg(all(not(feature = "ssl"), any(feature = "pipe", feature = "socket")))]
        fn should_connect_via_socket_for_127_0_0_1() {
            let opts = OptsBuilder::from_opts(get_opts());
            let conn = Conn::new(opts).unwrap();
            let debug_format = format!("{:?}", conn);
            assert!(debug_format.contains("UnixStream"));
        }

        #[test]
        #[cfg(all(not(feature = "ssl"), any(feature = "pipe", feature = "socket")))]
        fn should_connect_via_socket_localhost() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.ip_or_hostname(Some("localhost"));
            let conn = Conn::new(opts).unwrap();
            let debug_format = format!("{:?}", conn);
            assert!(debug_format.contains("UnixStream"));
        }

        #[test]
        #[cfg(any(feature = "pipe", feature = "socket"))]
        fn should_handle_multi_resultset() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.db_name(Some("mysql"));
            let mut conn = Conn::new(opts).unwrap();
            assert!(conn.query("DROP PROCEDURE IF EXISTS multi").is_ok());
            assert!(conn.query(r#"CREATE PROCEDURE multi() BEGIN
                                      SELECT 1;
                                      SELECT 1;
                                  END"#).is_ok());
            for (i, row) in conn.query("CALL multi()")
                                .unwrap().enumerate() {
                match i {
                    0 | 1 => assert_eq!(row.unwrap().unwrap(), vec![Bytes(b"1".to_vec())]),
                    _ => unreachable!(),
                }
            }
            let mut result = conn.query("SELECT 1; SELECT 2; SELECT 3;").unwrap();
            let mut i = 0;
            while { i += 1; result.more_results_exists() } {
                for row in result.by_ref() {
                    match i {
                        1 => assert_eq!(
                            row.unwrap().unwrap(),
                            vec![Bytes(b"1".to_vec())]
                        ),
                        2 => assert_eq!(
                            row.unwrap().unwrap(),
                            vec![Bytes(b"2".to_vec())]
                        ),
                        3 => assert_eq!(
                            row.unwrap().unwrap(),
                            vec![Bytes(b"3".to_vec())]
                        ),
                        _ => unreachable!(),
                    }
                }
            }
            assert_eq!(i, 4);
        }

        #[test]
        fn should_work_with_named_params() {
            let mut conn = Conn::new(get_opts()).unwrap();
            {
                let mut stmt = conn.prepare("SELECT :a, :b, :a, :c").unwrap();
                let mut result = stmt.execute(params!{"a" => 1, "b" => 2, "c" => 3}).unwrap();
                let row = result.next().unwrap().unwrap();
                assert_eq!((1, 2, 1, 3), from_row(row));
            }

            let mut result = conn.prep_exec("SELECT :a, :b, :a + :b, :c", params!{
                "a" => 1,
                "b" => 2,
                "c" => 3,
            }).unwrap();
            let row = result.next().unwrap().unwrap();
            assert_eq!((1, 2, 3, 3), from_row(row));
        }

        #[test]
        fn should_return_error_on_missing_named_parameter() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT :a, :b, :a, :c, :d").unwrap();
            let result = stmt.execute(params!{"a" => 1, "b" => 2, "c" => 3,});
            match result {
                Err(DriverError(MissingNamedParameter(ref x))) if x == "d" => (),
                _ => assert!(false),
            }
        }

        #[test]
        fn should_return_error_on_named_params_for_positional_statement() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
            let result = stmt.execute(params!{"a" => 1, "b" => 2, "c" => 3,});
            match result {
                Err(DriverError(NamedParamsForPositionalQuery)) => (),
                _ => assert!(false),
            }
        }

        #[test]
        #[should_panic]
        fn should_panic_on_named_param_redefinition() {
            let _: Params = params!{"a" => 1, "b" => 2, "a" => 3}.into();
        }

        #[test]
        fn should_parse_column_from_payload() {
            let payload1 = b"\x03def\x06schema\x05table\x09org_table\x04name\x08org_name\
                         \x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00".to_vec();
            let payload2 = b"\x03def\x06schema\x05table\x09org_table\x04name\x08org_name\
                         \x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07default".to_vec();
            let out1 = Column::from_payload(payload1).unwrap();
            let out2 = Column::from_payload(payload2).unwrap();
            assert_eq!(out1.schema(), b"schema");
            assert_eq!(out1.table(), b"table");
            assert_eq!(out1.org_table(), b"org_table");
            assert_eq!(out1.name(), b"name");
            assert_eq!(out1.org_name(), b"org_name");
            assert_eq!(out1.default_values(), None);
            assert_eq!(out2.schema(), b"schema");
            assert_eq!(out2.table(), b"table");
            assert_eq!(out2.org_table(), b"org_table");
            assert_eq!(out2.name(), b"name");
            assert_eq!(out2.org_name(), b"org_name");
            assert_eq!(out2.default_values(), Some(&b"default"[..]));
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use super::get_opts;
        use super::super::{Conn};
        use super::super::super::value::Value::NULL;

        #[bench]
        fn simple_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| { let _ = conn.query("DO 1"); })
        }

        #[bench]
        fn prepared_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("DO 1").unwrap();
            bencher.iter(|| { let _ = stmt.execute(()); })
        }

        #[bench]
        fn prepare_and_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| {
                let mut stmt = conn.prepare("SELECT ?").unwrap();
                let _ = stmt.execute((0,)).unwrap();
            })
        }

        #[bench]
        fn simple_query_row(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| { let _ = conn.query("SELECT 1"); })
        }

        #[bench]
        fn simple_prepared_query_row(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT 1").unwrap();
            bencher.iter(|| { let _ = stmt.execute(()); })
        }

        #[bench]
        fn simple_prepared_query_row_with_param(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?").unwrap();
            bencher.iter(|| { let _ = stmt.execute((0,)); })
        }

        #[bench]
        fn simple_prepared_query_row_with_named_param(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT :a").unwrap();
            bencher.iter(|| { let _ = stmt.execute(params!{"a" => 0}); })
        }

        #[bench]
        fn simple_prepared_query_row_with_5_params(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
            let params = (42i8, b"123456".to_vec(), 1.618f64, NULL, 1i8);
            bencher.iter(|| { let _ = stmt.execute(&params); })
        }

        #[bench]
        fn simple_prepared_query_row_with_5_named_params(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT :one, :two, :three, :four, :five").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(params!{
                    "one" => 42i8,
                    "two" => b"123456",
                    "three" => 1.618f64,
                    "four" => NULL,
                    "five" => 1i8,
                });
            })
        }

        #[bench]
        fn select_large_string(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| { let _ = conn.query("SELECT REPEAT('A', 10000)"); })
        }

        #[bench]
        fn select_prepared_large_string(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT REPEAT('A', 10000)").unwrap();
            bencher.iter(|| { let _ = stmt.execute(()); })
        }

        #[bench]
        fn many_small_rows(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query("CREATE TEMPORARY TABLE x.x (id INT)").unwrap();
            for _ in 0..512 {
                conn.query("INSERT INTO x.x VALUES (256)").unwrap();
            }
            let mut stmt = conn.prepare("SELECT * FROM x.x").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(());
            });
        }
    }
}
