use bit_vec::BitVec;
use packet::InnerStmt;
use Row;
use smallvec::SmallVec;
use std::borrow::Borrow;
use std::cmp;
use std::collections::HashMap;
use std::fs;
use std::fmt;
use std::hash::BuildHasherDefault as BldHshrDflt;
use std::io;
use std::io::Read;
use std::io::Write as NewWrite;
use std::mem;
use std::ops::{
    Deref,
    DerefMut,
};
use std::path;
use std::sync::{Arc, Mutex};

use super::consts::{CapabilityFlags, Command, ColumnType, StatusFlags, MAX_PAYLOAD_LEN, UTF8_GENERAL_CI, UTF8MB4_GENERAL_CI};
use super::io::Read as MyRead;
use super::io::Write;
use super::io::Stream;
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
use super::error::DriverError::SslNotSupported;
use myc::named_params::parse_named_params;
use super::scramble::scramble;
use Params;
use Value::{self, NULL, Int, UInt, Float, Bytes, Date, Time};
use from_value;
use from_value_opt;

use byteorder::LittleEndian as LE;
use byteorder::WriteBytesExt;
use fnv::FnvHasher;
use myc::packets::{Column, HandshakePacket, OkPacket, column_from_payload, parse_handshake_packet,
                   parse_err_packet, parse_ok_packet};
use myc::row::new_row;
use myc::row::convert::{from_row, FromRow};
use myc::value::{read_bin_values, read_text_values, serialize_bin_many};

pub mod pool;
mod opts;
mod stmt_cache;
use self::stmt_cache::StmtCache;
pub use self::opts::Opts;
pub use self::opts::OptsBuilder;
#[cfg(feature = "ssl")]
pub use self::opts::SslOpts;

/// A trait allowing abstraction over connections and transactions
pub trait GenericConnection {
    /// See
    /// [`Conn#query`](struct.Conn.html#method.query).
    fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult>;

    /// See
    /// [`Conn#first`](struct.Conn.html#method.first).
    fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>>;

    /// See
    /// [`Conn#prepare`](struct.Conn.html#method.prepare).
    fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt>;

    /// See
    /// [`Conn#prep_exec`](struct.Conn.html#method.prep_exec).
    fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult>
        where A: AsRef<str>, T: Into<Params>;

    /// See
    /// [`Conn#first_exec`](struct.Conn.html#method.first_exec).
    fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
        where Q: AsRef<str>, P: Into<Params>, T: FromRow;
}


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
    restore_local_infile_handler: Option<LocalInfileHandler>,
}

impl<'a> Transaction<'a> {
    fn new(conn: &'a mut Conn) -> Transaction<'a> {
        let handler = conn.local_infile_handler.clone();
        Transaction {
            conn: ConnRef::ViaConnRef(conn),
            committed: false,
            rolled_back: false,
            restore_local_infile_handler: handler,
        }
    }

    fn new_pooled(conn: pool::PooledConn) -> Transaction<'a> {
        let handler = conn.as_ref().local_infile_handler.clone();
        Transaction {
            conn: ConnRef::ViaPooledConn(conn),
            committed: false,
            rolled_back: false,
            restore_local_infile_handler: handler,
        }
    }

    /// See [`Conn::query`](struct.Conn.html#method.query).
    pub fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult> {
        self.conn.query(query)
    }

    /// See [`Conn::first`](struct.Conn.html#method.first).
    pub fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.query(query).and_then(|result| {
            for row in result {
                return row.map(|x| Some(from_row(x)));
            }
            return Ok(None)
        })
    }

    /// See [`Conn::prepare`](struct.Conn.html#method.prepare).
    pub fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt> {
        self.conn.prepare(query)
    }

    /// See [`Conn::prep_exec`](struct.Conn.html#method.prep_exec).
    pub fn prep_exec<A: AsRef<str>, T: Into<Params>>(&mut self, query: A, params: T) -> MyResult<QueryResult> {
        self.conn.prep_exec(query, params)
    }

    /// See [`Conn::first_exec`](struct.Conn.html#method.first_exec).
    pub fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
    where Q: AsRef<str>,
          P: Into<Params>,
          T: FromRow
    {
        self.prep_exec(query, params).and_then(|result| {
            for row in result {
                return row.map(|x| Some(from_row(x)));
            }
            return Ok(None)
        })
    }

    /// Will consume and commit transaction.
    pub fn commit(mut self) -> MyResult<()> {
        self.conn.query("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    /// Will consume and rollback transaction. You also can rely on `Drop` implementation but it
    /// will swallow errors.
    pub fn rollback(mut self) -> MyResult<()> {
        self.conn.query("ROLLBACK")?;
        self.rolled_back = true;
        Ok(())
    }

    /// A way to override local infile handler for this transaction.
    /// Destructor of transaction will restore original handler.
    pub fn set_local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) {
        self.conn.set_local_infile_handler(handler);
    }
}

impl<'a> GenericConnection for Transaction<'a> {
    fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult> {
        self.query(query)
    }

    fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.first(query)
    }

    fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt> {
        self.prepare(query)
    }

    fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult>
        where A: AsRef<str>, T: Into<Params> {
        self.prep_exec(query, params)
    }

    fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
        where Q: AsRef<str>, P: Into<Params>, T: FromRow {
        self.first_exec(query, params)
    }
}

impl<'a> Drop for Transaction<'a> {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if ! self.committed && ! self.rolled_back {
            let _ = self.conn.query("ROLLBACK");
        }
        self.conn.local_infile_handler = self.restore_local_infile_handler.take();
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
    pub fn first_exec<P, T>(&mut self, params: P) -> MyResult<Option<T>>
    where P: Into<Params>,
          T: FromRow,
    {
        self.execute(params).and_then(|result| {
            for row in result {
                return row.map(|x| Some(from_row(x)));
            }
            return Ok(None)
        })
    }

    fn prep_exec<T: Into<Params>>(mut self, params: T) -> MyResult<QueryResult<'a>> {
        let columns = self.conn._execute(&self.stmt, params.into())?;
        Ok(QueryResult::new(ResultConnRef::ViaStmt(self), columns, true))
    }
}

impl<'a> Drop for Stmt<'a> {
    fn drop(&mut self) {
        if self.conn.stmt_cache.get_cap() == 0 {
            let mut stmt_id = [0u8; 4];
            let _ = (&mut stmt_id[..]).write_u32::<LE>(self.stmt.id());
            let _ = self.conn.write_command_data(Command::COM_STMT_CLOSE, &stmt_id[..]);
        }
    }
}

/// Callback to handle requests for local files.
/// Consult [Mysql documentation](https://dev.mysql.com/doc/refman/5.7/en/load-data.html) for the
/// format of local infile data.
///
/// ```rust
/// # use std::io::Write;
/// # use mysql::{Pool, Opts, OptsBuilder, LocalInfileHandler, from_row};
/// # fn get_opts() -> Opts {
/// #     let user = "root";
/// #     let addr = "127.0.0.1";
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     let mut builder = OptsBuilder::default();
/// #     builder.user(Some(user))
/// #            .pass(Some(pwd))
/// #            .ip_or_hostname(Some(addr))
/// #            .tcp_port(port)
/// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
/// #     builder.into()
/// # }
/// # let opts = get_opts();
/// # let pool = Pool::new_manual(1, 1, opts).unwrap();
/// # pool.prep_exec("CREATE TEMPORARY TABLE tmp.Users (id INT, name TEXT, age INT, email TEXT)", ()).unwrap();
/// # pool.prep_exec("INSERT INTO tmp.Users (id, name, age, email) VALUES (?, ?, ?, ?)",
/// #                (1, "John", 17, "foo@bar.baz")).unwrap();
/// # let mut conn = pool.get_conn().unwrap();
/// conn.query("CREATE TEMPORARY TABLE x.tbl(a TEXT)").unwrap();
///
/// conn.set_local_infile_handler(Some(
///     LocalInfileHandler::new(|file_name, writer| {
///         writer.write_all(b"row1: file name is ")?;
///         writer.write_all(file_name)?;
///         writer.write_all(b"\n")?;
///
///         writer.write_all(b"row2: foobar\n")
///     })
/// ));
///
/// conn.query("LOAD DATA LOCAL INFILE 'file_name' INTO TABLE x.tbl").unwrap();
///
/// let mut row_num = 0;
/// for (row_idx, row) in conn.query("SELECT * FROM x.tbl").unwrap().enumerate() {
///     row_num = row_idx + 1;
///     let row: (String,) = from_row(row.unwrap());
///     match row_num {
///         1 => assert_eq!(row.0, "row1: file name is file_name"),
///         2 => assert_eq!(row.0, "row2: foobar"),
///         _ => unreachable!(),
///     }
/// }
///
/// assert_eq!(row_num, 2);
/// ```
#[derive(Clone)]
pub struct LocalInfileHandler(
    Arc<Mutex<
        for<'a> FnMut(&'a [u8], &'a mut LocalInfile) -> io::Result<()> + Send
    >>
);

impl LocalInfileHandler {
    pub fn new<F>(f: F) -> Self
    where F: for<'a> FnMut(&'a [u8], &'a mut LocalInfile) -> io::Result<()> + Send + 'static
    {
        LocalInfileHandler(Arc::new(Mutex::new(f)))
    }
}

impl PartialEq for LocalInfileHandler {
    fn eq(&self, other: &LocalInfileHandler) -> bool {
        (&*self.0 as *const _) == (&*other.0 as *const _)
    }
}

impl Eq for LocalInfileHandler {}

impl fmt::Debug for LocalInfileHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "LocalInfileHandler(...)")
    }
}

/// Local in-file stream.
/// The callback will be passed a reference to this stream, which it
/// should use to write the contents of the requested file.
/// See [LocalInfileHandler](struct.LocalInfileHandler.html) documentation for example.
#[derive(Debug)]
pub struct LocalInfile<'a> {
    buffer: io::Cursor<Box<[u8]>>,
    conn: &'a mut Conn,
}

impl<'a> io::Write for LocalInfile<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.buffer.write(buf);
        if let Ok(_) = result {
            if self.buffer.position() as usize >= self.buffer.get_ref().len() {
                self.flush()?;
            }
        }
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        let n = self.buffer.position() as usize;
        if n > 0 {
            let range = &self.buffer.get_ref()[..n];
            self.conn.write_packet(range).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, Box::new(e))
            })?;
        }
        self.buffer.set_position(0);
        Ok(())
    }
}

/***
 *     .d8888b.
 *    d88P  Y88b
 *    888    888
 *    888         .d88b.  88888b.  88888b.
 *    888        d88""88b 888 "88b 888 "88b
 *    888    888 888  888 888  888 888  888
 *    Y88b  d88P Y88..88P 888  888 888  888
 *     "Y8888P"   "Y88P"  888  888 888  888
 *
 */

/// Mysql connection.
#[derive(Debug)]
pub struct Conn {
    opts: Opts,
    stream: Option<Stream>,
    stmt_cache: StmtCache,
    server_version: Option<(u16, u16, u16)>,
    mariadb_server_version: Option<(u16, u16, u16)>,
    affected_rows: u64,
    last_insert_id: u64,
    warnings: u16,
    info: Option<Vec<u8>>,
    max_allowed_packet: usize,
    capability_flags: CapabilityFlags,
    connection_id: u32,
    status_flags: StatusFlags,
    seq_id: u8,
    character_set: u8,
    last_command: u8,
    connected: bool,
    has_results: bool,
    local_infile_handler: Option<LocalInfileHandler>,
}

impl Conn {
    fn empty<T: Into<Opts>>(opts: T) -> Conn {
        let opts = opts.into();
        Conn {
            stmt_cache: StmtCache::new(opts.get_stmt_cache_size()),
            opts: opts,
            stream: None,
            seq_id: 0u8,
            capability_flags: CapabilityFlags::empty(),
            status_flags: StatusFlags::empty(),
            connection_id: 0u32,
            character_set: 0u8,
            affected_rows: 0u64,
            last_insert_id: 0u64,
            warnings: 0,
            info: None,
            last_command: 0u8,
            max_allowed_packet: MAX_PAYLOAD_LEN,
            connected: false,
            has_results: false,
            server_version: None,
            mariadb_server_version: None,
            local_infile_handler: None
        }
    }

    /// Check the connection can be improved.
    fn can_improved(&mut self) -> Option<Opts> {
        if self.opts.get_prefer_socket() && self.opts.addr_is_loopback() {
            if let Some(socket) = self.get_system_var("socket") {
                if self.opts.get_socket().is_none() {
                    let mut socket_opts = OptsBuilder::from_opts(self.opts.clone());
                    let socket = from_value::<String>(socket);
                    if socket.len() > 0 {
                        socket_opts.socket(Some(socket));
                        return Some(socket_opts.into())
                    }
                }
            }
        }
        None
    }

    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        conn.connect_stream()?;
        conn.connect()?;
        let mut conn = {
            if let Some(new_opts) = conn.can_improved() {
                drop(conn);
                let mut improved_conn = Conn::empty(new_opts);
                improved_conn.connect_stream()?;
                improved_conn.connect()?;
                improved_conn
            } else {
                conn
            }
        };
        for cmd in conn.opts.get_init().clone() {
            conn.query(cmd)?;
        }
        return Ok(conn);
    }

    fn soft_reset(&mut self) -> MyResult<()> {
        self.write_command(Command::COM_RESET_CONNECTION)?;
        self.read_packet().and_then(|pld| {
            match pld[0] {
                0 => {
                    let ok = parse_ok_packet(&*pld, self.capability_flags)?;
                    self.handle_ok(&ok);
                    self.last_command = 0;
                    self.stmt_cache.clear();
                    Ok(())
                },
                _ => {
                    let err = parse_err_packet(&*pld, self.capability_flags)?;
                    Err(MySqlError(err.into()))
                },
            }
        })
    }

    fn hard_reset(&mut self) -> MyResult<()> {
        self.stream = None;
        self.stmt_cache.clear();
        self.seq_id = 0;
        self.capability_flags = CapabilityFlags::empty();
        self.status_flags = StatusFlags::empty();
        self.connection_id = 0;
        self.character_set = 0;
        self.affected_rows = 0;
        self.last_insert_id = 0;
        self.warnings = 0;
        self.info = None;
        self.last_command = 0;
        self.max_allowed_packet = MAX_PAYLOAD_LEN;
        self.connected = false;
        self.has_results = false;
        self.connect_stream()?;
        self.connect()
    }

    /// Resets `MyConn` (drops state then reconnects).
    pub fn reset(&mut self) -> MyResult<()> {
        match (self.server_version, self.mariadb_server_version) {
            (Some(ref version), _) if *version > (5, 7, 3) => {
                self.soft_reset().or_else(|_| self.hard_reset())
            },
            (_, Some(ref version)) if *version >= (10, 2, 7)  => {
                self.soft_reset().or_else(|_| self.hard_reset())
            },
            _ => self.hard_reset()
        }
    }

    fn get_mut_stream<'a>(&'a mut self) -> &'a mut Stream {
        self.stream.as_mut().unwrap()
    }

    #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
    fn switch_to_ssl(&mut self) -> MyResult<()> {
        if self.stream.is_some() {
            let stream = self.stream.take().unwrap();
            let stream = stream.make_secure(self.opts.get_verify_peer(),
                                            self.opts.get_ip_or_hostname(),
                                            self.opts.get_ssl_opts())?;
            self.stream = Some(stream);
        }
        Ok(())
    }

    #[cfg(any(not(feature = "ssl"), target_os = "windows"))]
    fn switch_to_ssl(&mut self) -> MyResult<()> {
        unimplemented!();
    }

    fn connect_stream(&mut self) -> MyResult<()> {
        let read_timeout = self.opts.get_read_timeout().clone();
        let write_timeout = self.opts.get_write_timeout().clone();
        let tcp_keepalive_time = self.opts.get_tcp_keepalive_time_ms().clone();
        let tcp_connect_timeout = self.opts.get_tcp_connect_timeout();
        let bind_address = self.opts.bind_address().cloned();
        let stream = if let Some(ref socket) = *self.opts.get_socket() {
            Stream::connect_socket(&*socket, read_timeout, write_timeout)?
        } else if let Some(ref ip_or_hostname) = *self.opts.get_ip_or_hostname() {
            let port = self.opts.get_tcp_port();
            Stream::connect_tcp(&*ip_or_hostname,
                                port,
                                read_timeout,
                                write_timeout,
                                tcp_keepalive_time,
                                tcp_connect_timeout,
                                bind_address)?
        } else {
            return Err(DriverError(CouldNotConnect(None)));
        };
        self.stream = Some(stream);
        return Ok(())
    }

    fn read_packet(&mut self) -> MyResult<Vec<u8>> {
        let old_seq_id = self.seq_id;
        let (data, seq_id) = self.get_mut_stream().as_mut().read_packet(old_seq_id)?;
        self.seq_id = seq_id;
        Ok(data)
    }

    fn drop_packet(&mut self) -> MyResult<()> {
        let old_seq_id = self.seq_id;
        let seq_id = self.get_mut_stream().as_mut().drop_packet(old_seq_id)?;
        self.seq_id = seq_id;
        Ok(())
    }

    fn write_packet(&mut self, data: &[u8]) -> MyResult<()> {
        let seq_id = self.seq_id;
        let max_allowed_packet = self.max_allowed_packet;
        self.seq_id = self.get_mut_stream().as_mut().write_packet(data, seq_id, max_allowed_packet)?;
        Ok(())
    }

    fn handle_handshake(&mut self, hp: &HandshakePacket) {
        self.capability_flags = hp.capabilities() & self.get_client_flags();
        self.status_flags = hp.status_flags();
        self.connection_id = hp.connection_id();
        self.character_set = hp.default_collation();
        self.server_version = hp.server_version_parsed();
        self.mariadb_server_version = hp.maria_db_server_version_parsed();
    }

    fn handle_ok(&mut self, op: &OkPacket) {
        self.affected_rows = op.affected_rows();
        self.last_insert_id = op.last_insert_id().unwrap_or(0);
        self.status_flags = op.status_flags();
        self.warnings = op.warnings();
        self.info = op.info_ref().map(Into::into);
    }

    fn do_handshake(&mut self) -> MyResult<()> {
        self.read_packet().and_then(|pld| {
            match pld[0] {
                0xFF => {
                    let error_packet = parse_err_packet(pld.as_ref(),
                                                        self.capability_flags)?;
                    Err(MySqlError(error_packet.into()))
                },
                _ => {
                    let handshake = parse_handshake_packet(pld.as_ref())?;
                    if handshake.protocol_version() != 10u8 {
                        return Err(DriverError(UnsupportedProtocol(handshake.protocol_version())));
                    }
                    if !handshake.capabilities().contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
                        return Err(DriverError(Protocol41NotSet));
                    }
                    self.handle_handshake(&handshake);
                    if self.opts.get_ssl_opts().is_some() && self.stream.is_some() {
                        if self.stream.as_ref().unwrap().is_insecure() {
                            if !handshake.capabilities().contains(CapabilityFlags::CLIENT_SSL) {
                                return Err(DriverError(SslNotSupported));
                            } else {
                                self.do_ssl_request()?;
                                self.switch_to_ssl()?;
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
                    let ok = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
                    self.handle_ok(&ok);
                    Ok(())
                },
                0xffu8 => {
                    let err = parse_err_packet(pld.as_ref(),
                                               self.capability_flags)?;
                    Err(MySqlError(err.into()))
                },
                _ => Err(DriverError(UnexpectedPacket))
            }
        })
    }

    fn get_client_flags(&self) -> CapabilityFlags {
        let mut client_flags = CapabilityFlags::CLIENT_PROTOCOL_41 |
            CapabilityFlags::CLIENT_SECURE_CONNECTION |
            CapabilityFlags::CLIENT_LONG_PASSWORD |
            CapabilityFlags::CLIENT_TRANSACTIONS |
            CapabilityFlags::CLIENT_LOCAL_FILES |
            CapabilityFlags::CLIENT_MULTI_STATEMENTS |
            CapabilityFlags::CLIENT_MULTI_RESULTS |
            CapabilityFlags::CLIENT_PS_MULTI_RESULTS |
                               (self.capability_flags & CapabilityFlags::CLIENT_LONG_FLAG);
        if let &Some(ref db_name) = self.opts.get_db_name() {
            if db_name.len() > 0 {
                client_flags.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
            }
        }
        if self.stream.is_some() && self.stream.as_ref().unwrap().is_insecure() {
            if self.opts.get_ssl_opts().is_some() {
                client_flags.insert(CapabilityFlags::CLIENT_SSL);
            }
        }
        client_flags
    }

    fn get_default_collation(&self) -> u8 {
        match self.server_version {
            Some(ref version) if *version > (5, 5, 3) => UTF8MB4_GENERAL_CI as u8,
            _ => UTF8_GENERAL_CI as u8,
        }
    }

    fn do_ssl_request(&mut self) -> MyResult<()> {
        let client_flags = self.get_client_flags();
        let mut buf = [0; 4 + 4 + 1 + 23];
        {
            let mut writer = &mut buf[..];
            writer.write_u32::<LE>(client_flags.bits())?;
            writer.write_all(&[0u8; 4])?;
            writer.write_u8(self.get_default_collation())?;
            writer.write_all(&[0u8; 23])?;
        }
        self.write_packet(&buf[..])
    }

    fn do_handshake_response(&mut self, hp: &HandshakePacket) -> MyResult<()> {
        let client_flags = self.get_client_flags();
        let scramble_buf = if let &Some(ref pass) = self.opts.get_pass() {
            let mut scramble_data = Vec::from(hp.scramble_1_ref());
            scramble_data.extend_from_slice(hp.scramble_2_ref().unwrap_or(&[][..]));
            scramble(&*scramble_data, pass.as_bytes())
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
            writer.write_u32::<LE>(client_flags.bits())?;
            writer.write_all(&[0u8; 4])?;
            writer.write_u8(self.get_default_collation())?;
            writer.write_all(&[0u8; 23])?;
            if let &Some(ref user) = self.opts.get_user() {
                writer.write_all(user.as_bytes())?;
            }
            writer.write_u8(0u8)?;
            writer.write_u8(scramble_buf_len as u8)?;
            if let Some(scr) = scramble_buf {
                writer.write_all(scr.as_ref())?;
            }
            if db_name_len > 0 {
                let db_name = self.opts.get_db_name().as_ref().unwrap();
                writer.write_all(db_name.as_bytes())?;
                writer.write_u8(0u8)?;
            }
        }
        self.write_packet(&*buf)
    }

    fn write_command(&mut self, cmd: Command) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd as u8;
        self.write_packet(&[cmd as u8])
    }

    fn write_command_data(&mut self, cmd: Command, data: &[u8]) -> MyResult<()> {
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

    fn send_long_data(&mut self, stmt: &InnerStmt, params: &[Value], ids: BitVec<u8>) -> MyResult<()> {
        for (i, bit) in ids.into_iter().enumerate() {
            if bit {
                match params[i as usize] {
                    Bytes(ref x) => {
                        for chunk in x.chunks(MAX_PAYLOAD_LEN - 6) {
                            let chunk_len = chunk.len() + 6;
                            let mut buf = vec![0u8; chunk_len];
                            {
                                let mut writer = &mut *buf;
                                writer.write_u32::<LE>(stmt.id())?;
                                writer.write_u16::<LE>(i as u16)?;
                                writer.write_all(chunk)?;
                            }
                            self.write_command_data(Command::COM_STMT_SEND_LONG_DATA, &*buf)?;
                        }
                    },
                    _ => unreachable!(),
                }
            }
        }
        Ok(())
    }

    fn _execute(&mut self, stmt: &InnerStmt, params: Params) -> MyResult<Vec<Column>> {
        let mut buf = [0u8; 4 + 1 + 4];
        let mut data: Vec<u8>;
        let out;
        match params {
            Params::Empty => {
                if stmt.num_params() != 0 {
                    return Err(DriverError(MismatchedStmtParams(stmt.num_params(), 0)));
                }
                {
                    let mut writer = &mut buf[..];
                    writer.write_u32::<LE>(stmt.id())?;
                    writer.write_u8(0u8)?;
                    writer.write_u32::<LE>(1u32)?;
                }
                out = &buf[..];
            },
            Params::Positional(params) => {
                if stmt.num_params() != params.len() as u16 {
                    return Err(DriverError(MismatchedStmtParams(stmt.num_params(), params.len())));
                }
                if let Some(sparams) = stmt.params() {
                    let (serialized, null_bitmap, large_bitmap) =
                        serialize_bin_many(sparams, &params)?;

                    if large_bitmap.any() {
                        self.send_long_data(stmt, &params, large_bitmap)?;
                    }

                    data = vec![0u8; 9 + null_bitmap.storage().len() + 1 + params.len() * 2 + serialized.len()];
                    {
                        let mut writer = &mut *data;
                        writer.write_u32::<LE>(stmt.id())?;
                        writer.write_u8(0)?;
                        writer.write_u32::<LE>(1u32)?;
                        writer.write_all(null_bitmap.storage())?;
                        writer.write_u8(1)?;
                        for (value, column) in params.iter().zip(sparams.iter()) {
                            match *value {
                                NULL => writer.write_all(&[column.column_type() as u8, 0])?,
                                Bytes(..) => {
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0])?
                                }
                                Int(..) => {
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 0])?
                                }
                                UInt(..) => {
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 128])?
                                }
                                Float(..) => {
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_DOUBLE as u8, 0])?
                                }
                                Date(..) => {
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_DATETIME as u8, 0])?
                                }
                                Time(..) => {
                                    writer.write_all(&[ColumnType::MYSQL_TYPE_TIME as u8, 0])?
                                }
                            }
                        }
                        writer.write_all(&*serialized)?;
                    }

                    out = &*data;
                } else {
                    unreachable!();
                }
            },
            Params::Named(_) => {
                if let None = stmt.named_params() {
                    return Err(DriverError(NamedParamsForPositionalQuery));
                }
                let named_params = stmt.named_params().unwrap();
                return self._execute(stmt, params.into_positional(named_params)?)
            }
        }
        self.write_command_data(Command::COM_STMT_EXECUTE, out)?;
        self.handle_result_set()
    }

    fn execute<'a, T: Into<Params>>(&'a mut self, stmt: &InnerStmt, params: T) -> MyResult<QueryResult<'a>> {
        match self._execute(stmt, params.into()) {
            Ok(columns) => {
                Ok(QueryResult::new(ResultConnRef::ViaConnRef(self), columns, true))
            },
            Err(err) => Err(err)
        }
    }

    fn _start_transaction(&mut self,
                          consistent_snapshot: bool,
                          isolation_level: Option<IsolationLevel>,
                          readonly: Option<bool>) -> MyResult<()> {
        if let Some(i_level) = isolation_level {
            let _ = self.query(format!("SET TRANSACTION ISOLATION LEVEL {}", i_level))?;
        }
        if let Some(readonly) = readonly {
            let supported = match (self.server_version, self.mariadb_server_version) {
                (Some(ref version), _) if *version < (5, 6, 5) => {
                    true
                }
                (_, Some(ref version)) if *version < (10, 0, 0) => {
                    true
                }
                _ => false,
            };
            if !supported {
                return Err(DriverError(ReadOnlyTransNotSupported));
            }
            let _ = if readonly {
                self.query("SET TRANSACTION READ ONLY")?
            } else {
                self.query("SET TRANSACTION READ WRITE")?
            };
        }
        let _ = if consistent_snapshot {
            self.query("START TRANSACTION WITH CONSISTENT SNAPSHOT")?
        } else {
            self.query("START TRANSACTION")?
        };
        Ok(())
    }

    fn send_local_infile(&mut self, file_name: &[u8]) -> MyResult<()> {
        {
            let buffer_size = cmp::min(MAX_PAYLOAD_LEN - 1, self.max_allowed_packet - 1);
            let chunk = vec![0u8; buffer_size].into_boxed_slice();
            let maybe_handler = self.local_infile_handler.clone().or_else(|| {
                self.opts.get_local_infile_handler().clone()
            });
            let mut local_infile = LocalInfile {
                buffer: io::Cursor::new(chunk),
                conn: self
            };
            if let Some(handler) = maybe_handler {
                // Unwrap won't panic because we have exclusive access to `self` and this
                // method is not re-entrant, because `LocalInfile` does not expose the
                // connection.
                let handler_fn = &mut *handler.0.lock().unwrap();
                handler_fn(file_name, &mut local_infile)?;
            } else {
                let path = String::from_utf8_lossy(file_name);
                let path = path.into_owned();
                let path: path::PathBuf = path.into();
                let mut file = fs::File::open(&path)?;
                io::copy(&mut file, &mut local_infile)?;
            };
            local_infile.flush()?;
        }
        self.write_packet(&[])?;
        let pld = self.read_packet()?;
        if pld[0] == 0u8 {
            let ok = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
            self.handle_ok(&ok);
        }
        Ok(())
    }

    fn handle_result_set(&mut self) -> MyResult<Vec<Column>> {
        let pld = self.read_packet()?;
        match pld[0] {
            0x00 => {
                let ok = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
                self.handle_ok(&ok);
                Ok(Vec::new())
            },
            0xfb => {
                let mut reader = &pld[1..];
                let mut file_name = Vec::with_capacity(reader.len());
                reader.read_to_end(&mut file_name)?;
                match self.send_local_infile(file_name.as_ref()) {
                    Ok(_) => Ok(Vec::new()),
                    Err(err) => Err(err)
                }
            },
            0xff => {
                let err = parse_err_packet(pld.as_ref(), self.capability_flags)?;
                Err(MySqlError(err.into()))
            },
            _ => {
                let mut reader = &pld[..];
                let column_count = reader.read_lenenc_int()?;
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as usize);
                for _ in 0..column_count {
                    let pld = self.read_packet()?;
                    columns.push(column_from_payload(pld)?);
                }
                // skip eof packet
                self.read_packet()?;
                self.has_results = true;
                Ok(columns)
            }
        }
    }

    fn _query(&mut self, query: &str) -> MyResult<Vec<Column>> {
        self.write_command_data(Command::COM_QUERY, query.as_bytes())?;
        self.handle_result_set()
    }

    /// Executes [`COM_PING`](http://dev.mysql.com/doc/internals/en/com-ping.html)
    /// on `Conn`. Return `true` on success or `false` on error.
    pub fn ping(&mut self) -> bool {
        match self.write_command(Command::COM_PING) {
            Ok(_) => {
                self.drop_packet().is_ok()
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
        let _ = self._start_transaction(consistent_snapshot, isolation_level, readonly)?;
        Ok(Transaction::new(self))
    }

    /// Implements text protocol of mysql server.
    ///
    /// Executes mysql query on `Conn`. [`QueryResult`](struct.QueryResult.html)
    /// will borrow `Conn` until the end of its scope.
    pub fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult> {
        match self._query(query.as_ref()) {
            Ok(columns) => {
                Ok(QueryResult::new(ResultConnRef::ViaConnRef(self), columns, false))
            },
            Err(err) => Err(err),
        }
    }

    /// Performs query and returns first row.
    pub fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.query(query).and_then(|result| {
            for row in result {
                return row.map(|x| Some(from_row(x)));
            }
            return Ok(None)
        })
    }

    fn _true_prepare(&mut self,
                     query: &str,
                     named_params: Option<Vec<String>>) -> MyResult<InnerStmt> {
        self.write_command_data(Command::COM_STMT_PREPARE, query.as_bytes())?;
        let pld = self.read_packet()?;
        match pld[0] {
            0xff => {
                let err = parse_err_packet(pld.as_ref(), self.capability_flags)?;
                Err(MySqlError(err.into()))
            },
            _ => {
                let mut stmt = InnerStmt::from_payload(pld.as_ref(), named_params)?;
                if stmt.num_params() > 0 {
                    let mut params: Vec<Column> = Vec::with_capacity(stmt.num_params() as usize);
                    for _ in 0..stmt.num_params() {
                        let pld = self.read_packet()?;
                        params.push(column_from_payload(pld)?);
                    }
                    stmt.set_params(Some(params));
                    self.read_packet()?;
                }
                if stmt.num_columns() > 0 {
                    let mut columns: Vec<Column> = Vec::with_capacity(stmt.num_columns() as usize);
                    for _ in 0..stmt.num_columns() {
                        let pld = self.read_packet()?;
                        columns.push(column_from_payload(pld)?);
                    }
                    stmt.set_columns(Some(columns));
                    self.read_packet()?;
                }
                Ok(stmt)
            }
        }
    }

    fn _prepare(&mut self, query: &str, named_params: Option<Vec<String>>) -> MyResult<InnerStmt> {
        if let Some(inner_st) = self.stmt_cache.get(query) {
            let mut inner_st = inner_st.clone();
            inner_st.set_named_params(named_params);
            return Ok(inner_st);
        }

        let inner_st = self._true_prepare(query, named_params)?;

        if self.stmt_cache.get_cap() > 0 {
            if let Some(old_st) = self.stmt_cache.put(query.into(), inner_st.clone()) {
                let mut stmt_id = [0u8; 4];
                (&mut stmt_id[..]).write_u32::<LE>(old_st.id())?;
                self.write_command_data(Command::COM_STMT_CLOSE, &stmt_id[..])?;
            }
        }

        Ok(inner_st)
    }

    /// Implements binary protocol of mysql server.
    ///
    /// Prepares mysql statement on `Conn`. [`Stmt`](struct.Stmt.html) will
    /// borrow `Conn` until the end of its scope.
    ///
    /// This call will take statement from cache if has been prepared on this connection.
    ///
    /// ### JSON caveats
    ///
    /// For the following statement you will get somewhat unexpected result `{"a": 0}`, because
    /// booleans in mysql binary protocol is `TINYINT(1)` and will be interpreted as `0`:
    ///
    /// ```ignore
    /// pool.prep_exec(r#"SELECT JSON_REPLACE('{"a": true}', '$.a', ?)"#, (false,));
    /// ```
    ///
    /// You should wrap such parameters to a proper json value. For example if you are using
    /// *rustc_serialize* for Json support:
    ///
    /// ```ignore
    /// pool.prep_exec(r#"SELECT JSON_REPLACE('{"a": true}', '$.a', ?)"#, (Json::Boolean(false),));
    /// ```
    ///
    /// ### Named parameters support
    ///
    /// `prepare` supports named parameters in form of `:named_param_name`. Allowed characters for
    /// parameter name is `[a-z_]`. Named parameters will be converted to positional before actual
    /// call to prepare so `SELECT :a-:b, :a*:b` is actually `SELECT ?-?, ?*?`.
    ///
    /// ```
    /// # #[macro_use] extern crate mysql; fn main() {
    /// # use mysql::{Pool, Opts, OptsBuilder, from_row};
    /// # use mysql::Error::DriverError;
    /// # use mysql::DriverError::MixedParams;
    /// # use mysql::DriverError::MissingNamedParameter;
    /// # use mysql::DriverError::NamedParamsForPositionalQuery;
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
    ///     DriverError(e) => {
    ///         assert_eq!(MissingNamedParameter("name".into()), e);
    ///     }
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
    pub fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt> {
        let query = query.as_ref();
        let (named_params, real_query) = parse_named_params(query)?;
        match self._prepare(real_query.borrow(), named_params) {
            Ok(stmt) => Ok(Stmt::new(stmt, self)),
            Err(err) => Err(err),
        }
    }

    /// Prepares and executes statement in one call. See
    /// ['Conn::prepare'](struct.Conn.html#method.prepare)
    ///
    /// This call will take statement from cache if has been prepared on this connection.
    pub fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult>
    where A: AsRef<str>,
          T: Into<Params> {
        self.prepare(query)?.prep_exec(params.into())
    }

    /// Executes statement and returns first row.
    pub fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
    where Q: AsRef<str>,
          P: Into<Params>,
          T: FromRow
    {
        self.prep_exec(query, params).and_then(|result| {
            for row in result {
                return row.map(|x| Some(from_row(x)));
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
        for row in self.query(format!("SELECT @@{}", name)).unwrap() {
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

    fn next_bin(&mut self, columns: &Vec<Column>) -> MyResult<Option<SmallVec<[Value; 12]>>> {
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
            let p = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
            self.handle_ok(&p);
            return Ok(None);
        }
        let res = read_bin_values(&*pld, columns.as_ref());
        match res {
            Ok(p) => Ok(Some(p)),
            Err(e) => {
                self.has_results = false;
                Err(IoError(e))
            }
        }
    }

    fn next_text(&mut self, col_count: usize) -> MyResult<Option<SmallVec<[Value; 12]>>> {
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
                let p = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
                self.handle_ok(&p);
                return Ok(None);
            } else /* x == 0xff */ {
                let p = parse_err_packet(pld.as_ref(), self.capability_flags);
                match p {
                    Ok(p) => return Err(MySqlError(p.into())),
                    Err(err) => return Err(IoError(err))
                }
            }
        }
        let res = read_text_values(&*pld, col_count);
        match res {
            Ok(p) => Ok(Some(p)),
            Err(err) => {
                self.has_results = false;
                Err(IoError(err))
            }
        }
    }

    fn has_stmt(&self, query: &str) -> bool {
        self.stmt_cache.contains(query)
    }

    /// Sets a callback to handle requests for local files. These are
    /// caused by using `LOAD DATA LOCAL INFILE` queries. The
    /// callback is passed the filename, and a `Write`able object
    /// to receive the contents of that file.
    /// Specifying `None` will reset the handler to the one specified
    /// in the `Opts` for this connection.
    pub fn set_local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) {
        self.local_infile_handler = handler;
    }

    pub fn no_backslash_escape(&self) -> bool {
        self.status_flags.contains(StatusFlags::SERVER_STATUS_NO_BACKSLASH_ESCAPES)
    }
}

impl GenericConnection for Conn {
    fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult> {
        self.query(query)
    }

    fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.first(query)
    }

    fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt> {
        self.prepare(query)
    }

    fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult>
        where A: AsRef<str>, T: Into<Params> {
        self.prep_exec(query, params)
    }

    fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
        where Q: AsRef<str>, P: Into<Params>, T: FromRow {
        self.first_exec(query, params)
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        let stmt_cache = mem::replace(&mut self.stmt_cache, StmtCache::new(0));
        let mut stmt_id = [0u8; 4];
        for (_, inner_st) in stmt_cache.into_iter() {
            let _ = (&mut stmt_id[..]).write_u32::<LE>(inner_st.id());
            let _ = self.write_command_data(Command::COM_STMT_CLOSE, &stmt_id[..]);
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
/// # use mysql::Pool;
/// # use mysql::{Opts, OptsBuilder, from_row};
/// # fn get_opts() -> Opts {
/// #     let user = "root";
/// #     let addr = "127.0.0.1";
/// #     let pwd: String = ::std::env::var("MYSQL_SERVER_PASS").unwrap_or("password".to_string());
/// #     let port: u16 = ::std::env::var("MYSQL_SERVER_PORT").ok()
/// #                                .map(|my_port| my_port.parse().ok().unwrap_or(3307))
/// #                                .unwrap_or(3307);
/// #     let mut builder = OptsBuilder::default();
/// #     builder.user(Some(user))
/// #            .pass(Some(pwd))
/// #            .ip_or_hostname(Some(addr))
/// #            .tcp_port(port)
/// #            .init(vec!["SET GLOBAL sql_mode = 'TRADITIONAL'"]);
/// #     builder.into()
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
    fn new(conn: ResultConnRef<'a>,
           columns: Vec<Column>,
           is_bin: bool) -> QueryResult<'a>
    {
        QueryResult {
            conn: conn,
            columns: Arc::new(columns),
            is_bin: is_bin
        }
    }

    fn handle_if_more_results(&mut self) -> Option<MyResult<Row>> {
        if self.conn.status_flags.contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS) {
            match self.conn.handle_result_set() {
                Ok(cols) => {
                    self.columns = Arc::new(cols);
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
        self.conn.warnings
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// info.
    pub fn info(&self) -> Vec<u8> {
        self.conn.info.as_ref().map(Clone::clone).unwrap_or(vec![])
    }

    /// Returns index of a `QueryResult`'s column by name.
    pub fn column_index<T: AsRef<str>>(&self, name: T) -> Option<usize> {
        let name = name.as_ref().as_bytes();
        for (i, c) in self.columns.iter().enumerate() {
            if c.name_ref() == name {
                return Some(i)
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
                    Some(values) => Some(Ok(new_row(values, self.columns.clone()))),
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
    static ADDR: &'static str = "localhost";
    static PORT: u16          = 3307;

    #[cfg(all(feature = "ssl", target_os = "macos"))]
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
            .verify_peer(true)
            .ssl_opts(Some(Some(("tests/client.p12", "pass", vec!["tests/ca-cert.cer"]))));
        builder.into()
    }

    #[cfg(all(feature = "ssl", not(target_os = "macos"), unix))]
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
               .verify_peer(true)
               .ssl_opts(Some(("tests/ca-cert.pem", None::<(String, String)>)));
        builder.into()
    }

    #[cfg(any(not(feature = "ssl"), target_os = "windows"))]
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
        use LocalInfileHandler;
        use Value::{NULL, Int, Bytes, Date};
        use from_row;
        use from_value;
        use prelude::ToValue;
        use super::get_opts;

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
        fn should_handle_LOCAL_INFILE_with_custom_handler() {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query("CREATE TEMPORARY TABLE x.tbl(a TEXT)").unwrap();
            conn.set_local_infile_handler(Some(
                LocalInfileHandler::new(|_, stream| {
                    let mut cell_data = vec![b'Z'; 65535];
                    cell_data.push(b'\n');
                    for _ in 0..1536 {
                        stream.write_all(&*cell_data)?;
                    }
                    Ok(())
                })
            ));
            conn.query("LOAD DATA LOCAL INFILE 'file_name' INTO TABLE x.tbl").unwrap();
            let count = conn.query("SELECT * FROM x.tbl").unwrap().map(|row| {
                assert_eq!(from_row::<(Vec<u8>,)>(row.unwrap()).0.len(), 65535);
                1
            }).sum::<usize>();
            assert_eq!(count, 1536);
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
        fn should_connect_via_socket_for_127_0_0_1() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            #[cfg(all(feature = "ssl", not(target_os = "windows")))]
            opts.ssl_opts::<String, String, String>(None);
            let conn = Conn::new(opts).unwrap();
            let debug_format = format!("{:#?}", conn);
            assert!(debug_format.contains("SocketStream"));
        }

        #[test]
        fn should_connect_via_socket_localhost() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.ip_or_hostname(Some("localhost"));
            #[cfg(all(feature = "ssl", not(target_os = "windows")))]
            opts.ssl_opts::<String, String, String>(None);
            let conn = Conn::new(opts).unwrap();
            let debug_format = format!("{:?}", conn);
            assert!(debug_format.contains("SocketStream"));
        }

        #[test]
        #[cfg(all(feature = "ssl", any(target_os = "macos", unix)))]
        fn should_connect_via_ssl() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            let conn = Conn::new(opts).unwrap();
            let debug_format = format!("{:#?}", conn);
            assert!(debug_format.contains("Secure stream"));
        }

        #[test]
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
        fn should_handle_tcp_connect_timeout() {
            use error::Error::DriverError;
            use error::DriverError::ConnectTimeout;

            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            assert!(Conn::new(opts).unwrap().ping());


            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            opts.ip_or_hostname(Some("192.168.255.255"));
            match Conn::new(opts).unwrap_err() {
                DriverError(ConnectTimeout) => {},
                err => panic!("Unexpected error: {}", err),
            }
        }

        #[test]
        #[cfg(not(feature = "ssl"))]
        fn should_bind_before_connect() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.ip_or_hostname(Some("127.0.0.1"));
            opts.bind_address(Some(([127, 0, 0, 1], 27272)));
            let conn = Conn::new(opts).unwrap();
            let debug_format: String = format!("{:?}", conn);
            assert!(debug_format.contains("addr: V4(127.0.0.1:27272)"));
        }

        #[test]
        #[cfg(not(feature = "ssl"))]
        fn should_bind_before_connect_with_timeout() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.ip_or_hostname(Some("127.0.0.1"));
            opts.bind_address(Some(([127, 0, 0, 1], 27273)));
            opts.tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            let mut conn = Conn::new(opts).unwrap();
            assert!(conn.ping());
            let debug_format: String = format!("{:?}", conn);
            assert!(debug_format.contains("addr: V4(127.0.0.1:27273)"));
        }

        #[test]
        fn should_not_cache_statements_if_stmt_cache_size_is_zero() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.stmt_cache_size(0);
            let mut conn = Conn::new(opts).unwrap();
            conn.prepare("DO 1").unwrap();
            conn.prepare("DO 2").unwrap();
            conn.prepare("DO 3").unwrap();
            let row = conn.first("SHOW SESSION STATUS LIKE 'Com_stmt_close';").unwrap().unwrap();
            assert_eq!(from_row::<(String, usize)>(row).1, 3);
        }

        #[test]
        fn should_hold_stmt_cache_size_bound() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.stmt_cache_size(3);
            let mut conn = Conn::new(opts).unwrap();
            conn.prepare("DO 1").unwrap();
            conn.prepare("DO 2").unwrap();
            conn.prepare("DO 3").unwrap();
            conn.prepare("DO 1").unwrap();
            conn.prepare("DO 4").unwrap();
            conn.prepare("DO 3").unwrap();
            conn.prepare("DO 5").unwrap();
            conn.prepare("DO 6").unwrap();
            let row = conn.first("SHOW SESSION STATUS LIKE 'Com_stmt_close';").unwrap().unwrap();
            assert_eq!(from_row::<(String, usize)>(row).1, 3);
            let order = conn.stmt_cache.iter().collect::<Vec<&String>>();
            assert_eq!(order, &["DO 3", "DO 5", "DO 6"]);
        }

        #[test]
        fn should_handle_json_columns() {
            #[cfg(feature = "rustc_serialize")]
            use rustc_serialize::json::Json;
            #[cfg(not(feature = "rustc_serialize"))]
            use serde_json::Value as Json;
            #[cfg(not(feature = "rustc_serialize"))]
            use std::str::FromStr;
            use Serialized;
            use Deserialized;

            #[cfg(feature = "rustc_serialize")]
            #[derive(RustcDecodable, RustcEncodable, Debug, Eq, PartialEq)]
            pub struct DecTest {
                foo: String,
                quux: (u64, String),
            }

            #[cfg(not(feature = "rustc_serialize"))]
            #[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
            pub struct DecTest {
                foo: String,
                quux: (u64, String),
            }

            let decodable = DecTest {
                foo: "bar".into(),
                quux: (42, "hello".into()),
            };

            let mut conn = Conn::new(get_opts()).unwrap();
            if conn.query("CREATE TEMPORARY TABLE x.tbl(a VARCHAR(32), b JSON)").is_err() {
                conn.query("CREATE TEMPORARY TABLE x.tbl(a VARCHAR(32), b TEXT)").unwrap();
            }
            conn.prep_exec(
                r#"INSERT INTO x.tbl VALUES ('hello', ?)"#,
                (Serialized(&decodable), )
            ).unwrap();

            let row = conn.first("SELECT a, b FROM x.tbl").unwrap().unwrap();
            let (a, b): (String, Json) = from_row(row);
            assert_eq!((a, b), ("hello".into(), Json::from_str(r#"{"foo": "bar", "quux": [42, "hello"]}"#).unwrap()));


            let row = conn.first_exec("SELECT a, b FROM x.tbl WHERE a = ?", ("hello", )).unwrap().unwrap();
            let (a, Deserialized(b)) = from_row(row);
            assert_eq!((a, b), (String::from("hello"), decodable));
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use super::get_opts;
        use Conn;
        use Value::NULL;

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
