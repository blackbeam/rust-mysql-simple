#[cfg(feature = "openssl")]
use openssl::{ssl, x509};

use std::fmt;
use std::default::{Default};
use std::old_io::{Reader, File, IoResult, EndOfFile};
use std::old_io::net::ip::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::old_io::net::tcp::{TcpStream};
use std::old_io::net::pipe::{UnixStream};
use std::num::{FromPrimitive};
use std::path::{BytesContainer};
use std::str::{FromStr};
use std::iter;
use super::consts;
use super::consts::Command;
use super::consts::ColumnType;
use super::io::MyStream;
use super::io::{
    MyReader,
    MyWriter,
    PlainStream
};
use super::io::TcpOrUnixStream::{TCPStream, UNIXStream};
#[cfg(feature = "ssl")]
use super::io::{MySslStream};
use super::error::MyError::{
    MyIoError,
    MySqlError,
    MyDriverError
};
use super::error::DriverError::{
    CouldNotConnect,
    UnsupportedProtocol,
    Protocol41NotSet,
    UnexpectedPacket,
    MismatchedStmtParams,
    SetupError,
    ReadOnlyTransNotSupported,
};
use super::error::MyResult;
#[cfg(feature = "ssl")]
use super::error::DriverError::SslNotSupported;
use super::scramble::{scramble};
use super::packet::{OkPacket, EOFPacket, ErrPacket, HandshakePacket, ServerVersion};
use super::value::Value;
use super::value::{ToValue, from_value, from_value_opt};
use super::value::Value::{NULL, Int, UInt, Float, Bytes, Date, Time};

pub mod pool;

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum IsolationLevel {
    ReadUncommied,
    ReadCommited,
    RepeaableRead,
    Serializable,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IsolationLevel::ReadUncommied => write!(f, "READ UNCOMMITTED"),
            IsolationLevel::ReadCommited => write!(f, "READ COMMITTED"),
            IsolationLevel::RepeaableRead => write!(f, "REPEATABLE READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

pub struct Transaction<'a> {
    conn: Option<&'a mut MyConn>,
    pooled_conn: Option<pool::MyPooledConn>,
    committed: bool,
    rollbacked: bool,
}

impl<'a> Transaction<'a> {
    fn new(conn: &'a mut MyConn) -> Transaction<'a> {
        Transaction {
            conn: Some(conn),
            pooled_conn: None,
            committed: false,
            rollbacked: false,
        }
    }

    fn new_pooled(conn: pool::MyPooledConn) -> Transaction<'a> {
        Transaction {
            conn: None,
            pooled_conn: Some(conn),
            committed: false,
            rollbacked: false,
        }
    }

    /// See [`MyConn#query`](struct.MyConn.html#method.query).
    pub fn query<'c>(&'c mut self, query: &str) -> MyResult<QueryResult<'c>> {
        if let Some(ref mut conn) = self.conn {
            conn.query(query)
        } else if let Some(ref mut conn) = self.pooled_conn {
            conn.query(query)
        } else {
            unreachable!()
        }
    }

    /// See [`MyConn#prepare`](struct.MyConn.html#method.prepare).
    pub fn prepare<'c>(&'c mut self, query: &str) -> MyResult<Stmt<'c>> {
        if let Some(ref mut conn) = self.conn {
            conn.prepare(query)
        } else if let Some(ref mut conn) = self.pooled_conn {
            conn.prepare(query)
        } else {
            unreachable!()
        }
    }

    /// Will consume and commit transaction.
    pub fn commit(mut self) -> MyResult<()> {
        if let Some(ref mut conn) = self.conn {
            try!(conn.query("COMMIT"));
        } else if let Some(ref mut conn) = self.pooled_conn {
            try!(conn.query("COMMIT"));
        } else {
            unreachable!()
        }
        self.committed = true;
        Ok(())
    }

    /// Will consume and rollback transaction. You also can rely on `Drop` implementation but it
    /// will swallow errors.
    pub fn rollback(mut self) -> MyResult<()> {
        if let Some(ref mut conn) = self.conn {
            let _ = try!(conn.query("ROLLBACK"));
        } else if let Some(ref mut conn) = self.pooled_conn {
            let _ = try!(conn.query("ROLLBACK"));
        }
        self.rollbacked = true;
        Ok(())
    }
}

#[unsafe_destructor]
impl<'a> Drop for Transaction<'a> {
    /// Will rollback transaction.
    fn drop(&mut self) {
        if ! self.committed && ! self.rollbacked {
            if let Some(ref mut conn) = self.conn {
                let _ = conn.query("ROLLBACK");
            } else if let Some(ref mut conn) = self.pooled_conn {
                let _ = conn.query("ROLLBACK");
            }
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
#[derive(Eq, PartialEq, Clone)]
struct InnerStmt {
    params: Option<Vec<Column>>,
    columns: Option<Vec<Column>>,
    statement_id: u32,
    num_columns: u16,
    num_params: u16,
    warning_count: u16,
}

impl InnerStmt {
    fn from_payload(pld: &[u8]) -> IoResult<InnerStmt> {
        let mut reader = &pld[];
        try!(reader.read_u8());
        let statement_id = try!(reader.read_le_u32());
        let num_columns = try!(reader.read_le_u16());
        let num_params = try!(reader.read_le_u16());
        let warning_count = try!(reader.read_le_u16());
        Ok(InnerStmt{statement_id: statement_id,
                     num_columns: num_columns,
                     num_params: num_params,
                     warning_count: warning_count,
                     params: None,
                     columns: None})
    }
}

/// Mysql
/// [prepared statement](http://dev.mysql.com/doc/internals/en/prepared-statements.html).
pub struct Stmt<'a> {
    stmt: InnerStmt,
    conn: Option<&'a mut MyConn>,
    pooled_conn: Option<pool::MyPooledConn>
}

impl<'a> Stmt<'a> {
    fn new(stmt: InnerStmt, conn: &'a mut MyConn) -> Stmt<'a> {
        Stmt{stmt: stmt, conn: Some(conn), pooled_conn: None}
    }

    fn new_pooled(stmt: InnerStmt, pooled_conn: pool::MyPooledConn) -> Stmt<'a> {
        Stmt{stmt: stmt, conn: None, pooled_conn: Some(pooled_conn)}
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s params if any.
    pub fn params_ref(&self) -> Option<&[Column]> {
        match self.stmt.params {
            Some(ref params) => Some(&params[]),
            None => None
        }
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s columns if any.
    pub fn columns_ref(&self) -> Option<&[Column]> {
        match self.stmt.columns {
            Some(ref columns) => Some(&columns[]),
            None => None
        }
    }

    /// Returns index of a `Stmt`'s column by name.
    pub fn column_index<T>(&self, name: T) -> Option<usize>
    where T: BytesContainer {
        match self.stmt.columns {
            None => None,
            Some(ref columns) => {
                let name = name.container_as_bytes();
                for (i, c) in columns.iter().enumerate() {
                    if &c.name[] == name {
                        return Some(i)
                    }
                }
                None
            }
        }
    }

    /// Executes prepared statement with an arguments passed as a slice of a
    /// references to a [`ToValue`](../value/trait.ToValue.html) trait
    /// implementors.
    pub fn execute<'s>(&'s mut self, params: &[&ToValue]) -> MyResult<QueryResult<'s>> {
        if self.conn.is_some() {
            let conn_ref = self.conn.as_mut().unwrap();
            conn_ref.execute(&self.stmt, params)
        } else {
            let conn_ref = self.pooled_conn.as_mut().unwrap().as_mut();
            conn_ref.execute(&self.stmt, params)
        }
    }
}

#[unsafe_destructor]
impl<'a> Drop for Stmt<'a> {
    fn drop(&mut self) {
        let data = [(self.stmt.statement_id & 0x000000FF) as u8,
                    ((self.stmt.statement_id & 0x0000FF00) >> 8) as u8,
                    ((self.stmt.statement_id & 0x00FF0000) >> 16) as u8,
                    ((self.stmt.statement_id & 0xFF000000) >> 24) as u8,];
        if self.conn.is_some() {
            let conn = self.conn.as_mut().unwrap();
            let _ = conn.write_command_data(Command::COM_STMT_CLOSE, &data);
        } else {
            let conn = self.pooled_conn.as_mut().unwrap().as_mut();
            let _ = conn.write_command_data(Command::COM_STMT_CLOSE, &data);
        }
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
#[derive(Clone, Eq, PartialEq)]
pub struct Column {
    /// Schema name.
    pub schema: Vec<u8>,
    /// Virtual table name.
    pub table: Vec<u8>,
    /// Phisical table name.
    pub org_table: Vec<u8>,
    /// Virtual column name.
    pub name: Vec<u8>,
    /// Phisical column name.
    pub org_name: Vec<u8>,
    /// Default values.
    pub default_values: Vec<u8>,
    /// Maximum length of the field.
    pub column_length: u32,
    /// Column character set.
    pub character_set: u16,
    /// Flags.
    pub flags: consts::ColumnFlags,
    /// Column type.
    pub column_type: consts::ColumnType,
    /// Max shown decimal digits
    pub decimals: u8
}

impl Column {
    #[inline]
    fn from_payload(command: u8, pld: &[u8]) -> IoResult<Column> {
        let mut reader = &pld[];
        // Skip catalog
        let _ = try!(reader.read_lenenc_bytes());
        let schema = try!(reader.read_lenenc_bytes());
        let table = try!(reader.read_lenenc_bytes());
        let org_table = try!(reader.read_lenenc_bytes());
        let name = try!(reader.read_lenenc_bytes());
        let org_name = try!(reader.read_lenenc_bytes());
        let _ = try!(reader.read_lenenc_int());
        let character_set = try!(reader.read_le_u16());
        let column_length = try!(reader.read_le_u32());
        let column_type = try!(reader.read_u8());
        let flags = consts::ColumnFlags::from_bits_truncate(try!(reader.read_le_u16()));
        let decimals = try!(reader.read_u8());
        // skip filler
        try!(reader.read_le_u16());
        let mut default_values = Vec::with_capacity(0);
        if command == Command::COM_FIELD_LIST as u8 {
            let len = try!(reader.read_lenenc_int());
            default_values = try!(reader.read_exact(len as usize));
        }
        Ok(Column{schema: schema,
                  table: table,
                  org_table: org_table,
                  name: name,
                  org_name: org_name,
                  character_set: character_set,
                  column_length: column_length,
                  column_type: FromPrimitive::from_u8(column_type).unwrap(),
                  flags: flags,
                  decimals: decimals,
                  default_values: default_values})
    }
}

/***
 *    888b     d888           .d88888b.           888
 *    8888b   d8888          d88P" "Y88b          888
 *    88888b.d88888          888     888          888
 *    888Y88888P888 888  888 888     888 88888b.  888888 .d8888b
 *    888 Y888P 888 888  888 888     888 888 "88b 888    88K
 *    888  Y8P  888 888  888 888     888 888  888 888    "Y8888b.
 *    888   "   888 Y88b 888 Y88b. .d88P 888 d88P Y88b.       X88
 *    888       888  "Y88888  "Y88888P"  88888P"   "Y888  88888P'
 *                       888             888
 *                  Y8b d88P             888
 *                   "Y88P"              888
 */
/// Mysql connection options.
///
/// For example:
///
/// ```ignore
/// let opts = MyOpts {
///     user: Some("username".to_string()),
///     pass: Some("password".to_string()),
///     db_name: Some("mydatabase".to_string()),
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Eq, PartialEq)]
pub struct MyOpts {
    /// TCP address of mysql server (defaults to `127.0.0.1`).
    pub tcp_addr: Option<String>,
    /// TCP port of mysql server (defaults to `3306`).
    pub tcp_port: u16,
    /// Path to unix socket of mysql server (defaults to `None`).
    pub unix_addr: Option<Path>,
    /// User (defaults to `None`).
    pub user: Option<String>,
    /// Password (defaults to `None`).
    pub pass: Option<String>,
    /// Database name (defaults to `None`).
    pub db_name: Option<String>,
    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket after TCP connection to `127.0.0.1` if `true`.
    pub prefer_socket: bool,
    /// TCP keepalive timeout in seconds (defaults to one hour).
    pub keepalive_timeout: Option<usize>,

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`).
    /// Only make sense if ssl_opts is not None.
    pub verify_peer: bool,

    #[cfg(feature = "ssl")]
    /// #### Only available if `ssl` feature enabled.
    /// SSL certificates and keys in pem format.
    /// If not None, then ssl connection implied.
    ///
    /// `Option<(ca_cert, Option<(client_cert, client_key)>)>.`
    pub ssl_opts: Option<(Path, Option<(Path, Path)>)>
}

impl MyOpts {
    fn get_user(&self) -> String {
        match self.user {
            Some(ref x) => x.clone(),
            None => String::from_str("")
        }
    }
    fn get_pass(&self) -> String {
        match self.pass {
            Some(ref x) => x.clone(),
            None => String::from_str("")
        }
    }
    fn get_db_name(&self) -> String {
        match self.db_name {
            Some(ref x) => x.clone(),
            None => String::from_str("")
        }
    }
}

#[cfg(feature = "ssl")]
impl Default for MyOpts {
    fn default() -> MyOpts {
        MyOpts{
            tcp_addr: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            unix_addr: None,
            user: None,
            pass: None,
            db_name: None,
            prefer_socket: true,
            keepalive_timeout: Some(60 * 60),
            verify_peer: false,
            ssl_opts: None,
        }
    }
}

#[cfg(not(feature = "ssl"))]
impl Default for MyOpts {
    fn default() -> MyOpts {
        MyOpts{
            tcp_addr: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            unix_addr: None,
            user: None,
            pass: None,
            db_name: None,
            prefer_socket: true,
            keepalive_timeout: Some(60 * 60),
        }
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
pub struct MyConn {
    opts: MyOpts,
    stream: Option<MyStream>,
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
    #[cfg(feature = "openssl")]
    ssl_context: Option<ssl::SslContext>,
}

#[cfg(feature = "openssl")]
impl Default for MyConn {
    fn default() -> MyConn {
        MyConn{
            stream: None,
            seq_id: 0u8,
            capability_flags: consts::CapabilityFlags::empty(),
            status_flags: consts::StatusFlags::empty(),
            connection_id: 0u32,
            character_set: 0u8,
            affected_rows: 0u64,
            last_insert_id: 0u64,
            last_command: 0u8,
            max_allowed_packet: consts::MAX_PAYLOAD_LEN,
            opts: Default::default(),
            connected: false,
            has_results: false,
            ssl_context: None,
            server_version: (0, 0, 0),
        }
    }
}

#[cfg(not(feature = "ssl"))]
impl Default for MyConn {
    fn default() -> MyConn {
        MyConn{
            stream: None,
            seq_id: 0u8,
            capability_flags: consts::CapabilityFlags::empty(),
            status_flags: consts::StatusFlags::empty(),
            connection_id: 0u32,
            character_set: 0u8,
            affected_rows: 0u64,
            last_insert_id: 0u64,
            last_command: 0u8,
            max_allowed_packet: consts::MAX_PAYLOAD_LEN,
            opts: Default::default(),
            connected: false,
            has_results: false,
            server_version: (0, 0, 0),
        }
    }
}

impl MyConn {
    #[cfg(not(feature = "ssl"))]
    /// Creates new `MyConn`.
    pub fn new(opts: MyOpts) -> MyResult<MyConn> {
        let mut conn = MyConn{opts: opts, ..Default::default()};
        try!(conn.connect_stream());
        try!(conn.connect());
        if conn.opts.unix_addr.is_none() && conn.opts.prefer_socket {
            let addr: Option<IpAddr> = FromStr::from_str(
                &conn.opts.tcp_addr.as_ref().unwrap()[]).ok();
            if addr == Some(Ipv4Addr(127, 0, 0, 1)) ||
               addr == Some(Ipv6Addr(0, 0, 0, 0, 0, 0, 0, 1)) {
                match conn.get_system_var("socket") {
                    Some(path) => {
                        let opts = MyOpts{unix_addr: Some(Path::new(from_value::<Vec<u8>>(&path))),
                                          ..conn.opts.clone()};
                        return MyConn::new(opts).or(Ok(conn));
                    },
                    _ => return Ok(conn)
                }
            }
        }
        return Ok(conn);
    }

    #[cfg(feature = "ssl")]
    /// Creates new `MyConn`.
    pub fn new(opts: MyOpts) -> MyResult<MyConn> {
        let mut conn = MyConn{opts: opts, ..Default::default()};
        try!(conn.connect_stream());
        try!(conn.connect());
        if let None = conn.opts.ssl_opts {
            if conn.opts.unix_addr.is_none() && conn.opts.prefer_socket {
                let addr: Option<IpAddr> = FromStr::from_str(
                    &conn.opts.tcp_addr.as_ref().unwrap()[]).ok();
                if addr == Some(Ipv4Addr(127, 0, 0, 1)) ||
                   addr == Some(Ipv6Addr(0, 0, 0, 0, 0, 0, 0, 1)) {
                    match conn.get_system_var("socket") {
                        Some(path) => {
                            let opts = MyOpts{
                                unix_addr: Some(Path::new(from_value::<Vec<u8>>(&path))),
                                ..conn.opts.clone()
                            };
                            return MyConn::new(opts).or(Ok(conn));
                        },
                        _ => return Ok(conn)
                    }
                }
            }
        }
        return Ok(conn);
    }

    #[cfg(feature = "ssl")]
    /// Resets `MyConn` (drops state then reconnects).
    pub fn reset(&mut self) -> MyResult<()> {
        if self.server_version > (5, 7, 2) {
            try!(self.write_command(Command::COM_RESET_CONNECTION));
            self.read_packet()
            .and_then(|pld| {
                match pld[0] {
                    0 => {
                        let ok = try!(OkPacket::from_payload(&pld[]));
                        self.handle_ok(&ok);
                        self.last_command = 0;
                        Ok(())
                    },
                    _ => {
                        let err = try!(ErrPacket::from_payload(&pld[]));
                        Err(MySqlError(err))
                    }
                }
            })
        } else {
            self.stream = None;
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
            self.ssl_context = None;
            try!(self.connect_stream());
            self.connect()
        }

    }

    #[cfg(not(feature = "ssl"))]
    /// Resets `MyConn` (drops state then reconnects).
    pub fn reset(&mut self) -> MyResult<()> {
        if self.server_version > (5, 7, 2) {
            try!(self.write_command(Command::COM_RESET_CONNECTION));
            self.read_packet()
            .and_then(|pld| {
                match pld[0] {
                    0 => {
                        let ok = try!(OkPacket::from_payload(&pld[]));
                        self.handle_ok(&ok);
                        self.last_command = 0;
                        Ok(())
                    },
                    _ => {
                        let err = try!(ErrPacket::from_payload(&pld[]));
                        Err(MySqlError(err))
                    }
                }
            })
        } else {
            self.stream = None;
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
    }

    fn get_mut_stream<'a>(&'a mut self) -> &'a mut MyStream {
        self.stream.as_mut().unwrap()
    }

    #[cfg(feature = "openssl")]
    fn switch_to_ssl(&mut self) -> MyResult<()> {
        let stream = self.stream.take().unwrap();
        match stream {
            MyStream::InsecureStream(mut s) => {
                let mut ctx = try!(ssl::SslContext::new(ssl::SslMethod::Tlsv1));
                if self.opts.verify_peer {
                    ctx.set_verify(ssl::SslVerifyMode::SslVerifyPeer, None);
                } else {
                    ctx.set_verify(ssl::SslVerifyMode::SslVerifyNone, None);
                }
                match self.opts.ssl_opts {
                    Some((ref ca_cert, None)) => {
                        ctx.set_CA_file(ca_cert);
                    },
                    Some((ref ca_cert, Some((ref client_cert, ref client_key)))) => {
                        ctx.set_CA_file(ca_cert);
                        ctx.set_certificate_file(client_cert, x509::X509FileType::PEM);
                        ctx.set_private_key_file(client_key, x509::X509FileType::PEM);
                    },
                    _ => { unreachable!() }
                }
                s.wrapped = true;
                self.stream = Some(MyStream::SecureStream(MySslStream(try!(ssl::SslStream::new(&ctx, s)))));
                self.ssl_context = Some(ctx);
            },
            s => {
                self.stream = Some(s);
            }
        }
        Ok(())
    }

    fn connect_stream(&mut self) -> MyResult<()> {
        if self.opts.unix_addr.is_some() {
            match UnixStream::connect(self.opts.unix_addr.as_ref().unwrap()) {
                Ok(stream) => {
                    self.stream = Some(MyStream::InsecureStream(PlainStream{s: UNIXStream(stream), wrapped: false}));
                    return Ok(());
                },
                _ => {
                    let path_str = format!("{}",
                                           self.opts.unix_addr.as_ref()
                                           .unwrap()
                                           .display()
                                   ).to_string();
                    return Err(MyDriverError(CouldNotConnect(Some(path_str))));
                }
            }
        }
        if self.opts.tcp_addr.is_some() {
            match TcpStream::connect(
                (&self.opts.tcp_addr.as_ref().unwrap()[],
                self.opts.tcp_port))
            {
                Ok(mut stream) => {
                    // keepalive one hour
                    let keepalive_timeout = self.opts.keepalive_timeout.clone();
                    try!(stream.set_keepalive(keepalive_timeout));
                    self.stream = Some(MyStream::InsecureStream(PlainStream{s: TCPStream(stream), wrapped: false}));
                    return Ok(());
                },
                _ => {
                    return Err(MyDriverError(CouldNotConnect(self.opts.tcp_addr.clone())));
                }
            }
        }
        return Err(MyDriverError(CouldNotConnect(None)));
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
        self.seq_id = try!(self.get_mut_stream()
                               .write_packet(data, seq_id, max_allowed_packet));
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
            let handshake = try!(HandshakePacket::from_payload(&pld[]));
            if handshake.protocol_version != 10u8 {
                return Err(MyDriverError(UnsupportedProtocol(handshake.protocol_version)));
            }
            if !handshake.capability_flags.contains(consts::CLIENT_PROTOCOL_41) {
                return Err(MyDriverError(Protocol41NotSet));
            }
            self.handle_handshake(&handshake);
            self.do_handshake_response(&handshake)
        }).and_then(|_| {
            self.read_packet()
        }).and_then(|pld| {
            match pld[0] {
                0u8 => {
                    let ok = try!(OkPacket::from_payload(&pld[]));
                    self.handle_ok(&ok);
                    Ok(())
                },
                0xffu8 => {
                    let err = try!(ErrPacket::from_payload(&pld[]));
                    Err(MySqlError(err))
                },
                _ => Err(MyDriverError(UnexpectedPacket))
            }
        })
    }

    #[cfg(feature = "ssl")]
    fn do_handshake(&mut self) -> MyResult<()> {
        self.read_packet().and_then(|pld| {
            let handshake = try!(HandshakePacket::from_payload(&pld[]));
            if handshake.protocol_version != 10u8 {
                return Err(MyDriverError(UnsupportedProtocol(handshake.protocol_version)));
            }
            if !handshake.capability_flags.contains(consts::CLIENT_PROTOCOL_41) {
                return Err(MyDriverError(Protocol41NotSet));
            }
            self.handle_handshake(&handshake);
            if self.opts.ssl_opts.is_some() {
                if let Some(MyStream::InsecureStream(PlainStream{s: TCPStream(_), ..})) = self.stream {
                    if !handshake.capability_flags.contains(consts::CLIENT_SSL) {
                        return Err(MyDriverError(SslNotSupported));
                    } else {
                        try!(self.do_ssl_request());
                        try!(self.switch_to_ssl());
                    }
                }
            }
            self.do_handshake_response(&handshake)
        }).and_then(|_| {
            self.read_packet()
        }).and_then(|pld| {
            match pld[0] {
                0u8 => {
                    let ok = try!(OkPacket::from_payload(&pld[]));
                    self.handle_ok(&ok);
                    Ok(())
                },
                0xffu8 => {
                    let err = try!(ErrPacket::from_payload(&pld[]));
                    Err(MySqlError(err))
                },
                _ => Err(MyDriverError(UnexpectedPacket))
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
        if self.opts.get_db_name().len() > 0 {
            client_flags.insert(consts::CLIENT_CONNECT_WITH_DB);
        }
        if let Some(MyStream::InsecureStream(_)) = self.stream {
            if let Some(_) = self.opts.ssl_opts {
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
        if self.opts.get_db_name().len() > 0 {
            client_flags.insert(consts::CLIENT_CONNECT_WITH_DB);
        }
        client_flags
    }

    #[cfg(feature = "ssl")]
    fn do_ssl_request(&mut self) -> MyResult<()> {
        let client_flags = self.get_client_flags();
        let mut writer = Vec::with_capacity(4 + 4 + 1 + 23);
        try!(writer.write_le_u32(client_flags.bits()));
        try!(writer.write_all(&[0u8; 4]));
        try!(writer.write_u8(consts::UTF8_GENERAL_CI));
        try!(writer.write_all(&[0u8; 23]));
        self.write_packet(&writer[])
    }

    fn do_handshake_response(&mut self, hp: &HandshakePacket) -> MyResult<()> {
        let client_flags = self.get_client_flags();
        let scramble_buf = scramble(&hp.auth_plugin_data[],
                                    self.opts.get_pass().as_bytes());
        let scramble_buf_len = if scramble_buf.is_some() { 20 } else { 0 };
        let mut payload_len = 4 + 4 + 1 + 23 + self.opts.get_user().len() + 1 + 1 + scramble_buf_len;
        if self.opts.get_db_name().len() > 0 {
            payload_len += self.opts.get_db_name().len() + 1;
        }
        let mut writer = Vec::with_capacity(payload_len);
        try!(writer.write_le_u32(client_flags.bits()));
        try!(writer.write_all(&[0u8; 4]));
        try!(writer.write_u8(consts::UTF8_GENERAL_CI));
        try!(writer.write_all(&[0u8; 23]));
        try!(writer.write_str(&self.opts.get_user()[]));
        try!(writer.write_u8(0u8));
        try!(writer.write_u8(scramble_buf_len as u8));
        if scramble_buf.is_some() {
            try!(writer.write_all(&scramble_buf.unwrap()[]));
        }
        if self.opts.get_db_name().len() > 0 {
            try!(writer.write_str(&self.opts.get_db_name()[]));
            try!(writer.write_u8(0u8));
        }
        self.write_packet(&writer[])
    }

    fn write_command(&mut self, cmd: consts::Command) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd as u8;
        self.write_packet(&[cmd as u8])
    }

    fn write_command_data(&mut self, cmd: consts::Command, buf: &[u8]) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd as u8;
        let mut writer = Vec::with_capacity(buf.len() + 1);
        let _ = writer.write_u8(cmd as u8);
        let _ = writer.write_all(buf);
        self.write_packet(&writer[])
    }

    /// Executes [`COM_PING`](http://dev.mysql.com/doc/internals/en/com-ping.html)
    /// on `MyConn`. Return `true` on success or `false` on error.
    pub fn ping(&mut self) -> bool {
        match self.write_command(Command::COM_PING) {
            Ok(_) => {
                // ommit ok packet
                let _ = self.read_packet();
                true
            },
            _ => false
        }
    }

    fn send_long_data(&mut self, stmt: &InnerStmt, params: &[Value], ids: Vec<u16>) -> MyResult<()> {
        for &id in ids.iter() {
            match params[id as usize] {
                Bytes(ref x) => {
                    for chunk in x.chunks(self.max_allowed_packet - 7) {
                        let chunk_len = chunk.len() + 7;
                        let mut writer = Vec::with_capacity(chunk_len);
                        try!(writer.write_le_u32(stmt.statement_id));
                        try!(writer.write_le_u16(id));
                        try!(writer.write_all(chunk));
                        try!(self.write_command_data(Command::COM_STMT_SEND_LONG_DATA,
                                                     &writer[]));
                    }
                },
                _ => (/* quite strange so do nothing */)
            }
        }
        Ok(())
    }

    fn _execute(&mut self, stmt: &InnerStmt, params: &[Value]) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        if stmt.num_params != params.len() as u16 {
            return Err(MyDriverError(MismatchedStmtParams(stmt.num_params, params.len())));
        }
        let mut writer: Vec<u8>;
        match stmt.params {
            Some(ref sparams) => {
                let (bitmap, values, large_ids) =
                    try!(Value::to_bin_payload(&sparams[],
                                               params,
                                               self.max_allowed_packet));
                match large_ids {
                    Some(ids) => try!(self.send_long_data(stmt, params, ids)),
                    _ => ()
                }
                writer = Vec::with_capacity(9 + bitmap.len() + 1 +
                                                  params.len() * 2 +
                                                  values.len());
                try!(writer.write_le_u32(stmt.statement_id));
                try!(writer.write_u8(0u8));
                try!(writer.write_le_u32(1u32));
                try!(writer.write_all(&bitmap[]));
                try!(writer.write_u8(1u8));
                for i in 0..params.len() {
                    match params[i] {
                        NULL => try!(writer.write_all(
                            &[sparams[i].column_type as u8, 0u8])),
                        Bytes(..) => try!(
                            writer.write_all(&[ColumnType::MYSQL_TYPE_VAR_STRING as u8,
                                          0u8])),
                        Int(..) => try!(
                            writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8,
                                          0u8])),
                        UInt(..) => try!(
                            writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8,
                                          128u8])),
                        Float(..) => try!(
                            writer.write_all(&[ColumnType::MYSQL_TYPE_DOUBLE as u8,
                                          0u8])),
                        Date(..) => try!(
                            writer.write_all(&[ColumnType::MYSQL_TYPE_DATE as u8,
                                          0u8])),
                        Time(..) => try!(
                            writer.write_all(&[ColumnType::MYSQL_TYPE_TIME as u8,
                                          0u8]))
                    }
                }
                try!(writer.write_all(&values[]));
            },
            None => {
                writer = Vec::with_capacity(4 + 1 + 4);
                try!(writer.write_le_u32(stmt.statement_id));
                try!(writer.write_u8(0u8));
                try!(writer.write_le_u32(1u32));
            }
        }
        try!(self.write_command_data(Command::COM_STMT_EXECUTE, &writer[]));
        self.handle_result_set()
    }

    fn execute<'a>(&'a mut self, stmt: &InnerStmt, params: &[&ToValue]) -> MyResult<QueryResult<'a>> {
        let _params: Vec<Value> = params.iter().map(|x| x.to_value() ).collect();
        match self._execute(stmt, &_params[]) {
            Ok((columns, ok_packet)) => {
                Ok(QueryResult::new(self, columns, ok_packet, true))
            },
            Err(err) => Err(err)
        }
    }

    fn _start_transaction(&mut self,
                          consistent_snapshot: bool,
                          isolation_level: Option<IsolationLevel>,
                          readonly: Option<bool>) -> MyResult<()> {
        if let Some(i_level) = isolation_level {
            let _ = try!(self.query(&format!("SET TRANSACTION ISOLATION LEVEL {}", i_level)[]));
        }
        if let Some(readonly) = readonly {
            if self.server_version < (5, 6, 5) {
                return Err(MyDriverError(ReadOnlyTransNotSupported));
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

    /// Starts new transaction with provided options.
    /// `readonly` is only available since MySQL 5.6.5.
    pub fn start_transaction<'a>(&'a mut self,
                                 consistent_snapshot: bool,
                                 isolation_level: Option<IsolationLevel>,
                                 readonly: Option<bool>) -> MyResult<Transaction<'a>> {
        let _ = try!(self._start_transaction(consistent_snapshot,
                                             isolation_level,
                                             readonly));
        Ok(Transaction::new(self))
    }

    fn send_local_infile(&mut self, file_name: &[u8]) -> MyResult<Option<OkPacket>> {
        let path = Path::new(file_name);
        let mut file = try!(File::open(&path));
        let mut chunk = iter::repeat(0u8)
                             .take(self.max_allowed_packet)
                             .collect::<Vec<u8>>();
        let mut r = file.read(&mut chunk[]);
        loop {
            match r {
                Ok(cnt) => {
                    try!(self.write_packet(&chunk[..cnt]));
                },
                Err(e) => {
                    if e.kind == EndOfFile {
                        break;
                    } else {
                        return Err(MyIoError(e));
                    }
                }
            }
            r = file.read(&mut chunk[]);
        }
        try!(self.write_packet(&[]));
        let pld = try!(self.read_packet());
        if pld[0] == 0u8 {
            let ok = try!(OkPacket::from_payload(&pld[]));
            self.handle_ok(&ok);
            return Ok(Some(ok));
        }
        Ok(None)
    }

    fn handle_result_set(&mut self) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        let pld = try!(self.read_packet());
        match pld[0] {
            0x00 => {
                let ok = try!(OkPacket::from_payload(&pld[]));
                self.handle_ok(&ok);
                Ok((Vec::new(), Some(ok)))
            },
            0xfb => {
                let mut reader = &pld[];
                try!(reader.read_u8());
                let file_name = try!(reader.read_to_end());
                match self.send_local_infile(&file_name[]) {
                    Ok(x) => Ok((Vec::new(), x)),
                    Err(err) => Err(err)
                }
            },
            0xff => {
                let err = try!(ErrPacket::from_payload(&pld[]));
                Err(MySqlError(err))
            },
            _ => {
                let mut reader = &pld[];
                let column_count = try!(reader.read_lenenc_int());
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as usize);
                for _ in (0..column_count) {
                    let pld = try!(self.read_packet());
                    columns.push(try!(Column::from_payload(self.last_command, &pld[])));
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

    /// Implements text protocol of mysql server.
    ///
    /// Executes mysql query on `MyConn`. [`QueryResult`](struct.QueryResult.html)
    /// will borrow `MyConn` until the end of its scope.
    pub fn query<'a>(&'a mut self, query: &str) -> MyResult<QueryResult<'a>> {
        match self._query(query) {
            Ok((columns, ok_packet)) => {
                Ok(QueryResult::new(self, columns, ok_packet, false))
            },
            Err(err) => {
                Err(err)
            }
        }
    }

    fn _prepare(&mut self, query: &str) -> MyResult<InnerStmt> {
        try!(self.write_command_data(Command::COM_STMT_PREPARE, query.as_bytes()));
        let pld = try!(self.read_packet());
        match pld[0] {
            0xff => {
                let err =  try!(ErrPacket::from_payload(&pld[]));
                Err(MySqlError(err))
            },
            _ => {
                let mut stmt = try!(InnerStmt::from_payload(&pld[]));
                if stmt.num_params > 0 {
                    let mut params: Vec<Column> = Vec::with_capacity(stmt.num_params as usize);
                    for _ in 0..stmt.num_params {
                        let pld = try!(self.read_packet());
                        params.push(try!(Column::from_payload(self.last_command, &pld[])));
                    }
                    stmt.params = Some(params);
                    try!(self.read_packet());
                }
                if stmt.num_columns > 0 {
                    let mut columns: Vec<Column> = Vec::with_capacity(stmt.num_columns as usize);
                    for _ in 0..stmt.num_columns {
                        let pld = try!(self.read_packet());
                        columns.push(try!(Column::from_payload(self.last_command, &pld[])));
                    }
                    stmt.columns = Some(columns);
                    try!(self.read_packet());
                }
                Ok(stmt)
            }
        }
    }

    /// Implements binary protocol of mysql server.
    ///
    /// Prepares mysql statement on `MyConn`. [`Stmt`](struct.Stmt.html) will
    /// borrow `MyConn` until the end of its scope.
    pub fn prepare<'a>(&'a mut self, query: &str) -> MyResult<Stmt<'a>> {
        match self._prepare(query) {
            Ok(stmt) => Ok(Stmt::new(stmt, self)),
            Err(err) => Err(err)
        }
    }

    fn more_results_exists(&self) -> bool {
        self.has_results
    }

    fn connect(&mut self) -> MyResult<()> {
        if self.connected {
            return Ok(());
        }
        self.do_handshake().and_then(|_| {
            Ok(from_value_opt::<usize>(&self.get_system_var("max_allowed_packet").unwrap_or(NULL))
               .unwrap_or(0))
        }).and_then(|max_allowed_packet| {
            if max_allowed_packet == 0 {
                Err(MyDriverError(SetupError))
            } else {
                self.max_allowed_packet = max_allowed_packet;
                self.connected = true;
                Ok(())
            }
        })
    }

    fn get_system_var(&mut self, name: &str) -> Option<Value> {
        for row in &mut self.query(&format!("SELECT @@{};", name)[]) {
            match row {
                Ok(mut r) => match r.len() {
                    0 => (),
                    _ => return Some(r.remove(0)),
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
            let p = try!(EOFPacket::from_payload(&pld[]));
            self.handle_eof(&p);
            return Ok(None);
        }
        let res = Value::from_bin_payload(&pld[], &columns[]);
        match res {
            Ok(p) => Ok(Some(p)),
            Err(e) => {
                self.has_results = false;
                Err(MyIoError(e))
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
                let p = try!(EOFPacket::from_payload(&pld[]));
                self.handle_eof(&p);
                return Ok(None);
            } else /* x == 0xff */ {
                let p = ErrPacket::from_payload(&pld[]);
                match p {
                    Ok(p) => return Err(MySqlError(p)),
                    Err(err) => return Err(MyIoError(err))
                }
            }
        }
        let res = Value::from_payload(&pld[], col_count);
        match res {
            Ok(p) => Ok(Some(p)),
            Err(err) => {
                self.has_results = false;
                Err(MyIoError(err))
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

/// Mysql result set for text and binary protocols.
pub struct QueryResult<'a> {
    pooled_conn: Option<pool::MyPooledConn>,
    conn: Option<&'a mut MyConn>,
    columns: Vec<Column>,
    ok_packet: Option<OkPacket>,
    is_bin: bool,
}

impl<'a> QueryResult<'a> {
    fn new(conn: &'a mut MyConn,
           columns: Vec<Column>,
           ok_packet: Option<OkPacket>,
           is_bin: bool) -> QueryResult<'a> {
        QueryResult{pooled_conn: None,
                    columns: columns,
                    conn: Some(conn),
                    ok_packet: ok_packet,
                    is_bin: is_bin}
    }

    fn new_pooled(conn: pool::MyPooledConn,
                      columns: Vec<Column>,
                      ok_packet: Option<OkPacket>,
                      is_bin: bool) -> QueryResult<'a> {
        QueryResult{pooled_conn: Some(conn),
                    columns: columns,
                    conn: None,
                    ok_packet: ok_packet,
                    is_bin: is_bin}
    }

    fn handle_if_more_results(&mut self) -> Option<MyResult<Vec<Value>>> {
        if self.conn.is_some() {
            let conn_ref = self.conn.as_mut().unwrap();
            if conn_ref.status_flags.contains(consts::SERVER_MORE_RESULTS_EXISTS) {
                match conn_ref.handle_result_set() {
                    Ok((cols, ok_p)) => {
                        self.columns = cols;
                        self.ok_packet = ok_p;
                        None
                    },
                    Err(e) => return Some(Err(e))
                }
            } else {
                None
            }
        } else {
            let conn_ref =
                self.pooled_conn.as_mut().unwrap().as_mut();
            if conn_ref.status_flags.contains(consts::SERVER_MORE_RESULTS_EXISTS) {
                match conn_ref.handle_result_set() {
                    Ok((cols, ok_p)) => {
                        self.columns = cols;
                        self.ok_packet = ok_p;
                        None
                    },
                    Err(e) => return Some(Err(e))
                }
            } else {
                None
            }
        }
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// affected rows.
    pub fn affected_rows(&self) -> u64 {
        if self.conn.is_some() {
            self.conn.as_ref().unwrap().affected_rows
        } else {
            self.pooled_conn.as_ref().unwrap().as_ref().affected_rows
        }
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// last insert id.
    pub fn last_insert_id(&self) -> u64 {
        if self.conn.is_some() {
            self.conn.as_ref().unwrap().last_insert_id
        } else {
            self.pooled_conn.as_ref().unwrap().as_ref().last_insert_id
        }
    }

    /// Returns
    /// [`OkPacket`'s](http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html)
    /// warnings count.
    pub fn warnings(&self) -> u16 {
        if self.ok_packet.is_some() {
            self.ok_packet.as_ref().unwrap().warnings
        } else {
            0u16
        }
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
    pub fn column_index<T:BytesContainer>(&self, name: T) -> Option<usize> {
        let name = name.container_as_bytes();
        for (i, c) in self.columns.iter().enumerate() {
            if &c.name[] == name {
                return Some(i)
            }
        }
        None
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
    ///     for x in result {
    ///         // On first iteration of `while` you will get result set from
    ///         // SELECT 1 and from SELECT 2 on second.
    ///     }
    /// }
    /// ```
    pub fn more_results_exists(&self) -> bool {
        if self.conn.is_some() {
            self.conn.as_ref().unwrap().has_results
        } else {
            self.pooled_conn.as_ref().unwrap().as_ref().has_results
        }
    }
}

impl<'a> Iterator for QueryResult<'a> {
    type Item = MyResult<Vec<Value>>;

    fn next(&mut self) -> Option<MyResult<Vec<Value>>> {
        let r = if self.is_bin {
            if self.conn.is_some() {
                let conn_ref = self.conn.as_mut().unwrap();
                conn_ref.next_bin(&self.columns)
            } else {
                let conn_ref = self.pooled_conn.as_mut().unwrap().as_mut();
                conn_ref.next_bin(&self.columns)
            }
        } else {
            if self.conn.is_some() {
                let conn_ref = self.conn.as_mut().unwrap();
                conn_ref.next_text(self.columns.len())
            } else {
                let conn_ref = self.pooled_conn.as_mut().unwrap().as_mut();
                conn_ref.next_text(self.columns.len())
            }
        };
        match r {
            Ok(r) => {
                match r {
                    None => self.handle_if_more_results(),
                    Some(r) => Some(Ok(r))
                }
            },
            Err(e) => Some(Err(e))
        }
    }
}

#[unsafe_destructor]
impl<'a> Drop for QueryResult<'a> {
    fn drop(&mut self) {
        if self.conn.is_some() {
            while self.conn.as_ref().unwrap().more_results_exists() {
                while let Some(_) = self.next() {}
            }
        } else {
            while self.pooled_conn.as_ref().unwrap().as_ref().more_results_exists() {
                while let Some(_) = self.next() {}
            }
        }
    }
}

impl<'a> Iterator for &'a mut MyResult<QueryResult<'a>> {
    type Item = MyResult<Vec<Value>>;

    fn next(&mut self) -> Option<MyResult<Vec<Value>>> {
        match self.as_mut() {
            Ok( result ) => result.next(),
            Err( _ ) => None
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
    use std::default::Default;
    use super::MyOpts;

    static USER: &'static str = "root";
    static PASS: &'static str = "password";
    static ADDR: &'static str = "127.0.0.1";
    static PORT: u16          = 3307;

    #[cfg(feature = "openssl")]
    pub fn get_opts() -> MyOpts {
        MyOpts {
            user: Some(USER.to_string()),
            pass: Some(PASS.to_string()),
            tcp_addr: Some(ADDR.to_string()),
            tcp_port: PORT,
            ssl_opts: Some((Path::new("tests/ca-cert.pem"), None)),
            ..Default::default()
        }
    }

    #[cfg(not(feature = "ssl"))]
    pub fn get_opts() -> MyOpts {
        MyOpts {
            user: Some(USER.to_string()),
            pass: Some(PASS.to_string()),
            tcp_addr: Some(ADDR.to_string()),
            tcp_port: PORT,
            ..Default::default()
        }
    }

    mod my_conn {
        use test;
        use std::iter;
        use std::{str};
        use std::os::{getcwd};
        use std::old_io::fs::{File, unlink};
        use time::{Tm, now};
        use super::super::{MyConn, MyOpts};
        use super::super::super::value::{ToValue, from_value};
        use super::super::super::value::Value::{NULL, Int, Bytes, Date};
        use super::get_opts;

        #[test]
        fn should_connect() {
            let mut conn = MyConn::new(get_opts()).unwrap();
            assert!(conn.ping());
        }
        #[test]
        fn should_connect_with_database() {
            let mut conn = MyConn::new(MyOpts {
                db_name: Some("mysql".to_string()),
                ..get_opts()
            }).unwrap();
            assert_eq!(conn.query("SELECT DATABASE()").unwrap().next(),
                       Some(Ok(vec![Bytes(b"mysql".to_vec())])));
        }
        #[test]
        fn should_execute_queryes_and_parse_results() {
            let mut conn = MyConn::new(get_opts()).unwrap();
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
            assert_eq!(conn.query("SELECT * FROM x.tbl \
                                   WHERE a = 'bar'").unwrap().next(), None);
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
            let mut conn = MyConn::new(get_opts()).unwrap();
            assert_eq!(
                conn.query("SELECT REPEAT('A', 20000000)").unwrap().next(),
                Some(Ok(
                    vec![Bytes(iter::repeat(b'A').take(20_000_000).collect())]
                ))
            );
        }
        #[test]
        fn should_execute_statements_and_parse_results() {
            let mut conn = MyConn::new(get_opts()).unwrap();
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
                assert!(stmt.execute(&[
                    &b"hello".to_vec(),
                    &-123,
                    &123,
                    &(tm.to_timespec()),
                    &123.123f64
                ]).is_ok());
                assert!(stmt.execute(&[
                    &b"world".to_vec(),
                    &NULL,
                    &NULL,
                    &NULL,
                    &321.321f64
                ]).is_ok());
                Ok(())
            }).unwrap();
            let _ = conn.prepare("SELECT * from x.tbl").and_then(|mut stmt| {
                for (i, row) in stmt.execute(&[]).unwrap().enumerate() {
                    let row = row.unwrap();
                    if i == 0 {
                        assert_eq!(row[0], Bytes(b"hello".to_vec()));
                        assert_eq!(row[1], Int(-123i64));
                        assert_eq!(row[2], Int(123i64));
                        assert_eq!(row[3], Date(2014u16, 5u8, 5u8, 0u8, 0u8, 0u8, 0u32));
                        assert_eq!(from_value::<f64>(&row[4]), 123.123);
                    } else if i == 1 {
                        assert_eq!(row[0], Bytes(b"world".to_vec()));
                        assert_eq!(row[1], NULL);
                        assert_eq!(row[2], NULL);
                        assert_eq!(row[3], NULL);
                        assert_eq!(from_value::<f64>(&row[4]), 321.321);
                    } else {
                        unreachable!();
                    }
                }
                Ok(())
            }).unwrap();
        }
        #[test]
        fn should_parse_large_binary_result() {
            let mut conn = MyConn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT REPEAT('A', 20000000);").unwrap();
            assert_eq!(
                stmt.execute(&[]).unwrap().next(),
                Some(Ok(
                     vec![Bytes(iter::repeat(b'A').take(20_000_000).collect())]
                ))
            );
        }
        #[test]
        fn should_start_commit_and_rollback_transactions() {
            let mut conn = MyConn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE x.tbl(a INT)").is_ok());
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                assert!(t.commit().is_ok());
                Ok(())
            }).unwrap();
            assert_eq!(conn.query("SELECT COUNT(a) from x.tbl").unwrap().next(),
                       Some(Ok(vec![Bytes(b"2".to_vec())])));
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO tbl(a) VALUES(1)").is_err());
                Ok(())
                // implicit rollback
            }).unwrap();
            assert_eq!(conn.query("SELECT COUNT(a) from x.tbl").unwrap().next(),
                       Some(Ok(vec![Bytes(b"2".to_vec())])));
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(1)").is_ok());
                assert!(t.query("INSERT INTO x.tbl(a) VALUES(2)").is_ok());
                assert!(t.rollback().is_ok());
                Ok(())
            }).unwrap();
            assert_eq!(conn.query("SELECT COUNT(a) from x.tbl").unwrap().next(),
                       Some(Ok(vec![Bytes(b"2".to_vec())])));
            let _ = conn.start_transaction(false, None, None).and_then(|mut t| {
                let _ = t.prepare("INSERT INTO x.tbl(a) VALUES(?)")
                .and_then(|mut stmt| {
                    assert!(stmt.execute(&[&3]).is_ok());
                    assert!(stmt.execute(&[&4]).is_ok());
                    Ok(())
                }).unwrap();
                assert!(t.commit().is_ok());
                Ok(())
            }).unwrap();
            assert_eq!(conn.query("SELECT COUNT(a) from x.tbl").unwrap().next(),
                       Some(Ok(vec![Bytes(b"4".to_vec())])));
        }
        #[test]
        fn should_handle_LOCAL_INFILE() {
            let mut conn = MyConn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE x.tbl(a TEXT)").is_ok());
            let mut path = getcwd().unwrap();
            path.push("local_infile.txt".to_string());
            {
                let mut file = File::create(&path).unwrap();
                let _ = file.write_line("AAAAAA");
                let _ = file.write_line("BBBBBB");
                let _ = file.write_line("CCCCCC");
            }
            let query = format!("LOAD DATA LOCAL INFILE '{}' INTO TABLE x.tbl",
                                str::from_utf8(path.as_vec()).unwrap());
            assert!(conn.query(&query[]).is_ok());
            for (i, row) in conn.query("SELECT * FROM x.tbl")
                                .unwrap().enumerate() {
                let row = row.unwrap();
                match i {
                    0 => assert_eq!(row, vec!(Bytes(b"AAAAAA".to_vec()))),
                    1 => assert_eq!(row, vec!(Bytes(b"BBBBBB".to_vec()))),
                    2 => assert_eq!(row, vec!(Bytes(b"CCCCCC".to_vec()))),
                    _ => unreachable!()
                }
            }
            let _ = unlink(&path);
        }
        #[test]
        fn should_reset_connection() {
            let mut conn = MyConn::new(get_opts()).unwrap();
            assert!(conn.query("CREATE TEMPORARY TABLE `db`.`test` \
                                (`test` VARCHAR(255) NULL);").is_ok());
            assert!(conn.query("SELECT * FROM `db`.`test`;").is_ok());
            assert!(conn.reset().is_ok());
            assert!(conn.query("SELECT * FROM `db`.`test`;").is_err());
        }
        #[test]
        fn should_handle_multi_resultset() {
            let mut conn = MyConn::new(MyOpts {
                prefer_socket: false,
                db_name: Some("mysql".to_string()),
                ..get_opts()
            }).unwrap();
            assert!(conn.query("DROP PROCEDURE IF EXISTS multi").is_ok());
            assert!(conn.query(r#"CREATE PROCEDURE multi() BEGIN
                                      SELECT 1;
                                      SELECT 1;
                                  END"#).is_ok());
            for (i, row) in conn.query("CALL multi()")
                                .unwrap().enumerate() {
                match i {
                    0 | 1 => assert_eq!(row, Ok(vec![Bytes(b"1".to_vec())])),
                    _ => unreachable!(),
                }
            }
            let mut result = conn.query("SELECT 1; SELECT 2; SELECT 3;").unwrap();
            let mut i = 0;
            while { i += 1; result.more_results_exists() } {
                for row in result.by_ref() {
                    match i {
                        1 => assert_eq!(row, Ok(vec![Bytes(b"1".to_vec())])),
                        2 => assert_eq!(row, Ok(vec![Bytes(b"2".to_vec())])),
                        3 => assert_eq!(row, Ok(vec![Bytes(b"3".to_vec())])),
                        _ => unreachable!(),
                    }
                }
            }
        }

        #[bench]
        fn simple_exec(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            bencher.iter(|| { let _ = conn.query("DO 1"); })
        }
        #[bench]
        fn prepared_exec(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("DO 1").unwrap();
            bencher.iter(|| { let _ = stmt.execute(&[]); })
        }
        #[bench]
        fn simple_query_row(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            bencher.iter(|| { let _ = conn.query("SELECT 1"); })
        }
        #[bench]
        fn simple_prepared_query_row(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT 1").unwrap();
            bencher.iter(|| { let _ = stmt.execute(&[]); })
        }
        #[bench]
        fn simple_prepared_query_row_with_param(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?").unwrap();
            bencher.iter(|| { let _ = stmt.execute(&[&0]); })
        }
        #[bench]
        fn simple_prepared_query_row_with_5_params(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
            let params: &[&ToValue] = &[
                &42i8,
                &b"123456".to_vec(),
                &1.618f64,
                &NULL,
                &1i8
            ];
            bencher.iter(|| { let _ = stmt.execute(params); })
        }
        #[bench]
        fn select_large_string(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            bencher.iter(|| { let _ = conn.query("SELECT REPEAT('A', 10000)"); })
        }
        #[bench]
        fn select_prepared_large_string(bencher: &mut test::Bencher) {
            let mut conn = MyConn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT REPEAT('A', 10000)").unwrap();
            bencher.iter(|| { let _ = stmt.execute(&[]); })
        }
    }
}

