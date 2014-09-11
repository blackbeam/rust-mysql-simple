use std::{uint};
use std::default::{Default};
use std::io::{Reader, File, IoResult, Seek, Stream,
              SeekCur, EndOfFile, BufReader, MemWriter};
use std::io::net::ip::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::io::net::tcp::{TcpStream};
use std::io::net::unix::{UnixStream};
use std::from_str::FromStr;
use std::num::{FromPrimitive};
use std::path::{BytesContainer};
use super::consts;
use super::io::{MyReader, MyWriter};
use super::error::{MyIoError, MySqlError, MyDriverError, CouldNotConnect,
                   UnsupportedProtocol, PacketOutOfSync, PacketTooLarge,
                   Protocol41NotSet, UnexpectedPacket, MismatchedStmtParams,
                   SetupError, MyResult};
use super::scramble::{scramble};
use super::packet::{OkPacket, EOFPacket, ErrPacket, HandshakePacket};
use super::value::{Value, NULL, Int, UInt, Float, Bytes, Date, Time, ToValue};

pub mod pool;

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
#[deriving(Eq, PartialEq)]
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
        let mut reader = BufReader::new(pld);
        try!(reader.seek(1, SeekCur));
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
    fn new<'a>(stmt: InnerStmt, conn: &'a mut MyConn) -> Stmt<'a> {
        Stmt{stmt: stmt, conn: Some(conn), pooled_conn: None}
    }

    fn new_pooled(stmt: InnerStmt, pooled_conn: pool::MyPooledConn) -> Stmt<'a> {
        Stmt{stmt: stmt, conn: None, pooled_conn: Some(pooled_conn)}
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s params if any.
    pub fn params_ref(&self) -> Option<&[Column]> {
        match self.stmt.params {
            Some(ref params) => Some(params.as_slice()),
            None => None
        }
    }

    /// Returns a slice of a [`Column`s](struct.Column.html) which represents
    /// `Stmt`'s columns if any.
    pub fn columns_ref(&self) -> Option<&[Column]> {
        match self.stmt.columns {
            Some(ref columns) => Some(columns.as_slice()),
            None => None
        }
    }

    /// Returns index of a `Stmt`'s column by name.
    pub fn column_index<T>(&self, name: T) -> Option<uint>
    where T: BytesContainer {
        match self.stmt.columns {
            None => None,
            Some(ref columns) => {
                let name = name.container_as_bytes();
                for (i, c) in columns.iter().enumerate() {
                    if c.name.as_slice() == name {
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
    pub fn execute<'a>(&'a mut self, params: &[&ToValue]) -> MyResult<QueryResult<'a>> {
        if self.conn.is_some() {
            let conn_ref: &'a mut &mut MyConn = self.conn.as_mut().unwrap();
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
            let _ = conn.write_command_data(consts::COM_STMT_CLOSE, data);
        } else {
            let conn = self.pooled_conn.as_mut().unwrap().as_mut();
            let _ = conn.write_command_data(consts::COM_STMT_CLOSE, data);
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
#[deriving(Clone, Eq, PartialEq)]
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
    pub flags: u16,
    /// Column type.
    pub column_type: consts::ColumnType,
    /// Max shown decimal digits
    pub decimals: u8
}

impl Column {
    #[inline]
    fn from_payload(command: u8, pld: &[u8]) -> IoResult<Column> {
        let mut reader = BufReader::new(pld);
        // Skip catalog
        let _ = try!(reader.read_lenenc_bytes());
        let schema = try!(reader.read_lenenc_bytes());
        let table = try!(reader.read_lenenc_bytes());
        let org_table = try!(reader.read_lenenc_bytes());
        let name = try!(reader.read_lenenc_bytes());
        let org_name = try!(reader.read_lenenc_bytes());
        try!(reader.skip_lenenc_int());
        let character_set = try!(reader.read_le_u16());
        let column_length = try!(reader.read_le_u32());
        let column_type = try!(reader.read_u8());
        let flags = try!(reader.read_le_u16());
        let decimals = try!(reader.read_u8());
        // skip filler
        try!(reader.seek(2, SeekCur));
        let mut default_values = Vec::with_capacity(0);
        if command == consts::COM_FIELD_LIST as u8 {
            let len = try!(reader.read_lenenc_int());
            default_values = try!(reader.read_exact(len as uint));
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
#[deriving(Clone, Eq, PartialEq)]
pub struct MyOpts {
    /// TCP address of mysql server (defaults to `127.0.0.1`).
    pub tcp_addr: Option<String>,
    /// TCP port of mysql server (defaults to `3306`).
    pub tcp_port: u16,
    /// UNIX address of mysql server (defaults to `None`).
    pub unix_addr: Option<Path>,
    /// User (defaults to `None`).
    pub user: Option<String>,
    /// Password (defaults to `None`).
    pub pass: Option<String>,
    /// Database name (defaults to `None`).
    pub db_name: Option<String>,
    /// Prefer socet connection (defaults to `true`).
    ///
    /// Will reconnect via socket after TCP connection to `127.0.0.1` if `true`.
    pub prefer_socket: bool,
    /// TCP keepalive timeout in seconds (defaults to one hour).
    pub keepalive_timeout: Option<uint>,
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

impl Default for MyOpts {
    fn default() -> MyOpts {
        MyOpts{tcp_addr: Some("127.0.0.1".to_string()),
               tcp_port: 3306,
               unix_addr: None,
               user: None,
               pass: None,
               db_name: None,
               prefer_socket: true,
               keepalive_timeout: Some(60 * 60)}
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
    tcp_stream: Option<TcpStream>,
    unix_stream: Option<UnixStream>,
    affected_rows: u64,
    last_insert_id: u64,
    max_allowed_packet: uint,
    capability_flags: u32,
    connection_id: u32,
    status_flags: u16,
    seq_id: u8,
    character_set: u8,
    last_command: u8,
    connected: bool,
    has_results: bool
}

impl Default for MyConn {
    fn default() -> MyConn {
        MyConn{tcp_stream: None,
                    unix_stream: None,
                    seq_id: 0u8,
                    capability_flags: 0,
                    status_flags: 0u16,
                    connection_id: 0u32,
                    character_set: 0u8,
                    affected_rows: 0u64,
                    last_insert_id: 0u64,
                    last_command: 0u8,
                    max_allowed_packet: consts::MAX_PAYLOAD_LEN,
                    opts: Default::default(),
                    connected: false,
                    has_results: false}
    }
}

impl MyConn {
    /// Creates new `MyConn`.
    pub fn new(opts: MyOpts) -> MyResult<MyConn> {
        let mut conn = MyConn{opts: opts, ..Default::default()};
        try!(conn.connect_stream());
        try!(conn.connect());
        if conn.opts.unix_addr.is_none() && conn.opts.prefer_socket {
            let addr: Option<IpAddr> = FromStr::from_str(
                conn.opts.tcp_addr.as_ref().unwrap().as_slice());
            if addr == Some(Ipv4Addr(127, 0, 0, 1)) ||
               addr == Some(Ipv6Addr(0, 0, 0, 0, 0, 0, 0, 1)) {
                match conn.get_system_var("socket") {
                    Some(path) => {
                        let opts = MyOpts{unix_addr: Some(Path::new(path.unwrap_bytes())),
                                          ..conn.opts.clone()};
                        return MyConn::new(opts).or(Ok(conn));
                    },
                    _ => return Ok(conn)
                }
            }
        }
        return Ok(conn);
    }

    /// Resets `MyConn` (drops state then reconnects).
    pub fn reset(&mut self) -> MyResult<()> {
        self.tcp_stream = None;
        self.unix_stream = None;
        self.seq_id = 0;
        self.capability_flags = 0;
        self.status_flags = 0;
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

    fn get_mut_stream<'a>(&'a mut self) -> &'a mut Stream {
        if self.unix_stream.is_some() {
            self.unix_stream.as_mut().unwrap() as &mut Stream
        } else {
            self.tcp_stream.as_mut().unwrap() as &mut Stream
        }
    }

    fn connect_stream(&mut self) -> MyResult<()> {
        if self.opts.unix_addr.is_some() {
            match UnixStream::connect(self.opts.unix_addr.as_ref().unwrap()) {
                Ok(stream) => {
                    self.unix_stream = Some(stream);
                    return Ok(());
                },
                _ => {
                    let path_str = format!("{:?}",
                                           *self.opts.unix_addr.as_ref().unwrap()
                                   ).to_string();
                    return Err(MyDriverError(CouldNotConnect(Some(path_str))));
                }
            }
        }
        if self.opts.tcp_addr.is_some() {
            match TcpStream::connect(
                self.opts.tcp_addr.as_ref().unwrap().as_slice(),                     
                self.opts.tcp_port)
            {
                Ok(mut stream) => {
                    // keepalive one hour
                    let keepalive_timeout = self.opts.keepalive_timeout.clone();
                    try_io!(stream.set_keepalive(keepalive_timeout));
                    self.tcp_stream = Some(stream);
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
        let mut output = Vec::new();
        let mut pos = 0;
        loop {
            let payload_len = try_io!(self.get_mut_stream().read_le_uint_n(3)) as uint;
            let seq_id = try_io!(self.get_mut_stream().read_u8());
            if seq_id != self.seq_id {
                return Err(MyDriverError(PacketOutOfSync));
            }
            self.seq_id += 1;
            if payload_len == consts::MAX_PAYLOAD_LEN {
                output.reserve(pos + consts::MAX_PAYLOAD_LEN);
                unsafe { output.set_len(pos + consts::MAX_PAYLOAD_LEN); }
                try_io!(self.get_mut_stream().read_at_least(consts::MAX_PAYLOAD_LEN,
                                                            output.mut_slice_from(pos)));
                pos += consts::MAX_PAYLOAD_LEN;
            } else if payload_len == 0 {
                break;
            } else {
                output.reserve(pos + payload_len);
                unsafe { output.set_len(pos + payload_len); }
                try_io!(self.get_mut_stream().read_at_least(payload_len,
                                                            output.mut_slice_from(pos)));
                break;
            }
        }
        Ok(output)
    }

    fn write_packet(&mut self, data: &Vec<u8>) -> MyResult<()> {
        if data.len() > self.max_allowed_packet &&
           self.max_allowed_packet < consts::MAX_PAYLOAD_LEN {
            return Err(MyDriverError(PacketTooLarge));
        }
        if data.len() == 0 {
            let seq_id = self.seq_id;
            try_io!(self.get_mut_stream().write([0u8, 0u8, 0u8, seq_id]));
            self.seq_id += 1;
            return Ok(());
        }
        let mut last_was_max = false;
        for chunk in data.as_slice().chunks(consts::MAX_PAYLOAD_LEN) {
            let chunk_len = chunk.len();
            let mut writer = MemWriter::with_capacity(4 + chunk_len);
            last_was_max = chunk_len == consts::MAX_PAYLOAD_LEN;
            try_io!(writer.write_le_uint_n(chunk_len as u64, 3));
            try_io!(writer.write_u8(self.seq_id));
            self.seq_id += 1;
            try_io!(writer.write(chunk));
            try_io!(self.get_mut_stream().write(writer.unwrap().as_slice()));
        }
        if last_was_max {
            let seq_id = self.seq_id;
            try_io!(self.get_mut_stream().write([0u8, 0u8, 0u8, seq_id]));
            self.seq_id += 1;
        }
        Ok(())
    }

    fn handle_handshake(&mut self, hp: &HandshakePacket) {
        self.capability_flags = hp.capability_flags;
        self.status_flags = hp.status_flags;
        self.connection_id = hp.connection_id;
        self.character_set = hp.character_set;
    }

    fn handle_ok(&mut self, op: &OkPacket) {
        self.affected_rows = op.affected_rows;
        self.last_insert_id = op.last_insert_id;
        self.status_flags = op.status_flags;
    }

    fn handle_eof(&mut self, eof: &EOFPacket) {
        self.status_flags = eof.status_flags;
    }

    fn do_handshake(&mut self) -> MyResult<()> {
        self.read_packet().and_then(|pld| {
            let handshake = try_io!(HandshakePacket::from_payload(pld.as_slice()));
            if handshake.protocol_version != 10u8 {
                return Err(MyDriverError(UnsupportedProtocol(handshake.protocol_version)));
            }
            if (handshake.capability_flags & consts::CLIENT_PROTOCOL_41 as u32) == 0 {
                return Err(MyDriverError(Protocol41NotSet));
            }
            self.handle_handshake(&handshake);
            self.do_handshake_response(&handshake)
        }).and_then(|_| {
            self.read_packet()
        }).and_then(|pld| {
            match pld[0] {
                0u8 => {
                    let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
                    self.handle_ok(&ok);
                    Ok(())
                },
                0xffu8 => {
                    let err = try_io!(ErrPacket::from_payload(pld.as_slice()));
                    Err(MySqlError(err))
                },
                _ => Err(MyDriverError(UnexpectedPacket))
            }
        })
    }

    fn do_handshake_response(&mut self, hp: &HandshakePacket) -> MyResult<()> {
        let mut client_flags = consts::CLIENT_PROTOCOL_41 as u32 |
                               consts::CLIENT_SECURE_CONNECTION as u32 |
                               consts::CLIENT_LONG_PASSWORD as u32 |
                               consts::CLIENT_TRANSACTIONS as u32 |
                               consts::CLIENT_LOCAL_FILES as u32 |
                               consts::CLIENT_MULTI_STATEMENTS as u32 |
                               consts::CLIENT_MULTI_RESULTS as u32 |
                               consts::CLIENT_PS_MULTI_RESULTS as u32 |
                               (self.capability_flags &
                                consts::CLIENT_LONG_FLAG as u32);
        let scramble_buf = scramble(hp.auth_plugin_data.as_slice(),
                                    self.opts.get_pass().as_bytes());
        let scramble_buf_len = if scramble_buf.is_some() { 20 } else { 0 };
        let mut payload_len = 4 + 4 + 1 + 23 + self.opts.get_user().len() + 1 + 1 + scramble_buf_len;
        if self.opts.get_db_name().len() > 0 {
            client_flags |= consts::CLIENT_CONNECT_WITH_DB as u32;
            payload_len += self.opts.get_db_name().len() + 1;
        }
        let mut writer = MemWriter::with_capacity(payload_len);
        try_io!(writer.write_le_u32(client_flags));
        try_io!(writer.write([0u8, ..4]));
        try_io!(writer.write_u8(consts::UTF8_GENERAL_CI));
        try_io!(writer.write([0u8, ..23]));
        try_io!(writer.write_str(self.opts.get_user().as_slice()));
        try_io!(writer.write_u8(0u8));
        try_io!(writer.write_u8(scramble_buf_len as u8));
        if scramble_buf.is_some() {
            try_io!(writer.write(scramble_buf.unwrap().as_slice()));
        }
        if self.opts.get_db_name().len() > 0 {
            try_io!(writer.write_str(self.opts.get_db_name().as_slice()));
            try_io!(writer.write_u8(0u8));
        }
        self.write_packet(&writer.unwrap())
    }

    fn write_command(&mut self, cmd: consts::Command) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd as u8;
        self.write_packet(&vec!(cmd as u8))
    }

    fn write_command_data(&mut self, cmd: consts::Command, buf: &[u8]) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd as u8;
        self.write_packet(&vec!(cmd as u8).append(buf))
    }

    /// Executes [`COM_PING`](http://dev.mysql.com/doc/internals/en/com-ping.html)
    /// on `MyConn`. Return `true` on success or `false` on error.
    pub fn ping(&mut self) -> bool {
        match self.write_command(consts::COM_PING) {
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
            match params[id as uint] {
                Bytes(ref x) => {
                    for chunk in x.as_slice().chunks(self.max_allowed_packet - 7) {
                        let chunk_len = chunk.len() + 7;
                        let mut writer = MemWriter::with_capacity(chunk_len);
                        try_io!(writer.write_le_u32(stmt.statement_id));
                        try_io!(writer.write_le_u16(id));
                        try_io!(writer.write(chunk));
                        try!(self.write_command_data(consts::COM_STMT_SEND_LONG_DATA,
                                                     writer.unwrap().as_slice()));
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
        let mut writer: MemWriter;
        if stmt.num_params > 0 {
            let (bitmap, values, large_ids) = try_io!(Value::to_bin_payload(stmt.params.get_ref().as_slice(),
                                                                            params,
                                                                            self.max_allowed_packet));
            if large_ids.is_some() {
                try!(self.send_long_data(stmt, params, large_ids.unwrap()));
            }
            let data_len = 4 + 1 + 4 + bitmap.len() + 1 + params.len() * 2 + values.len();
            writer = MemWriter::with_capacity(data_len);
            try_io!(writer.write_le_u32(stmt.statement_id));
            try_io!(writer.write_u8(0u8));
            try_io!(writer.write_le_u32(1u32));
            try_io!(writer.write(bitmap.as_slice()));
            try_io!(writer.write_u8(1u8));
            for i in range(0, params.len()) {
                match params[i] {
                    NULL => try_io!(writer.write([stmt.params.get_ref()[i].column_type as u8, 0u8])),
                    Bytes(..) => try_io!(writer.write([consts::MYSQL_TYPE_VAR_STRING as u8, 0u8])),
                    Int(..) => try_io!(writer.write([consts::MYSQL_TYPE_LONGLONG as u8, 0u8])),
                    UInt(..) => try_io!(writer.write([consts::MYSQL_TYPE_LONGLONG as u8, 128u8])),
                    Float(..) => try_io!(writer.write([consts::MYSQL_TYPE_DOUBLE as u8, 0u8])),
                    Date(..) => try_io!(writer.write([consts::MYSQL_TYPE_DATE as u8, 0u8])),
                    Time(..) => try_io!(writer.write([consts::MYSQL_TYPE_TIME as u8, 0u8]))
                }
            }
            try_io!(writer.write(values.as_slice()));
        } else {
            let data_len = 4 + 1 + 4;
            writer = MemWriter::with_capacity(data_len);
            try_io!(writer.write_le_u32(stmt.statement_id));
            try_io!(writer.write_u8(0u8));
            try_io!(writer.write_le_u32(1u32));
        }
        try!(self.write_command_data(consts::COM_STMT_EXECUTE, writer.unwrap().as_slice()));
        self.handle_result_set()
    }

    fn execute<'a>(&'a mut self, stmt: &InnerStmt, params: &[&ToValue]) -> MyResult<QueryResult<'a>> {
        let _params: Vec<Value> = params.iter().map(|x| x.to_value() ).collect();
        match self._execute(stmt, _params.as_slice()) {
            Ok((columns, ok_packet)) => Ok(QueryResult{pooled_conn: None,
                                                       conn: Some(self),
                                                       columns: columns,
                                                       is_bin: true,
                                                       ok_packet: ok_packet}),
            Err(err) => Err(err)
        }
    }

    fn send_local_infile(&mut self, file_name: &[u8]) -> MyResult<Option<OkPacket>> {
        let path = Path::new(file_name);
        let mut file = try_io!(File::open(&path));
        let mut chunk = Vec::from_elem(self.max_allowed_packet, 0u8);
        let mut r = file.read(chunk.as_mut_slice());
        loop {
            match r {
                Ok(cnt) => {
                    try!(self.write_packet(&Vec::from_slice(chunk.slice_to(cnt))));
                },
                Err(e) => {
                    if e.kind == EndOfFile {
                        break;
                    } else {
                        return Err(MyIoError(e));
                    }
                }
            }
            r = file.read(chunk.as_mut_slice());
        }
        try!(self.write_packet(&Vec::new()));
        let pld = try!(self.read_packet());
        if pld[0] == 0u8 {
            let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
            self.handle_ok(&ok);
            return Ok(Some(ok));
        }
        Ok(None)
    }

    fn handle_result_set(&mut self) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        let pld = try!(self.read_packet());
        match pld[0] {
            0x00 => {
                let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
                self.handle_ok(&ok);
                Ok((Vec::new(), Some(ok)))
            },
            0xfb => {
                let mut reader = BufReader::new(pld.as_slice());
                try_io!(reader.seek(1, SeekCur));
                let file_name = try_io!(reader.read_to_end());
                match self.send_local_infile(file_name.as_slice()) {
                    Ok(x) => Ok((Vec::new(), x)),
                    Err(err) => Err(err)
                }
            },
            0xff => {
                let err = try_io!(ErrPacket::from_payload(pld.as_slice()));
                Err(MySqlError(err))
            },
            _ => {
                let mut reader = BufReader::new(pld.as_slice());
                let column_count = try_io!(reader.read_lenenc_int());
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as uint);
                for _ in range(0, column_count) {
                    let pld = try!(self.read_packet());
                    columns.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
                }
                // skip eof packet
                try!(self.read_packet());
                self.has_results = true;
                Ok((columns, None))
            }
        }
    }

    fn _query(&mut self, query: &str) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        try!(self.write_command_data(consts::COM_QUERY, query.as_bytes()));
        self.handle_result_set()
    }

    /// Implements text protocol of mysql server.
    ///
    /// Executes mysql query on `MyConn`. [`QueryResult`](struct.QueryResult.html)
    /// will borrow `MyConn` until the end of its scope.
    pub fn query<'a>(&'a mut self, query: &str) -> MyResult<QueryResult<'a>> {
        match self._query(query) {
            Ok((columns, ok_packet)) => Ok(QueryResult{pooled_conn: None,
                                                       conn: Some(self),
                                                       columns: columns,
                                                       is_bin: false,
                                                       ok_packet: ok_packet}),
            Err(err) => {
                Err(err)
            }
        }
    }

    fn _prepare(&mut self, query: &str) -> MyResult<InnerStmt> {
        try!(self.write_command_data(consts::COM_STMT_PREPARE, query.as_bytes()));
        let pld = try!(self.read_packet());
        match pld[0] {
            0xff => {
                let err =  try_io!(ErrPacket::from_payload(pld.as_slice()));
                Err(MySqlError(err))
            },
            _ => {
                let mut stmt = try_io!(InnerStmt::from_payload(pld.as_slice()));
                if stmt.num_params > 0 {
                    let mut params: Vec<Column> = Vec::with_capacity(stmt.num_params as uint);
                    for _ in range(0, stmt.num_params) {
                        let pld = try!(self.read_packet());
                        params.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
                    }
                    stmt.params = Some(params);
                    try!(self.read_packet());
                }
                if stmt.num_columns > 0 {
                    let mut columns: Vec<Column> = Vec::with_capacity(stmt.num_columns as uint);
                    for _ in range(0, stmt.num_columns) {
                        let pld = try!(self.read_packet());
                        columns.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
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
            let max_allowed_packet = self.get_system_var("max_allowed_packet")
                                         .unwrap_or(NULL)
                                         .unwrap_bytes_or(Vec::with_capacity(0));
            Ok(uint::parse_bytes(max_allowed_packet.as_slice(), 10).unwrap_or(0))
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
        for row in &mut self.query(format!("SELECT @@{:s};", name).as_slice()) {
            if row.is_ok() {
                let mut row = row.unwrap();
                return row.remove(0);
            } else {
                return None;
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
            let p = try_io!(EOFPacket::from_payload(pld.as_slice()));
            self.handle_eof(&p);
            return Ok(None);
        }
        let res = Value::from_bin_payload(pld.as_slice(), columns.as_slice());
        match res {
            Ok(p) => Ok(Some(p)),
            Err(e) => {
                self.has_results = false;
                Err(MyIoError(e))
            }
        }
    }

    fn next_text(&mut self, col_count: uint) -> MyResult<Option<Vec<Value>>> {
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
                let p = try_io!(EOFPacket::from_payload(pld.as_slice()));
                self.handle_eof(&p);
                return Ok(None);
            } else /* x == 0xff */ {
                let p = ErrPacket::from_payload(pld.as_slice());
                match p {
                    Ok(p) => return Err(MySqlError(p)),
                    Err(err) => return Err(MyIoError(err))
                }
            }
        }
        let res = Value::from_payload(pld.as_slice(), col_count);
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
    fn new<'a>(conn: &'a mut MyConn,
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
            if conn_ref.status_flags &
               consts::SERVER_MORE_RESULTS_EXISTS as u16 > 0 {
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
            if conn_ref.status_flags &
               consts::SERVER_MORE_RESULTS_EXISTS as u16 > 0 {
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
    pub fn column_index<T:BytesContainer>(&self, name: T) -> Option<uint> {
        let name = name.container_as_bytes();
        for (i, c) in self.columns.iter().enumerate() {
            if c.name.as_slice() == name {
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

impl<'a> Iterator<MyResult<Vec<Value>>> for QueryResult<'a> {
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
                for _ in *self {}
            }
        } else {
            while self.pooled_conn.as_ref().unwrap().as_ref().more_results_exists() {
                for _ in *self {}
            }
        }
    }
}

impl<'a> Iterator<MyResult<Vec<Value>>> for &'a mut MyResult<QueryResult<'a>> {
    fn next(&mut self) -> Option<MyResult<Vec<Value>>> {
        if self.is_ok() {
            let result = self.as_mut().unwrap();
            return result.next();
        }
        return None;
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
mod test {
    use test::{Bencher};
    use std::default::{Default};
    use std::{str};
    use std::os::{getcwd};
    use std::io::fs::{File, unlink};
    use super::{MyConn, MyOpts};
    use super::super::value::{NULL, Int, Bytes, Date, ToValue, from_value};
    use time::{Tm, now};

    #[test]
    fn test_connect() {
        let conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                      ..Default::default()});
        assert!(conn.is_ok());
    }

    #[test]
    fn test_connect_with_db() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          db_name: Some("mysql".to_string()),
                                          ..Default::default()}).unwrap();
        for x in &mut conn.query("SELECT DATABASE()") {
            assert_eq!(x.unwrap().shift().unwrap().unwrap_bytes(),
                       Vec::from_slice(b"mysql"));
        }
    }

    #[test]
    fn test_query() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        assert!(conn.ping());
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT, b INT, c INT UNSIGNED, d DATE, e FLOAT)").is_ok());
        assert!(conn.query("INSERT INTO tbl(a, b, c, d, e) VALUES ('hello', -123, 123, '2014-05-05', 123.123)").is_ok());
        assert!(conn.query("INSERT INTO tbl(a, b, c, d, e) VALUES ('world', -321, 321, '2014-06-06', 321.321)").is_ok());
        assert!(conn.query("SELECT * FROM unexisted").is_err());
        assert!(conn.query("SELECT * FROM tbl").is_ok());
        // Drop
        assert!(conn.query("SELECT * FROM tbl").is_ok());
        assert!(conn.query("UPDATE tbl SET a = 'foo';").is_ok());
        assert_eq!(conn.affected_rows, 2);
        for _ in &mut conn.query("SELECT * FROM tbl WHERE a = 'bar'") {
            assert!(false);
        }
        let mut count = 0;
        for row in &mut conn.query("SELECT * FROM tbl") {
            assert!(row.is_ok());
            let row = row.unwrap();
            if count == 0 {
                assert_eq!(row[0], Bytes(Vec::from_slice(b"foo")));
                assert_eq!(row[1], Bytes(Vec::from_slice(b"-123")));
                assert_eq!(row[2], Bytes(Vec::from_slice(b"123")));
                assert_eq!(row[3], Bytes(Vec::from_slice(b"2014-05-05")));
                assert_eq!(row[4], Bytes(Vec::from_slice(b"123.123")));
            } else {
                assert_eq!(row[0], Bytes(Vec::from_slice(b"foo")));
                assert_eq!(row[1], Bytes(Vec::from_slice(b"-321")));
                assert_eq!(row[2], Bytes(Vec::from_slice(b"321")));
                assert_eq!(row[3], Bytes(Vec::from_slice(b"2014-06-06")));
                assert_eq!(row[4], Bytes(Vec::from_slice(b"321.321")));
            }
            count += 1;
        }
        assert_eq!(count, 2u);
        for row in &mut conn.query("SELECT REPEAT('A', 20000000)") {
            assert!(row.is_ok());
            let row = row.unwrap();
            let val= row[0].bytes_ref();
            assert_eq!(val.len(), 20000000);
            assert_eq!(val, Vec::from_elem(20000000, 65u8).as_slice());
        }
    }

    #[test]
    fn test_prepared_statemenst() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT, b INT, c INT UNSIGNED, d DATE, e DOUBLE)").is_ok());
        {
            let stmt = conn.prepare("INSERT INTO tbl(a, b, c, d, e) VALUES (?, ?, ?, ?, ?)");
            assert!(stmt.is_ok());
            let mut stmt = stmt.unwrap();
            let t = Tm{tm_year: 2014, tm_mon: 4, tm_mday: 5,
                       tm_hour: 0,    tm_min: 0, tm_sec: 0, tm_nsec: 0, ..now()};
            assert!(stmt.execute(&[&b"hello", &-123i, &123i, &(t.to_timespec()), &123.123f64]).is_ok());
            assert!(stmt.execute(&[&b"world", &NULL, &NULL, &NULL, &321.321f64]).is_ok());
        }
        {
            let stmt = conn.prepare("SELECT * FROM tbl");
            assert!(stmt.is_ok());
            let mut stmt = stmt.unwrap();
            let mut i = 0i;
            for row in &mut stmt.execute([]) {
                assert!(row.is_ok());
                let row = row.unwrap();
                if i == 0 {
                    assert_eq!(row[0], Bytes(Vec::from_slice(b"hello")));
                    assert_eq!(row[1], Int(-123i64));
                    assert_eq!(row[2], Int(123i64));
                    assert_eq!(row[3], Date(2014u16, 5u8, 5u8, 0u8, 0u8, 0u8, 0u32));
                    assert_eq!(row[4].get_float(), 123.123);
                } else {
                    assert_eq!(row[0], Bytes(Vec::from_slice(b"world")));
                    assert_eq!(row[1], NULL);
                    assert_eq!(row[2], NULL);
                    assert_eq!(row[3], NULL);
                    assert_eq!(row[4].get_float(), 321.321);
                }
                i += 1;
            }
        }
        let stmt = conn.prepare("SELECT REPEAT('A', 20000000);");
        let mut stmt = stmt.unwrap();
        for row in &mut stmt.execute([]) {
            assert!(row.is_ok());
            let row = row.unwrap();
            let val= row[0].bytes_ref();
            assert_eq!(val.len(), 20000000);
            assert_eq!(val, Vec::from_elem(20000000, 65u8).as_slice());
        }
    }

    #[test]
    fn test_large_insert() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a LONGBLOB)").is_ok());
        let query = format!("INSERT INTO tbl(a) VALUES('{:s}')", str::from_chars(Vec::from_elem(20000000, 'A').as_slice()));
        assert!(conn.query(query.as_slice()).is_ok());
        let x = (&mut conn.query("SELECT * FROM tbl")).next().unwrap();
        assert!(x.is_ok());
        let v: Vec<u8> = x.unwrap().remove(0).unwrap().unwrap_bytes();
        assert_eq!(v.len(), 20000000);
        assert_eq!(v, Vec::from_elem(20000000, 65u8));
    }

    #[test]
    fn test_large_insert_prepared() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a LONGBLOB)").is_ok());
        {
            let stmt = conn.prepare("INSERT INTO tbl(a) values ( ? );");
            assert!(stmt.is_ok());
            let mut stmt = stmt.unwrap();
            let val = Vec::from_elem(20000000, 65u8);
            assert!(stmt.execute(&[&val]).is_ok());
        }
        let row = (&mut conn.query("SELECT * FROM tbl")).next().unwrap();
        assert!(row.is_ok());
        let row = row.unwrap();
        let val= row[0].bytes_ref();
        assert_eq!(val.len(), 20000000);
        assert_eq!(val, Vec::from_elem(20000000, 65u8).as_slice());
    }

    #[test]
    #[allow(unused_must_use)]
    fn test_local_infile() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT)").is_ok());
        let mut path = getcwd();
        path.push("local_infile.txt".to_string());
        {
            let mut file = File::create(&path).unwrap();
            file.write_line("AAAAAA");
            file.write_line("BBBBBB");
            file.write_line("CCCCCC");
        }
        let query = format!("LOAD DATA LOCAL INFILE '{:s}' INTO TABLE tbl", str::from_utf8(path.clone().into_vec().as_slice()).unwrap());
        assert!(conn.query(query.as_slice()).is_ok());
        let mut count = 0;
        for row in &mut conn.query("SELECT * FROM tbl") {
            assert!(row.is_ok());
            let row = row.unwrap();
            match count {
                0 => assert_eq!(row, vec!(Bytes(Vec::from_slice(b"AAAAAA")))),
                1 => assert_eq!(row, vec!(Bytes(Vec::from_slice(b"BBBBBB")))),
                2 => assert_eq!(row, vec!(Bytes(Vec::from_slice(b"CCCCCC")))),
                _ => assert!(false)
            }
            count += 1;
        }
        assert_eq!(count, 3u);
        unlink(&path);
    }

    #[test]
    fn test_reset() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                               ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.reset().is_ok());
        for row in &mut conn.query("SELECT DATABASE()") {
            assert!(row.is_ok());
            let row = row.unwrap();
            assert_eq!(row, vec!(NULL));
        }
    }

    #[test]
    fn test_multi_resultset() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          prefer_socket: false,
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("DROP PROCEDURE IF EXISTS multi").is_ok());
        assert!(conn.query(r#"CREATE PROCEDURE multi() BEGIN
                                  SELECT 1;
                                  SELECT 1;
                              END"#).is_ok());
        assert!(conn.query("CALL multi();").is_ok());
        {
            let mut result = conn.query("SELECT 1; SELECT 2; SELECT 3;").unwrap();
            let mut i: i64 = 1;
            while result.more_results_exists() {
                for x in result {
                    assert_eq!(i, from_value::<i64>(&x.unwrap()[0]));
                }
                i += 1;
            }

        }
        assert!(conn.query("DROP DATABASE test").is_ok());
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_exec(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { conn.query("DO 1"); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_prepared_exec(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("DO 1").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_query_row(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { (&mut conn.query("SELECT 1")).next(); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_prepared_query_row(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_prepared_query_row_param(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT ?").unwrap();
        let mut i = 0i;
        bench.iter(|| { stmt.execute(&[&i]); i += 1; })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_prepared_query_row_5param(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        let params: &[&ToValue] = &[&42i8, &b"123456", &1.618f64, &NULL, &1i8];
        bench.iter(|| { stmt.execute(params); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_select_large_string(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { for _ in &mut conn.query("SELECT REPEAT('A', 10000)") {} })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_select_prepared_large_string(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT REPEAT('A', 10000)").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }
}
