use std::{uint};
use std::default::{Default};
use std::io::{Reader, File, IoResult, Seek,
              SeekCur, EndOfFile, BufReader, MemWriter};
use std::io::net::ip::{Ipv4Addr, Ipv6Addr};
use std::io::net::tcp::{TcpStream};
use std::io::net::unix::{UnixStream};
use std::from_str::FromStr;
use std::num::{FromPrimitive};
use super::consts;
use super::io::{MyReader, MyWriter};
use super::error::{MyIoError, MySqlError, MyDriverError, CouldNotConnect,
                   UnsupportedProtocol, PacketOutOfSync, PacketTooLarge,
                   Protocol41NotSet, UnexpectedPacket, MismatchedStmtParams,
                   SetupError, MyResult};
use super::scramble::{scramble};
use super::packet::{OkPacket, EOFPacket, ErrPacket, HandshakePacket};
use super::value::{Value, NULL, Int, UInt, Float, Bytes, Date, Time};

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

pub struct Stmt<'a> {
    stmt: InnerStmt,
    conn: Option<&'a mut MyConn>,
    pooled_conn: Option<pool::MyPooledConn>
}

impl<'a> Stmt<'a> {
    fn new<'a>(stmt: InnerStmt, conn: &'a mut MyConn) -> Stmt<'a> {
        Stmt{stmt: stmt, conn: Some(conn), pooled_conn: None}
    }
    fn new_pooled(stmt: InnerStmt, pooled_conn: pool::MyPooledConn) -> Stmt {
        Stmt{stmt: stmt, conn: None, pooled_conn: Some(pooled_conn)}
    }
    pub fn execute<'a>(&'a mut self, params: &[Value]) -> MyResult<QueryResult<'a>> {
        if self.conn.is_some() {
            let conn_ref: &'a mut &mut MyConn = self.conn.get_mut_ref();
            conn_ref.execute(&self.stmt, params)
        } else {
            let conn_ref = self.pooled_conn.get_mut_ref().get_mut_ref();
            conn_ref.execute(&self.stmt, params)
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

#[deriving(Clone)]
pub struct Column {
    pub catalog: Vec<u8>,
    pub schema: Vec<u8>,
    pub table: Vec<u8>,
    pub org_table: Vec<u8>,
    pub name: Vec<u8>,
    pub org_name: Vec<u8>,
    pub default_values: Vec<u8>,
    pub column_length: u32,
    pub character_set: u16,
    pub flags: u16,
    pub column_type: consts::ColumnType,
    pub decimals: u8
}

impl Column {
    #[inline]
    fn from_payload(command: u8, pld: &[u8]) -> IoResult<Column> {
        let mut reader = BufReader::new(pld);
        let catalog = try!(reader.read_lenenc_bytes());
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
        if command == consts::COM_FIELD_LIST {
            let len = try!(reader.read_lenenc_int());
            default_values = try!(reader.read_exact(len as uint));
        }
        Ok(Column{catalog: catalog,
                schema: schema,
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
#[deriving(Clone, PartialEq)]
pub struct MyOpts {
    pub tcp_addr: Option<String>,
    pub tcp_port: u16,
    pub unix_addr: Option<Path>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub db_name: Option<String>,
    pub prefer_socket: bool,
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
    pub fn new(opts: MyOpts) -> MyResult<MyConn> {
        let mut conn = MyConn{opts: opts, ..Default::default()};
        try!(conn.connect_stream());
        try!(conn.connect());
        if conn.opts.unix_addr.is_none() && conn.opts.prefer_socket {
            if FromStr::from_str(conn.opts.tcp_addr.get_ref().as_slice()) == Some(Ipv4Addr(127, 0, 0, 1)) ||
               FromStr::from_str(conn.opts.tcp_addr.get_ref().as_slice()) == Some(Ipv6Addr(0, 0, 0, 0, 0, 0, 0, 1)) {
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
    fn connect_stream(&mut self) -> MyResult<()> {
        if self.opts.unix_addr.is_some() {
            match UnixStream::connect(self.opts.unix_addr.get_ref()) {
                Ok(stream) => {
                    self.unix_stream = Some(stream);
                    return Ok(());
                },
                _ => {
                    let path_str = format!("{:?}", *self.opts.unix_addr.get_ref()).to_string();
                    return Err(MyDriverError(CouldNotConnect(Some(path_str))));
                }
            }
        }
        if self.opts.tcp_addr.is_some() {
            match TcpStream::connect(self.opts.tcp_addr.get_ref().as_slice(),
                                     self.opts.tcp_port) {
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
            let payload_len = try_io!(self.read_le_uint_n(3)) as uint;
            let seq_id = try_io!(self.read_u8());
            if seq_id != self.seq_id {
                return Err(MyDriverError(PacketOutOfSync));
            }
            self.seq_id += 1;
            if payload_len == consts::MAX_PAYLOAD_LEN {
                output.reserve(pos + consts::MAX_PAYLOAD_LEN);
                unsafe { output.set_len(pos + consts::MAX_PAYLOAD_LEN); }
                try_io!(self.read_at_least(consts::MAX_PAYLOAD_LEN,
                                           output.mut_slice_from(pos)));
                pos += consts::MAX_PAYLOAD_LEN;
            } else if payload_len == 0 {
                break;
            } else {
                output.reserve(pos + payload_len);
                unsafe { output.set_len(pos + payload_len); }
                try_io!(self.read_at_least(payload_len,
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
            try_io!(self.write([0u8, 0u8, 0u8, seq_id]));
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
            try_io!(self.write(writer.unwrap().as_slice()));
        }
        if last_was_max {
            let seq_id = self.seq_id;
            try_io!(self.write([0u8, 0u8, 0u8, seq_id]));
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
            if (handshake.capability_flags & consts::CLIENT_PROTOCOL_41) == 0 {
                return Err(MyDriverError(Protocol41NotSet));
            }
            self.handle_handshake(&handshake);
            self.do_handshake_response(&handshake)
        }).and_then(|_| {
            self.read_packet()
        }).and_then(|pld| {
            match *pld.get(0) {
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
        let mut client_flags = consts::CLIENT_PROTOCOL_41 |
                               consts::CLIENT_SECURE_CONNECTION |
                               consts::CLIENT_LONG_PASSWORD |
                               consts::CLIENT_TRANSACTIONS |
                               consts::CLIENT_LOCAL_FILES |
                               (self.capability_flags &
                                consts::CLIENT_LONG_FLAG);
        let scramble_buf = scramble(hp.auth_plugin_data.as_slice(),
                                    self.opts.get_pass().as_bytes());
        let scramble_buf_len = if scramble_buf.is_some() { 20 } else { 0 };
        let mut payload_len = 4 + 4 + 1 + 23 + self.opts.get_user().len() + 1 + 1 + scramble_buf_len;
        if self.opts.get_db_name().len() > 0 {
            client_flags |= consts::CLIENT_CONNECT_WITH_DB;
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
    fn write_command(&mut self, cmd: u8) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd;
        self.write_packet(&vec!(cmd))
    }
    fn write_command_data(&mut self, cmd: u8, buf: &[u8]) -> MyResult<()> {
        self.seq_id = 0u8;
        self.last_command = cmd;
        self.write_packet(&vec!(cmd).append(buf))
    }
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
                    NULL => try_io!(writer.write([stmt.params.get_ref().get(i).column_type as u8, 0u8])),
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
        let pld = try!(self.read_packet());
        match *pld.get(0) {
            0x00 => {
                let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
                self.handle_ok(&ok);
                Ok((Vec::with_capacity(0), Some(ok)))
            },
            0xff => {
                let err = try_io!(ErrPacket::from_payload(pld.as_slice()));
                Err(MySqlError(err))
            }
            _ => {
                let mut reader = BufReader::new(pld.as_slice());
                let column_count = try_io!(reader.read_lenenc_int());
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as uint);
                for _ in range(0, column_count) {
                    let pld = try!(self.read_packet());
                    columns.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
                }
                try!(self.read_packet());
                self.has_results = true;
                Ok((columns, None))
            }
        }
    }
    fn execute<'a>(&'a mut self, stmt: &InnerStmt, params: &[Value]) -> MyResult<QueryResult<'a>> {
        match self._execute(stmt, params) {
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
        try!(self.write_packet(&Vec::with_capacity(0)));
        let pld = try!(self.read_packet());
        if *pld.get(0) == 0u8 {
            let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
            self.handle_ok(&ok);
            return Ok(Some(ok));
        }
        Ok(None)
    }
    fn _query(&mut self, query: &str) -> MyResult<(Vec<Column>, Option<OkPacket>)> {
        try!(self.write_command_data(consts::COM_QUERY, query.as_bytes()));
        let pld = try!(self.read_packet());
        match *pld.get(0) {
            0x00 => {
                let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
                self.handle_ok(&ok);
                return Ok((Vec::with_capacity(0), Some(ok)))
            },
            0xfb => {
                let mut reader = BufReader::new(pld.as_slice());
                try_io!(reader.seek(1, SeekCur));
                let file_name = try_io!(reader.read_to_end());
                match self.send_local_infile(file_name.as_slice()) {
                    Ok(x) => Ok((Vec::with_capacity(0), x)),
                    Err(err) => Err(err)
                }
            }
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
    pub fn query<'a>(&'a mut self, query: &str) -> MyResult<QueryResult<'a>> {
        match self._query(query) {
            Ok((columns, ok_packet)) => Ok(QueryResult{pooled_conn: None,
                                                       conn: Some(self),
                                                       columns: columns,
                                                       is_bin: false,
                                                       ok_packet: ok_packet}),
            Err(err) => Err(err)
        }
    }
    fn _prepare(&mut self, query: &str) -> MyResult<InnerStmt> {
        try!(self.write_command_data(consts::COM_STMT_PREPARE, query.as_bytes()));
        let pld = try!(self.read_packet());
        match *pld.get(0) {
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
    pub fn prepare<'a>(&'a mut self, query: &str) -> MyResult<Stmt<'a>> {
        match self._prepare(query) {
            Ok(stmt) => Ok(Stmt::new(stmt, self)),
            Err(err) => Err(err)
        }
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
                return row.shift();
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
        let x = *pld.get(0);
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
        let x = *pld.get(0);
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

impl Reader for MyConn {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        if self.unix_stream.is_some() {
            self.unix_stream.get_mut_ref().read(buf)
        } else {
            self.tcp_stream.get_mut_ref().read(buf)
        }
    }
}

impl Writer for MyConn {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        if self.unix_stream.is_some() {
            self.unix_stream.get_mut_ref().write(buf)
        } else {
            self.tcp_stream.get_mut_ref().write(buf)
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

pub struct QueryResult<'a> {
    pooled_conn: Option<pool::MyPooledConn>,
    conn: Option<&'a mut MyConn>,
    columns: Vec<Column>,
    ok_packet: Option<OkPacket>,
    is_bin: bool
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
                      is_bin: bool) -> QueryResult {
        QueryResult{pooled_conn: Some(conn),
                    columns: columns,
                    conn: None,
                    ok_packet: ok_packet,
                    is_bin: is_bin}
    }
    pub fn affected_rows(&self) -> u64 {
        if self.conn.is_some() {
            self.conn.get_ref().affected_rows
        } else {
            self.pooled_conn.get_ref().get_ref().affected_rows
        }
    }
    pub fn last_insert_id(&self) -> u64 {
        if self.conn.is_some() {
            self.conn.get_ref().last_insert_id
        } else {
            self.pooled_conn.get_ref().get_ref().last_insert_id
        }
    }
    pub fn warnings(&self) -> u16 {
        if self.ok_packet.is_some() {
            self.ok_packet.get_ref().warnings
        } else {
            0u16
        }
    }
    pub fn info(&self) -> Vec<u8> {
        if self.ok_packet.is_some() {
            self.ok_packet.get_ref().info.clone()
        } else {
            Vec::with_capacity(0)
        }
    }
    pub fn next(&mut self) -> Option<MyResult<Vec<Value>>> {
        if self.is_bin {
            let r = if self.conn.is_some() {
                let conn_ref = self.conn.get_mut_ref();
                conn_ref.next_bin(&self.columns)
            } else {
                let conn_ref = self.pooled_conn.get_mut_ref().get_mut_ref();
                conn_ref.next_bin(&self.columns)
            };
            match r {
                Ok(r) => {
                    match r {
                        None => {
                            return None;
                        },
                        Some(r) => return Some(Ok(r))
                    }
                },
                Err(e) => {
                    return Some(Err(e));
                }
            }
        } else {
            let r = if self.conn.is_some() {
                let conn_ref = self.conn.get_mut_ref();
                conn_ref.next_text(self.columns.len())
            } else {
                let conn_ref = self.pooled_conn.get_mut_ref().get_mut_ref();
                conn_ref.next_text(self.columns.len())
            };
            match r {
                Ok(r) => {
                    match r {
                        None => {
                            return None;
                        },
                        Some(r) => return Some(Ok(r))
                    }
                },
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }
    }
}

#[unsafe_destructor]
impl<'a> Drop for QueryResult<'a> {
    fn drop(&mut self) {
        for _ in *self {}
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
    use std::path::posix::{Path};
    use super::{MyConn, MyOpts};
    use super::super::value::{NULL, Int, UInt, Float, Bytes, Date};

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
                assert_eq!(*row.get(0), Bytes(Vec::from_slice(b"foo")));
                assert_eq!(*row.get(1), Bytes(Vec::from_slice(b"-123")));
                assert_eq!(*row.get(2), Bytes(Vec::from_slice(b"123")));
                assert_eq!(*row.get(3), Bytes(Vec::from_slice(b"2014-05-05")));
                assert_eq!(*row.get(4), Bytes(Vec::from_slice(b"123.123")));
            } else {
                assert_eq!(*row.get(0), Bytes(Vec::from_slice(b"foo")));
                assert_eq!(*row.get(1), Bytes(Vec::from_slice(b"-321")));
                assert_eq!(*row.get(2), Bytes(Vec::from_slice(b"321")));
                assert_eq!(*row.get(3), Bytes(Vec::from_slice(b"2014-06-06")));
                assert_eq!(*row.get(4), Bytes(Vec::from_slice(b"321.321")));
            }
            count += 1;
        }
        assert_eq!(count, 2u);
        for row in &mut conn.query("SELECT REPEAT('A', 20000000)") {
            assert!(row.is_ok());
            let row = row.unwrap();
            let val= row.get(0).bytes_ref();
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
            assert!(stmt.execute([Bytes(Vec::from_slice(b"hello")), Int(-123), UInt(123), Date(2014, 5, 5,0,0,0,0), Float(123.123f64)]).is_ok());
            assert!(stmt.execute([Bytes(Vec::from_slice(b"world")), NULL, NULL, NULL, Float(321.321f64)]).is_ok());
        }
        {
            let stmt = conn.prepare("SELECT * FROM tbl");
            assert!(stmt.is_ok());
            let mut stmt = stmt.unwrap();
            let mut i = 0;
            for row in &mut stmt.execute([]) {
                assert!(row.is_ok());
                let row = row.unwrap();
                if i == 0 {
                    assert_eq!(*row.get(0), Bytes(Vec::from_slice(b"hello")));
                    assert_eq!(*row.get(1), Int(-123i64));
                    assert_eq!(*row.get(2), Int(123i64));
                    assert_eq!(*row.get(3), Date(2014u16, 5u8, 5u8, 0u8, 0u8, 0u8, 0u32));
                    assert_eq!(row.get(4).get_float(), 123.123);
                } else {
                    assert_eq!(*row.get(0), Bytes(Vec::from_slice(b"world")));
                    assert_eq!(*row.get(1), NULL);
                    assert_eq!(*row.get(2), NULL);
                    assert_eq!(*row.get(3), NULL);
                    assert_eq!(row.get(4).get_float(), 321.321);
                }
                i += 1;
            }
        }
        let stmt = conn.prepare("SELECT REPEAT('A', 20000000);");
        let mut stmt = stmt.unwrap();
        for row in &mut stmt.execute([]) {
            assert!(row.is_ok());
            let row = row.unwrap();
            let val= row.get(0).bytes_ref();
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
        let v: Vec<u8> = x.unwrap().shift().unwrap().unwrap_bytes();
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
            assert!(stmt.execute([Bytes(val)]).is_ok());
        }
        let row = (&mut conn.query("SELECT * FROM tbl")).next().unwrap();
        assert!(row.is_ok());
        let row = row.unwrap();
        let val= row.get(0).bytes_ref();
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

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_exec(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { conn.query("DO 1"); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_prepared_exec(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("DO 1").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_query_row(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { (&mut conn.query("SELECT 1")).next(); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_prepared_query_row(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_prepared_query_row_param(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT ?").unwrap();
        let mut i = 0;
        bench.iter(|| { stmt.execute([Int(i)]); i += 1; })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_prepared_query_row_5param(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        let params = [Int(42), Bytes(Vec::from_slice(b"123456")), Float(1.618), NULL, Int(1)];
        bench.iter(|| { stmt.execute(params); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_select_large_string(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { for _ in &mut conn.query("SELECT REPEAT('A', 10000)") {} })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_select_prepared_large_string(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_string()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT REPEAT('A', 10000)").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }
}
