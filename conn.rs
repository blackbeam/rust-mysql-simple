use std::{uint, default};
use std::io::{Stream, Reader, File, IoResult, Seek,
              SeekCur, EndOfFile, BufReader, MemWriter};
use std::io::net::ip::{SocketAddr, Ipv4Addr, Ipv6Addr};
use std::io::net::tcp::{TcpStream};
use std::io::net::unix::{UnixStream};
use super::consts;
use super::io::{MyReader};
use super::error::{MyError, MyIoError, MySqlError, MyStrError};
use super::scramble::{scramble};
use super::packet::{OkPacket, EOFPacket, ErrPacket, HandshakePacket};
use super::value::{Value, NULL, Int, UInt, Float, Bytes, Date, Time};

pub type MyResult<T> = Result<T, MyError>;

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
    conn: &'a mut MyConn
}

impl<'a> Stmt<'a> {
    pub fn execute<'a>(&'a mut self, params: &[Value]) -> MyResult<QueryResult<'a>> {
        self.conn.execute(&self.stmt, params)
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
    pub column_type: u8,
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
        // skip next length
        let _ = reader.read_lenenc_int();
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
                column_type: column_type,
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
#[deriving(Clone, Eq)]
pub struct MyOpts {
    pub tcp_addr: Option<SocketAddr>,
    pub unix_addr: Option<Path>,
    pub user: Option<~str>,
    pub pass: Option<~str>,
    pub db_name: Option<~str>,
    pub prefer_socket: bool,
}

impl MyOpts {
    fn get_user(&self) -> ~str {
        match self.user {
            Some(ref x) => x.clone(),
            None => "".to_owned()
        }
    }
    fn get_pass(&self) -> ~str {
        match self.pass {
            Some(ref x) => x.clone(),
            None => "".to_owned()
        }
    }
    fn get_db_name(&self) -> ~str {
        match self.db_name {
            Some(ref x) => x.clone(),
            None => "".to_owned()
        }
    }
}

impl default::Default for MyOpts {
    fn default() -> MyOpts {
        MyOpts{tcp_addr: Some(SocketAddr{ip: Ipv4Addr(127, 0, 0, 1), port: 3306}),
               unix_addr: None,
               user: None,
               pass: None,
               db_name: None,
               prefer_socket: true}
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
    stream: ~Stream,
    affected_rows: u64,
    last_insert_id: u64,
    max_allowed_packet: uint,
    capability_flags: u32,
    connection_id: u32,
    status_flags: u16,
    seq_id: u8,
    character_set: u8,
    last_command: u8,
    connected: bool
}

impl MyConn {
    pub fn new(opts: MyOpts) -> MyResult<MyConn> {
        if opts.unix_addr.is_some() {
            let unix_stream = UnixStream::connect(opts.unix_addr.get_ref());
            if unix_stream.is_ok() {
                let mut conn = MyConn{
                    stream: ~(unix_stream.unwrap()) as ~Stream,
                    seq_id: 0u8,
                    capability_flags: 0,
                    status_flags: 0u16,
                    connection_id: 0u32,
                    character_set: 0u8,
                    affected_rows: 0u64,
                    last_insert_id: 0u64,
                    last_command: 0u8,
                    max_allowed_packet: consts::MAX_PAYLOAD_LEN,
                    opts: opts,
                    connected: false
                };
                return conn.connect().and(Ok(conn));
            } else {
                return Err(MyStrError(format!("Could not connect to address: {:?}", opts.unix_addr)));
            }
        }
        if opts.tcp_addr.is_some() {
            let tcp_stream = TcpStream::connect(opts.tcp_addr.unwrap());
            if tcp_stream.is_ok() {
                let mut conn = MyConn{
                    stream: ~(tcp_stream.unwrap()) as ~Stream,
                    seq_id: 0u8,
                    capability_flags: 0,
                    status_flags: 0u16,
                    connection_id: 0u32,
                    character_set: 0u8,
                    affected_rows: 0u64,
                    last_insert_id: 0u64,
                    last_command: 0u8,
                    max_allowed_packet: consts::MAX_PAYLOAD_LEN,
                    opts: opts,
                    connected: false
                };
                let res = conn.connect();
                match res {
                    Err(err) => return Err(err),
                    _ => {
                        if conn.opts.prefer_socket &&
                           (conn.opts.tcp_addr.get_ref().ip == Ipv4Addr(127, 0, 0, 1) ||
                            conn.opts.tcp_addr.get_ref().ip == Ipv6Addr(0, 0, 0, 0, 0, 0, 0, 1))
                        {
                            let path = conn.get_system_var("socket");
                            if !path.is_some() {
                                return Ok(conn);
                            } else {
                                let path = path.unwrap().unwrap_bytes();
                                let opts = MyOpts{
                                    unix_addr: Some(Path::new(path)),
                                    ..conn.opts.clone()
                                };
                                return MyConn::new(opts).or(Ok(conn));
                            }
                        } else {
                            return Ok(conn);
                        }
                    }
                }
            } else {
                return Err(MyStrError(format!("Could not connect to address: {:?}", opts.tcp_addr)));
            }
        } else {
            return Err(MyStrError("Could not connect. Address not specified".to_owned()));
        }
    }
    fn read_packet(&mut self) -> MyResult<Vec<u8>> {
        let mut output = Vec::new();
        loop {
            let payload_len = try_io!(self.read_le_uint_n(3));
            let seq_id = try_io!(self.read_u8());
            if seq_id != self.seq_id {
                return Err(MyStrError("Packet out of sync".to_owned()));
            }
            self.seq_id += 1;
            if payload_len as uint >= consts::MAX_PAYLOAD_LEN {
                try_io!(self.push_exact(&mut output, consts::MAX_PAYLOAD_LEN));
            } else if payload_len == 0 {
                break;
            } else {
                try_io!(self.push_exact(&mut output, payload_len as uint));
                break;
            }
        }
        Ok(output)
    }
    fn write_packet(&mut self, data: &Vec<u8>) -> MyResult<()> {
        if data.len() > self.max_allowed_packet && self.max_allowed_packet < consts::MAX_PAYLOAD_LEN {
            return Err(MyStrError("Packet too large".to_owned()));
        }
        if data.len() == 0 {
            try_io!(self.write([0u8, 0u8, 0u8, self.seq_id]));
            self.seq_id += 1;
            return Ok(());
        }
        let mut last_was_max = false;
        for chunk in data.as_slice().chunks(consts::MAX_PAYLOAD_LEN) {
            let chunk_len = chunk.len();
            let full_chunk_len = 4 + chunk_len;
            let mut full_chunk: Vec<u8> = Vec::from_elem(full_chunk_len, 0u8);
            if chunk_len == consts::MAX_PAYLOAD_LEN {
                last_was_max = true;
                *full_chunk.get_mut(0) = 255u8;
                *full_chunk.get_mut(1) = 255u8;
                *full_chunk.get_mut(2) = 255u8;
            } else {
                last_was_max = false;
                *full_chunk.get_mut(0) = (chunk_len & 255) as u8;
                *full_chunk.get_mut(1) = ((chunk_len & (255 << 8)) >> 8) as u8;
                *full_chunk.get_mut(2) = ((chunk_len & (255 << 16)) >> 16) as u8;
            }
            *full_chunk.get_mut(3) = self.seq_id;
            self.seq_id += 1;
            unsafe {
                let payload_slice = full_chunk.mut_slice_from(4);
                payload_slice.copy_memory(chunk);
            }
            try_io!(self.write(full_chunk.as_slice()));
        }
        if last_was_max {
            try_io!(self.write([0u8, 0u8, 0u8, self.seq_id]));
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
                return Err(MyStrError(format!("Unsupported protocol version {:u}", handshake.protocol_version)));
            }
            if (handshake.capability_flags & consts::CLIENT_PROTOCOL_41) == 0 {
                return Err(MyStrError("Server must set CLIENT_PROTOCOL_41 flag".to_owned()));
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
                    return Ok(());
                },
                0xffu8 => {
                    let err = try_io!(ErrPacket::from_payload(pld.as_slice()));
                    return Err(MySqlError(err));
                },
                _ => return Err(MyStrError("Unexpected packet".to_owned()))
            }
        })
    }
    fn do_handshake_response(&mut self, hp: &HandshakePacket) -> MyResult<()> {
        let mut client_flags = consts::CLIENT_PROTOCOL_41 |
                           consts::CLIENT_SECURE_CONNECTION |
                           consts::CLIENT_LONG_PASSWORD |
                           consts::CLIENT_TRANSACTIONS |
                           consts::CLIENT_LOCAL_FILES |
                           (self.capability_flags & consts::CLIENT_LONG_FLAG);
        let scramble_buf = scramble(hp.auth_plugin_data.as_slice(), self.opts.get_pass().into_bytes());
        let scramble_buf_len = if scramble_buf.is_some() { 20 } else { 0 };
        let mut payload_len = 4 + 4 + 1 + 23 + self.opts.get_user().len() + 1 + 1 + scramble_buf_len;
        if self.opts.get_db_name().len() > 0 {
            client_flags |= consts::CLIENT_CONNECT_WITH_DB;
            payload_len += self.opts.get_db_name().len() + 1;
        }

        let mut writer = MemWriter::with_capacity(payload_len);
        try_io!(writer.write_le_u32(client_flags));
        try_io!(writer.write_le_u32(0u32));
        try_io!(writer.write_u8(consts::UTF8_GENERAL_CI));
        try_io!(writer.write(~[0u8, ..23]));
        try_io!(writer.write_str(self.opts.get_user()));
        try_io!(writer.write_u8(0u8));
        try_io!(writer.write_u8(scramble_buf_len as u8));
        if scramble_buf.is_some() {
            try_io!(writer.write(scramble_buf.unwrap()));
        }
        if self.opts.get_db_name().len() > 0 {
            try_io!(writer.write_str(self.opts.get_db_name()));
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
                        try!(self.write_command_data(consts::COM_STMT_SEND_LONG_DATA, writer.unwrap().as_slice()));
                    }
                },
                _ => (/* quite strange so do nothing */)
            }
        }
        Ok(())
    }
    fn execute<'a>(&'a mut self, stmt: &InnerStmt, params: &[Value]) -> MyResult<QueryResult<'a>> {
        if stmt.num_params != params.len() as u16 {
            return Err(MyStrError(format!("Statement takes {:u} parameters but {:u} was supplied", stmt.num_params, params.len())));
        }
        let mut writer = MemWriter::new();
        try_io!(writer.write_le_u32(stmt.statement_id));
        try_io!(writer.write_u8(0u8));
        try_io!(writer.write_le_u32(1u32));
        if stmt.num_params > 0 {
            let (bitmap, values, large_ids) = try_io!(Value::to_bin_payload(stmt.params.get_ref().as_slice(),
                                                                    params,
                                                                    self.max_allowed_packet));
            if large_ids.is_some() {
                try!(self.send_long_data(stmt, params, large_ids.unwrap()));
            }
            try_io!(writer.write(bitmap.as_slice()));
            try_io!(writer.write_u8(1u8));
            let mut i = -1;
            while { i += 1; i < params.len() } {
                let _ = match params[i] {
                    NULL => try_io!(writer.write([stmt.params.get_ref().get(i).column_type, 0u8])),
                    Bytes(..) => try_io!(writer.write([consts::MYSQL_TYPE_VAR_STRING, 0u8])),
                    Int(..) => try_io!(writer.write([consts::MYSQL_TYPE_LONGLONG, 0u8])),
                    UInt(..) => try_io!(writer.write([consts::MYSQL_TYPE_LONGLONG, 128u8])),
                    Float(..) => try_io!(writer.write([consts::MYSQL_TYPE_DOUBLE, 0u8])),
                    Date(..) => try_io!(writer.write([consts::MYSQL_TYPE_DATE, 0u8])),
                    Time(..) => try_io!(writer.write([consts::MYSQL_TYPE_TIME, 0u8]))
                };
            }
            try_io!(writer.write(values.as_slice()));
        }
        try!(self.write_command_data(consts::COM_STMT_EXECUTE, writer.unwrap().as_slice()));
        let pld = try!(self.read_packet());
        match *pld.get(0) {
            0u8 => {
                let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
                self.handle_ok(&ok);
                Ok(QueryResult{conn: self,
                               columns: Vec::with_capacity(0),
                               eof: true,
                               is_bin: true,
                               ok_packet: Some(ok)})
            },
            0xffu8 => {
                let err = try_io!(ErrPacket::from_payload(pld.as_slice()));
                Err(MySqlError(err))
            },
            _ => {
                let mut reader = BufReader::new(pld.as_slice());
                let column_count = try_io!(reader.read_lenenc_int());
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as uint);
                let mut i = -1;
                while { i += 1; i < column_count } {
                    let pld = try!(self.read_packet());
                    //let pld = match self.read_packet() {
                    //    Ok(pld) => pld,
                    //    Err(error) => return Err(error)
                    //};
                    columns.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
                }
                try!(self.read_packet());
                return Ok(QueryResult{conn: self,
                                      columns: columns,
                                      eof: false,
                                      is_bin: true,
                                      ok_packet: None});
            }
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
    pub fn query<'a>(&'a mut self, query: &str) -> MyResult<QueryResult<'a>> {
        try!(self.write_command_data(consts::COM_QUERY, query.as_bytes()));
        let pld = try!(self.read_packet());
        match *pld.get(0) {
            0u8 => {
                let ok = try_io!(OkPacket::from_payload(pld.as_slice()));
                self.handle_ok(&ok);
                return Ok(QueryResult{conn: self,
                                      columns: Vec::with_capacity(0),
                                      eof: true,
                                      is_bin: false,
                                      ok_packet: Some(ok)});
            },
            0xfb_u8 => {
                let mut reader = BufReader::new(pld.as_slice());
                try_io!(reader.seek(1, SeekCur));
                let file_name = try_io!(reader.read_to_end());
                return match self.send_local_infile(file_name.as_slice()) {
                    Ok(x) => Ok(QueryResult{conn: self,
                                            columns: Vec::with_capacity(0),
                                            eof: true,
                                            is_bin: false,
                                            ok_packet: x}),
                    Err(err) => Err(err)
                };
            },
            0xff_u8 => {
                let err = try_io!(ErrPacket::from_payload(pld.as_slice()));
                return Err(MySqlError(err));
            },
            _ => {
                let mut reader = BufReader::new(pld.as_slice());
                let column_count = try_io!(reader.read_lenenc_int());
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as uint);
                let mut i = -1;
                while { i += 1; i < column_count } {
                    let pld = try!(self.read_packet());
                    columns.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
                }
                // skip eof packet
                try!(self.read_packet());
                return Ok(QueryResult{conn: self,
                                      columns: columns,
                                      eof: false,
                                      is_bin: false,
                                      ok_packet: None});
            }
        }
    }
    pub fn prepare<'a>(&'a mut self, query: &str) -> MyResult<Stmt<'a>> {
        try!(self.write_command_data(consts::COM_STMT_PREPARE, query.as_bytes()));
        let pld = try!(self.read_packet());
        match *pld.get(0) {
            0xff => {
                let err = try_io!(ErrPacket::from_payload(pld.as_slice()));
                return Err(MySqlError(err));
            },
            _ => {
                let mut stmt = try_io!(InnerStmt::from_payload(pld.as_slice()));
                if stmt.num_params > 0 {
                    let mut params: Vec<Column> = Vec::with_capacity(stmt.num_params as uint);
                    let mut i = -1;
                    while { i += 1; i < stmt.num_params } {
                        let pld = try!(self.read_packet());
                        params.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
                    }
                    stmt.params = Some(params);
                    try!(self.read_packet());
                }
                if stmt.num_columns > 0 {
                    let mut columns: Vec<Column> = Vec::with_capacity(stmt.num_columns as uint);
                    let mut i = -1;
                    while { i += 1; i < stmt.num_columns } {
                        let pld = try!(self.read_packet());
                        columns.push(try_io!(Column::from_payload(self.last_command, pld.as_slice())));
                    }
                    stmt.columns = Some(columns);
                    try!(self.read_packet());
                }
                return Ok(Stmt{conn: self,
                               stmt: stmt});
            }
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
                Err(MyStrError("Can't get max_allowed_packet value".to_owned()))
            } else {
                self.max_allowed_packet = max_allowed_packet;
                self.connected = true;
                Ok(())
            }
        })
    }
    fn get_system_var(&mut self, name: &str) -> Option<Value> {
        for row in &mut self.query(format!("SELECT @@{:s};", name)) {
            if row.is_ok() {
                let mut row = row.unwrap();
                return row.shift();
            } else {
                return None;
            }
        }
        return None;
    }
}

impl Reader for MyConn {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        self.stream.read(buf)
    }
}
impl Reader for ~MyConn {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        self.read(buf)
    }
}
impl<'a> Reader for &'a MyConn {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        self.read(buf)
    }
}

impl Writer for MyConn {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        self.stream.write(buf)
    }
}
impl Writer for ~MyConn {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        self.write(buf)
    }
}
impl<'a> Writer for &'a MyConn {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        self.write(buf)
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
    conn: &'a mut MyConn,
    columns: Vec<Column>,
    ok_packet: Option<OkPacket>,
    eof: bool,
    is_bin: bool
}

impl<'a> QueryResult<'a> {
    pub fn affected_rows(&self) -> u64 {
        self.conn.affected_rows
    }
    pub fn last_insert_id(&self) -> u64 {
        self.conn.last_insert_id
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
        if self.eof {
            return None
        }
        let pld = match self.conn.read_packet() {
                Err(err) => {
                    self.eof = true;
                    return Some(Err(err));
                },
                Ok(pld) => pld
        };
        if self.is_bin {
            if *pld.get(0) == 0xfe && pld.len() < 0xfe {
                self.eof = true;
                let p = EOFPacket::from_payload(pld.as_slice());
                match p {
                    Ok(p) => {
                        self.conn.handle_eof(&p);
                    },
                    Err(e) => return Some(Err(MyIoError(e)))
                }
                return None;
            }
            let res = Value::from_bin_payload(pld.as_slice(), self.columns.as_slice());
            match res {
                Ok(p) => Some(Ok(p)),
                Err(e) => {
                    self.eof = true;
                    return Some(Err(MyIoError(e)));
                }
            }
        } else {
            if (*pld.get(0) == 0xfe_u8 || *pld.get(0) == 0xff_u8) && pld.len() < 0xfe {
                self.eof = true;
                if *pld.get(0) == 0xfe_u8 {
                    let p = EOFPacket::from_payload(pld.as_slice());
                    match p {
                        Ok(p) => {
                            self.conn.handle_eof(&p);
                        },
                        Err(e) => return Some(Err(MyIoError(e)))
                    }
                    return None;
                } else if *pld.get(0) == 0xff_u8 {
                    let p = ErrPacket::from_payload(pld.as_slice());
                    match p {
                        Ok(p) => return Some(Err(MySqlError(p))),
                        Err(e) => return Some(Err(MyIoError(e)))
                    }
                }
            }
            let res = Value::from_payload(pld.as_slice(), self.columns.len());
            match res {
                Ok(p) => Some(Ok(p)),
                Err(e) => {
                    self.eof = true;
                    Some(Err(MyIoError(e)))
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
        let conn = MyConn::new(MyOpts{user: Some("root".to_owned()),
                                      ..Default::default()});
        assert!(conn.is_ok());
    }

    #[test]
    fn test_connect_with_db() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_owned()),
                                          db_name: Some("mysql".to_owned()),
                                          ..Default::default()}).unwrap();
        for x in &mut conn.query("SELECT DATABASE()") {
            assert!(x.unwrap().shift().unwrap().unwrap_bytes() == Vec::from_slice(("mysql".to_owned()).into_bytes()));
        }
    }

    #[test]
    fn test_query() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_owned()),
                                          ..Default::default()}).unwrap();
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
        assert!(conn.affected_rows == 2);
        for _ in &mut conn.query("SELECT * FROM tbl WHERE a = 'bar'") {
            assert!(false);
        }
        let mut count = 0;
        for row in &mut conn.query("SELECT * FROM tbl") {
            assert!(row.is_ok());
            let row = row.unwrap();
            if count == 0 {
                assert!(*row.get(0) == Bytes(Vec::from_slice("foo".to_owned().into_bytes())));
                assert!(*row.get(1) == Bytes(Vec::from_slice("-123".to_owned().into_bytes())));
                assert!(*row.get(2) == Bytes(Vec::from_slice("123".to_owned().into_bytes())));
                assert!(*row.get(3) == Bytes(Vec::from_slice("2014-05-05".to_owned().into_bytes())));
                assert!(*row.get(4) == Bytes(Vec::from_slice("123.123".to_owned().into_bytes())));
            } else {
                assert!(*row.get(0) == Bytes(Vec::from_slice("foo".to_owned().into_bytes())));
                assert!(*row.get(1) == Bytes(Vec::from_slice("-321".to_owned().into_bytes())));
                assert!(*row.get(2) == Bytes(Vec::from_slice("321".to_owned().into_bytes())));
                assert!(*row.get(3) == Bytes(Vec::from_slice("2014-06-06".to_owned().into_bytes())));
                assert!(*row.get(4) == Bytes(Vec::from_slice("321.321".to_owned().into_bytes())));
            }
            count += 1;
        }
        assert!(count == 2);
        for row in &mut conn.query("SELECT REPEAT('A', 20000000)") {
            assert!(row.is_ok());
            let row = row.unwrap();
            let val = row.get(0).bytes_ref();
            assert!(val.len() == 20000000);
            for y in val.iter() {
                assert!(y == &65u8);
            }
        }
    }

    #[test]
    fn test_prepared_statemenst() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_owned()),
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT, b INT, c INT UNSIGNED, d DATE, e DOUBLE)").is_ok());
        {
            let stmt = conn.prepare("INSERT INTO tbl(a, b, c, d, e) VALUES (?, ?, ?, ?, ?)");
            assert!(stmt.is_ok());
            let mut stmt = stmt.unwrap();
            assert!(stmt.execute([Bytes(Vec::from_slice("hello".to_owned().into_bytes())), Int(-123), UInt(123), Date(2014, 5, 5,0,0,0,0), Float(123.123f64)]).is_ok());
            assert!(stmt.execute([Bytes(Vec::from_slice("world".to_owned().into_bytes())), NULL, NULL, NULL, Float(321.321f64)]).is_ok());
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
                    assert!(*row.get(0) == Bytes(vec!(104u8, 101u8, 108u8, 108u8, 111u8)));
                    assert!(*row.get(1) == Int(-123i64));
                    assert!(*row.get(2) == Int(123i64));
                    assert!(*row.get(3) == Date(2014u16, 5u8, 5u8, 0u8, 0u8, 0u8, 0u32));
                    assert!(row.get(4).get_float() == 123.123);
                } else {
                    assert!(*row.get(0) == Bytes(vec!(119u8, 111u8, 114u8, 108u8, 100u8)));
                    assert!(*row.get(1) == NULL);
                    assert!(*row.get(2) == NULL);
                    assert!(*row.get(3) == NULL);
                    assert!(row.get(4).get_float() == 321.321);
                }
                i += 1;
            }
        }
        let stmt = conn.prepare("SELECT REPEAT('A', 20000000);");
        let mut stmt = stmt.unwrap();
        for row in &mut stmt.execute([]) {
            assert!(row.is_ok());
            let row = row.unwrap();
            let v: &[u8] = row.get(0).bytes_ref();
            assert!(v.len() == 20000000);
            for y in v.iter() {
                assert!(y == &65u8);
            }
        }
    }

    #[test]
    fn test_large_insert() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_owned()),
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a LONGBLOB)").is_ok());
        let query = format!("INSERT INTO tbl(a) VALUES('{:s}')", str::from_chars(Vec::from_elem(20000000, 'A').as_slice()));
        assert!(conn.query(query).is_ok());
        let x = (&mut conn.query("SELECT * FROM tbl")).next().unwrap();
        assert!(x.is_ok());
        let v: Vec<u8> = x.unwrap().shift().unwrap().unwrap_bytes();
        assert!(v.len() == 20000000);
        for y in v.iter() {
            assert!(y == &65u8);
        }

    }

    #[test]
    fn test_large_insert_prepared() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_owned()),
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
        let v = row.get(0).bytes_ref();
        assert!(v.len() == 20000000);
        for y in v.iter() {
            assert!(y == &65u8);
        }
    }

    #[test]
    #[allow(unused_must_use)]
    fn test_local_infile() {
        let mut conn = MyConn::new(MyOpts{user: Some("root".to_owned()),
                                          ..Default::default()}).unwrap();
        assert!(conn.query("DROP DATABASE IF EXISTS test").is_ok());
        assert!(conn.query("CREATE DATABASE test").is_ok());
        assert!(conn.query("USE test").is_ok());
        assert!(conn.query("CREATE TABLE tbl(a TEXT)").is_ok());
        let mut path = getcwd();
        path.push("local_infile.txt".to_owned());
        {
            let mut file = File::create(&path).unwrap();
            file.write_line("AAAAAA");
            file.write_line("BBBBBB");
            file.write_line("CCCCCC");
        }
        let query = format!("LOAD DATA LOCAL INFILE '{:s}' INTO TABLE tbl", str::from_utf8(path.clone().into_vec().as_slice()).unwrap());
        assert!(conn.query(query).is_ok());
        let mut count = 0;
        for row in &mut conn.query("SELECT * FROM tbl") {
            assert!(row.is_ok());
            let row = row.unwrap();
            match count {
                0 => assert!(row == vec!(Bytes(vec!(65u8, 65u8, 65u8, 65u8, 65u8, 65u8)))),
                1 => assert!(row == vec!(Bytes(vec!(66u8, 66u8, 66u8, 66u8, 66u8, 66u8)))),
                2 => assert!(row == vec!(Bytes(vec!(67u8, 67u8, 67u8, 67u8, 67u8, 67u8)))),
                _ => assert!(false)
            }
            count += 1;
        }
        assert!(count == 3);
        unlink(&path);
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_exec(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { conn.query("DO 1"); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_prepared_exec(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("DO 1").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_query_row(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { (&mut conn.query("SELECT 1")).next(); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_prepared_query_row(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT 1").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_simple_prepared_query_row_param(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT ?").unwrap();
        let mut i = 0;
        bench.iter(|| { stmt.execute([Int(i)]); i += 1; })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_prepared_query_row_5param(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        let params = ~[Int(42), Bytes(vec!(104u8, 101u8, 108u8, 108u8, 111u8, 111u8)), Float(1.618), NULL, Int(1)];
        bench.iter(|| { stmt.execute(params); })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_select_large_string(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        bench.iter(|| { for _ in &mut conn.query("SELECT REPEAT('A', 10000)") {} })
    }

    #[bench]
    #[allow(unused_must_use)]
    fn bench_select_prepared_large_string(bench: &mut Bencher) {
        let mut conn = MyConn::new(MyOpts{unix_addr: Some(Path::new("/run/mysqld/mysqld.sock")),
                                          user: Some("root".to_owned()),
                                          pass: Some("password".to_owned()),
                                          ..Default::default()}).unwrap();
        let mut stmt = conn.prepare("SELECT REPEAT('A', 10000)").unwrap();
        bench.iter(|| { stmt.execute([]); })
    }
}
