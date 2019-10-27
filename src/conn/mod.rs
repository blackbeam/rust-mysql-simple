use mysql_common::crypto;
use mysql_common::named_params::parse_named_params;
use mysql_common::packets::{
    column_from_payload, parse_auth_switch_request, parse_err_packet, parse_handshake_packet,
    parse_ok_packet, AuthPlugin, AuthSwitchRequest, Column, ComStmtClose,
    ComStmtExecuteRequestBuilder, ComStmtSendLongData, HandshakePacket, HandshakeResponse,
    OkPacket, SslRequest,
};
use mysql_common::proto::{codec::Compression, sync_framed::MySyncFramed};
use mysql_common::value::{read_bin_values, read_text_values, ServerSide};

use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::{self, Read, Write as _};
use std::ops::{Deref, DerefMut};
use std::{cmp, fs, mem, path, process};

use crate::conn::local_infile::LocalInfile;
use crate::conn::query_result::ResultConnRef;
use crate::conn::stmt::InnerStmt;
use crate::conn::stmt_cache::StmtCache;
use crate::conn::transaction::IsolationLevel;
use crate::consts::{CapabilityFlags, Command, StatusFlags, MAX_PAYLOAD_LEN};
use crate::io::{Read as MyRead, Stream};
use crate::prelude::FromRow;
use crate::DriverError::{
    CouldNotConnect, MismatchedStmtParams, NamedParamsForPositionalQuery, Protocol41NotSet,
    ReadOnlyTransNotSupported, SetupError, TlsNotSupported, UnexpectedPacket, UnknownAuthPlugin,
    UnsupportedProtocol,
};
use crate::Error::{DriverError, MySqlError};
use crate::Value::{self, Bytes, NULL};
use crate::{
    from_row, from_value, from_value_opt, LocalInfileHandler, Opts, OptsBuilder, Params,
    QueryResult, Result as MyResult, SslOpts, Stmt, Transaction,
};

pub mod local_infile;
pub mod opts;
pub mod pool;
pub mod query_result;
pub mod stmt;
mod stmt_cache;
pub mod transaction;

/// A trait allowing abstraction over connections and transactions
pub trait GenericConnection {
    /// See
    /// [`Conn#query`](struct.Conn.html#method.query).
    fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult<'_>>;

    /// See
    /// [`Conn#first`](struct.Conn.html#method.first).
    fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>>;

    /// See
    /// [`Conn#prepare`](struct.Conn.html#method.prepare).
    fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt<'_>>;

    /// See
    /// [`Conn#prep_exec`](struct.Conn.html#method.prep_exec).
    fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult<'_>>
    where
        A: AsRef<str>,
        T: Into<Params>;

    /// See
    /// [`Conn#first_exec`](struct.Conn.html#method.first_exec).
    fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        T: FromRow;
}

/// Mysql connection.
#[derive(Debug)]
pub struct Conn {
    opts: Opts,
    stream: Option<MySyncFramed<Stream>>,
    stmt_cache: StmtCache,
    server_version: Option<(u16, u16, u16)>,
    mariadb_server_version: Option<(u16, u16, u16)>,
    affected_rows: u64,
    last_insert_id: u64,
    warnings: u16,
    info: Option<Vec<u8>>,
    capability_flags: CapabilityFlags,
    connection_id: u32,
    status_flags: StatusFlags,
    character_set: u8,
    last_command: u8,
    connected: bool,
    has_results: bool,
    local_infile_handler: Option<LocalInfileHandler>,
}

impl Conn {
    fn stream_ref(&self) -> &MySyncFramed<Stream> {
        self.stream.as_ref().expect("incomplete connection")
    }

    fn stream_mut(&mut self) -> &mut MySyncFramed<Stream> {
        self.stream.as_mut().expect("incomplete connection")
    }

    fn empty<T: Into<Opts>>(opts: T) -> Conn {
        let opts = opts.into();
        Conn {
            stmt_cache: StmtCache::new(opts.get_stmt_cache_size()),
            opts,
            stream: None,
            capability_flags: CapabilityFlags::empty(),
            status_flags: StatusFlags::empty(),
            connection_id: 0u32,
            character_set: 0u8,
            affected_rows: 0u64,
            last_insert_id: 0u64,
            warnings: 0,
            info: None,
            last_command: 0u8,
            connected: false,
            has_results: false,
            server_version: None,
            mariadb_server_version: None,
            local_infile_handler: None,
        }
    }

    fn is_insecure(&self) -> bool {
        self.stream_ref().get_ref().is_insecure()
    }

    fn is_socket(&self) -> bool {
        self.stream_ref().get_ref().is_socket()
    }

    /// Check the connection can be improved.
    #[allow(unused_assignments)]
    fn can_improved(&mut self) -> Option<Opts> {
        if self.opts.get_prefer_socket() && self.opts.addr_is_loopback() {
            let mut socket = None;
            #[cfg(test)]
            {
                socket = self.opts.0.injected_socket.clone();
            }
            if socket.is_none() {
                socket = self.get_system_var("socket").map(from_value::<String>);
            }
            if let Some(socket) = socket {
                if self.opts.get_socket().is_none() {
                    let mut socket_opts = OptsBuilder::from_opts(self.opts.clone());
                    if !socket.is_empty() {
                        socket_opts.socket(Some(socket));
                        return Some(socket_opts.into());
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
                let mut improved_conn = Conn::empty(new_opts);
                improved_conn
                    .connect_stream()
                    .and_then(|_| {
                        improved_conn.connect()?;
                        Ok(improved_conn)
                    })
                    .unwrap_or(conn)
            } else {
                conn
            }
        };
        for cmd in conn.opts.get_init() {
            conn.query(cmd)?;
        }
        Ok(conn)
    }

    fn soft_reset(&mut self) -> MyResult<()> {
        self.write_command(Command::COM_RESET_CONNECTION, &[])?;
        self.read_packet().and_then(|pld| match pld[0] {
            0 => {
                let ok = parse_ok_packet(&*pld, self.capability_flags)?;
                self.handle_ok(&ok);
                self.last_command = 0;
                self.stmt_cache.clear();
                Ok(())
            }
            _ => {
                let err = parse_err_packet(&*pld, self.capability_flags)?;
                Err(MySqlError(err.into()))
            }
        })
    }

    fn hard_reset(&mut self) -> MyResult<()> {
        self.stream = None;
        self.stmt_cache.clear();
        self.capability_flags = CapabilityFlags::empty();
        self.status_flags = StatusFlags::empty();
        self.connection_id = 0;
        self.character_set = 0;
        self.affected_rows = 0;
        self.last_insert_id = 0;
        self.warnings = 0;
        self.info = None;
        self.last_command = 0;
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
            }
            (_, Some(ref version)) if *version >= (10, 2, 7) => {
                self.soft_reset().or_else(|_| self.hard_reset())
            }
            _ => self.hard_reset(),
        }
    }

    fn switch_to_ssl(&mut self, ssl_opts: SslOpts) -> MyResult<()> {
        let stream = self.stream.take().expect("incomplete conn");
        let (in_buf, out_buf, codec, stream) = stream.destruct();
        let stream = stream.make_secure(self.opts.get_ip_or_hostname(), ssl_opts)?;
        let stream = MySyncFramed::construct(in_buf, out_buf, codec, stream);
        self.stream = Some(stream);
        Ok(())
    }

    fn connect_stream(&mut self) -> MyResult<()> {
        let read_timeout = self.opts.get_read_timeout().cloned();
        let write_timeout = self.opts.get_write_timeout().cloned();
        let tcp_keepalive_time = self.opts.get_tcp_keepalive_time_ms();
        let tcp_nodelay = self.opts.get_tcp_nodelay();
        let tcp_connect_timeout = self.opts.get_tcp_connect_timeout();
        let bind_address = self.opts.bind_address().cloned();
        let stream = if let Some(socket) = self.opts.get_socket() {
            Stream::connect_socket(&*socket, read_timeout, write_timeout)?
        } else if let Some(ip_or_hostname) = self.opts.get_ip_or_hostname() {
            let port = self.opts.get_tcp_port();
            Stream::connect_tcp(
                &*ip_or_hostname,
                port,
                read_timeout,
                write_timeout,
                tcp_keepalive_time,
                tcp_nodelay,
                tcp_connect_timeout,
                bind_address,
            )?
        } else {
            return Err(DriverError(CouldNotConnect(None)));
        };
        self.stream = Some(MySyncFramed::new(stream));
        Ok(())
    }

    fn read_packet(&mut self) -> MyResult<Vec<u8>> {
        let data = self.stream_mut().next().transpose()?.ok_or(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "server disconnected",
        ))?;
        match data[0] {
            0xff => {
                let error_packet = parse_err_packet(&*data, self.capability_flags)?;
                self.handle_err();
                Err(MySqlError(error_packet.into()))
            }
            _ => Ok(data),
        }
    }

    fn drop_packet(&mut self) -> MyResult<()> {
        self.read_packet().map(|_| ())
    }

    fn write_packet<T: Into<Vec<u8>>>(&mut self, data: T) -> MyResult<()> {
        self.stream_mut().send(data.into())?;
        Ok(())
    }

    fn handle_handshake(&mut self, hp: &HandshakePacket<'_>) {
        self.capability_flags = hp.capabilities() & self.get_client_flags();
        self.status_flags = hp.status_flags();
        self.connection_id = hp.connection_id();
        self.character_set = hp.default_collation();
        self.server_version = hp.server_version_parsed();
        self.mariadb_server_version = hp.maria_db_server_version_parsed();
    }

    fn handle_ok(&mut self, op: &OkPacket<'_>) {
        self.affected_rows = op.affected_rows();
        self.last_insert_id = op.last_insert_id().unwrap_or(0);
        self.status_flags = op.status_flags();
        self.warnings = op.warnings();
        self.info = op.info_ref().map(Into::into);
    }

    fn handle_err(&mut self) {
        self.has_results = false;
        self.last_insert_id = 0;
        self.affected_rows = 0;
        self.warnings = 0;
    }

    fn more_results_exists(&self) -> bool {
        self.status_flags
            .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }

    fn perform_auth_switch(&mut self, auth_switch_request: AuthSwitchRequest<'_>) -> MyResult<()> {
        let nonce = auth_switch_request.plugin_data();
        let plugin_data = auth_switch_request
            .auth_plugin()
            .gen_data(self.opts.get_pass(), nonce);
        self.write_packet(plugin_data.unwrap_or_else(Vec::new))?;
        self.continue_auth(auth_switch_request.auth_plugin(), nonce, true)
    }

    fn do_handshake(&mut self) -> MyResult<()> {
        let payload = self.read_packet()?;
        let handshake = parse_handshake_packet(payload.as_ref())?;

        if handshake.protocol_version() != 10u8 {
            return Err(DriverError(UnsupportedProtocol(
                handshake.protocol_version(),
            )));
        }

        if !handshake
            .capabilities()
            .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
        {
            return Err(DriverError(Protocol41NotSet));
        }

        self.handle_handshake(&handshake);

        if self.is_insecure() {
            if let Some(ssl_opts) = self.opts.get_ssl_opts().cloned() {
                if !handshake
                    .capabilities()
                    .contains(CapabilityFlags::CLIENT_SSL)
                {
                    return Err(DriverError(TlsNotSupported));
                } else {
                    self.do_ssl_request()?;
                    self.switch_to_ssl(ssl_opts)?;
                }
            }
        }

        let nonce = handshake.nonce();

        let auth_plugin = handshake
            .auth_plugin()
            .unwrap_or(&AuthPlugin::MysqlNativePassword);
        if let AuthPlugin::Other(ref name) = auth_plugin {
            let plugin_name = String::from_utf8_lossy(name).into();
            Err(DriverError(UnknownAuthPlugin(plugin_name)))?
        }

        let auth_data = auth_plugin.gen_data(self.opts.get_pass(), &*nonce);
        self.write_handshake_response(auth_plugin, auth_data.as_ref().map(AsRef::as_ref))?;

        self.continue_auth(auth_plugin, &*nonce, false)?;

        if self
            .capability_flags
            .contains(CapabilityFlags::CLIENT_COMPRESS)
        {
            self.switch_to_compressed();
        }

        Ok(())
    }

    fn switch_to_compressed(&mut self) {
        self.stream_mut()
            .codec_mut()
            .compress(Compression::default());
    }

    fn get_client_flags(&self) -> CapabilityFlags {
        let mut client_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_LOCAL_FILES
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PS_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_CONNECT_ATTRS
            | (self.capability_flags & CapabilityFlags::CLIENT_LONG_FLAG);
        if self.opts.get_compress().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_COMPRESS);
        }
        if let Some(db_name) = self.opts.get_db_name() {
            if !db_name.is_empty() {
                client_flags.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
            }
        }
        if self.is_insecure() && self.opts.get_ssl_opts().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_SSL);
        }
        client_flags | self.opts.get_additional_capabilities()
    }

    fn connect_attrs(&self) -> HashMap<String, String> {
        let arg0 = std::env::args_os().next();
        let arg0 = arg0.as_ref().map(|x| x.to_string_lossy());
        let program_name = arg0.unwrap_or("".into()).to_owned();

        let mut attrs = HashMap::new();

        attrs.insert("_client_name".into(), "rust-mysql-simple".into());
        attrs.insert("_client_version".into(), env!("CARGO_PKG_VERSION").into());
        attrs.insert("_os".into(), env!("CARGO_CFG_TARGET_OS").into());
        attrs.insert("_pid".into(), process::id().to_string());
        attrs.insert("_platform".into(), env!("CARGO_CFG_TARGET_ARCH").into());
        attrs.insert("program_name".into(), program_name.to_string());

        for (name, value) in self.opts.get_connect_attrs().clone() {
            attrs.insert(name, value);
        }

        attrs
    }

    fn do_ssl_request(&mut self) -> MyResult<()> {
        let ssl_request = SslRequest::new(self.get_client_flags());
        self.write_packet(ssl_request)
    }

    fn write_handshake_response(
        &mut self,
        auth_plugin: &AuthPlugin<'_>,
        scramble_buf: Option<&[u8]>,
    ) -> MyResult<()> {
        let handshake_response = HandshakeResponse::new(
            &scramble_buf,
            self.server_version.unwrap_or((0, 0, 0)),
            self.opts.get_user(),
            self.opts.get_db_name(),
            auth_plugin,
            self.capability_flags,
            &self.connect_attrs(),
        );
        self.write_packet(handshake_response)
    }

    fn continue_auth(
        &mut self,
        auth_plugin: &AuthPlugin<'_>,
        nonce: &[u8],
        auth_switched: bool,
    ) -> MyResult<()> {
        match auth_plugin {
            AuthPlugin::MysqlNativePassword => {
                self.continue_mysql_native_password_auth(auth_switched)
            }
            AuthPlugin::CachingSha2Password => {
                self.continue_caching_sha2_password_auth(nonce, auth_switched)
            }
            AuthPlugin::Other(ref name) => {
                let plugin_name = String::from_utf8_lossy(name).into();
                Err(DriverError(UnknownAuthPlugin(plugin_name)))?
            }
        }
    }

    fn continue_mysql_native_password_auth(&mut self, auth_switched: bool) -> MyResult<()> {
        let payload = self.read_packet()?;

        match payload[0] {
            // auth ok
            0x00 => {
                let ok = parse_ok_packet(&*payload, self.capability_flags)?;
                self.handle_ok(&ok);
                Ok(())
            }
            // auth switch
            0xfe if !auth_switched => {
                let auth_switch_request = parse_auth_switch_request(&*payload)?;
                self.perform_auth_switch(auth_switch_request)
            }
            _ => Err(DriverError(UnexpectedPacket)),
        }
    }

    fn continue_caching_sha2_password_auth(
        &mut self,
        nonce: &[u8],
        auth_switched: bool,
    ) -> MyResult<()> {
        let payload = self.read_packet()?;

        match payload[0] {
            0x00 => {
                // ok packet for empty password
                Ok(())
            }
            0x01 => match payload[1] {
                0x03 => {
                    let payload = self.read_packet()?;
                    let ok = parse_ok_packet(&*payload, self.capability_flags)?;
                    self.handle_ok(&ok);
                    Ok(())
                }
                0x04 => {
                    if !self.is_insecure() || self.is_socket() {
                        let mut pass = self.opts.get_pass().map(Vec::from).unwrap_or_else(Vec::new);
                        pass.push(0);
                        self.write_packet(pass)?;
                    } else {
                        self.write_packet(vec![0x02])?;
                        let payload = self.read_packet()?;
                        let key = &payload[1..];
                        let mut pass = self.opts.get_pass().map(Vec::from).unwrap_or_else(Vec::new);
                        pass.push(0);
                        for i in 0..pass.len() {
                            pass[i] ^= nonce[i % nonce.len()];
                        }
                        let encrypted_pass = crypto::encrypt(&*pass, key);
                        self.write_packet(encrypted_pass)?;
                    }

                    let payload = self.read_packet()?;
                    let ok = parse_ok_packet(&*payload, self.capability_flags)?;
                    self.handle_ok(&ok);
                    Ok(())
                }
                _ => Err(DriverError(UnexpectedPacket)),
            },
            0xfe if !auth_switched => {
                let auth_switch_request = parse_auth_switch_request(&*payload)?;
                self.perform_auth_switch(auth_switch_request)
            }
            _ => Err(DriverError(UnexpectedPacket)),
        }
    }

    fn reset_seq_id(&mut self) {
        self.stream_mut().codec_mut().reset_seq_id();
    }

    fn sync_seq_id(&mut self) {
        self.stream_mut().codec_mut().sync_seq_id();
    }

    fn write_command_raw<T: Into<Vec<u8>>>(&mut self, body: T) -> MyResult<()> {
        let body = body.into();
        self.reset_seq_id();
        self.last_command = body[0];
        self.write_packet(body)
    }

    fn write_command(&mut self, cmd: Command, data: &[u8]) -> MyResult<()> {
        let mut body = Vec::with_capacity(1 + data.len());
        body.push(cmd as u8);
        body.extend_from_slice(data);

        self.write_command_raw(body)
    }

    fn send_long_data(&mut self, stmt: &InnerStmt, params: &[Value]) -> MyResult<()> {
        for (i, value) in params.into_iter().enumerate() {
            match value {
                Bytes(bytes) => {
                    let chunks = bytes.chunks(MAX_PAYLOAD_LEN - 6);
                    let chunks = chunks.chain(if bytes.is_empty() {
                        Some(&[][..])
                    } else {
                        None
                    });
                    for chunk in chunks {
                        let com = ComStmtSendLongData::new(stmt.id(), i, chunk);
                        self.write_command_raw(com)?;
                    }
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn _execute(&mut self, stmt: &InnerStmt, params: Params) -> MyResult<Vec<Column>> {
        let exec_request = match params {
            Params::Empty => {
                if stmt.num_params() != 0 {
                    return Err(DriverError(MismatchedStmtParams(stmt.num_params(), 0)));
                }

                let (body, _) = ComStmtExecuteRequestBuilder::new(stmt.id()).build(&[]);
                body
            }
            Params::Positional(params) => {
                if stmt.num_params() != params.len() as u16 {
                    return Err(DriverError(MismatchedStmtParams(
                        stmt.num_params(),
                        params.len(),
                    )));
                }

                let (body, as_long_data) =
                    ComStmtExecuteRequestBuilder::new(stmt.id()).build(&*params);

                if as_long_data {
                    self.send_long_data(stmt, &*params)?;
                }

                body
            }
            Params::Named(_) => {
                if stmt.named_params().is_none() {
                    return Err(DriverError(NamedParamsForPositionalQuery));
                }
                let named_params = stmt.named_params().unwrap();
                return self._execute(stmt, params.into_positional(named_params)?);
            }
        };
        self.write_command_raw(exec_request)?;
        self.handle_result_set()
    }

    fn execute<T: Into<Params>>(&mut self, stmt: &InnerStmt, params: T) -> MyResult<QueryResult> {
        match self._execute(stmt, params.into()) {
            Ok(columns) => Ok(QueryResult::new(
                ResultConnRef::ViaConnRef(self),
                columns,
                true,
            )),
            Err(err) => Err(err),
        }
    }

    fn _start_transaction(
        &mut self,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> MyResult<()> {
        if let Some(i_level) = isolation_level {
            let _ = self.query(format!("SET TRANSACTION ISOLATION LEVEL {}", i_level))?;
        }
        if let Some(readonly) = readonly {
            let supported = match (self.server_version, self.mariadb_server_version) {
                (Some(ref version), _) if *version >= (5, 6, 5) => true,
                (_, Some(ref version)) if *version >= (10, 0, 0) => true,
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
            let buffer_size = cmp::min(
                MAX_PAYLOAD_LEN - 1,
                self.stream_ref().codec().max_allowed_packet - 1,
            );
            let chunk = vec![0u8; buffer_size].into_boxed_slice();
            let maybe_handler = self
                .local_infile_handler
                .clone()
                .or_else(|| self.opts.get_local_infile_handler().cloned());
            let mut local_infile = LocalInfile::new(io::Cursor::new(chunk), self);
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
        self.write_packet(Vec::new())?;
        let pld = self.read_packet()?;
        if pld[0] == 0u8 {
            let ok = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
            self.handle_ok(&ok);
        }
        Ok(())
    }

    fn handle_result_set(&mut self) -> MyResult<Vec<Column>> {
        if self.more_results_exists() {
            self.sync_seq_id();
        }

        let pld = self.read_packet()?;
        match pld[0] {
            0x00 => {
                let ok = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
                self.handle_ok(&ok);
                Ok(Vec::new())
            }
            0xfb => {
                let mut reader = &pld[1..];
                let mut file_name = Vec::with_capacity(reader.len());
                reader.read_to_end(&mut file_name)?;
                match self.send_local_infile(file_name.as_ref()) {
                    Ok(_) => Ok(Vec::new()),
                    Err(err) => Err(err),
                }
            }
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
                self.has_results = column_count > 0;
                Ok(columns)
            }
        }
    }

    fn _query(&mut self, query: &str) -> MyResult<Vec<Column>> {
        self.write_command(Command::COM_QUERY, query.as_bytes())?;
        self.handle_result_set()
    }

    /// Executes [`COM_PING`](http://dev.mysql.com/doc/internals/en/com-ping.html)
    /// on `Conn`. Return `true` on success or `false` on error.
    pub fn ping(&mut self) -> bool {
        match self.write_command(Command::COM_PING, &[]) {
            Ok(_) => self.drop_packet().is_ok(),
            _ => false,
        }
    }

    /// Executes [`COM_INIT_DB`](https://dev.mysql.com/doc/internals/en/com-init-db.html)
    /// on `Conn`.
    pub fn select_db(&mut self, schema: &str) -> bool {
        match self.write_command(Command::COM_INIT_DB, schema.as_bytes()) {
            Ok(_) => self.drop_packet().is_ok(),
            _ => false,
        }
    }

    /// Starts new transaction with provided options.
    /// `readonly` is only available since MySQL 5.6.5.
    pub fn start_transaction(
        &mut self,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> MyResult<Transaction> {
        self._start_transaction(consistent_snapshot, isolation_level, readonly)?;
        Ok(Transaction::new(self))
    }

    /// Implements text protocol of mysql server.
    ///
    /// Executes mysql query on `Conn`. [`QueryResult`](struct.QueryResult.html)
    /// will borrow `Conn` until the end of its scope.
    pub fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult<'_>> {
        match self._query(query.as_ref()) {
            Ok(columns) => Ok(QueryResult::new(
                ResultConnRef::ViaConnRef(self),
                columns,
                false,
            )),
            Err(err) => Err(err),
        }
    }

    /// Performs query and returns first row.
    pub fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.query(query).and_then(|mut result| {
            if let Some(row) = result.next() {
                row.map(|x| Some(from_row(x)))
            } else {
                Ok(None)
            }
        })
    }

    fn _true_prepare(
        &mut self,
        query: &str,
        named_params: Option<Vec<String>>,
    ) -> MyResult<InnerStmt> {
        self.write_command(Command::COM_STMT_PREPARE, query.as_bytes())?;
        let pld = self.read_packet()?;
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

    fn _prepare(&mut self, query: &str, named_params: Option<Vec<String>>) -> MyResult<InnerStmt> {
        if let Some(inner_st) = self.stmt_cache.get(query) {
            let mut inner_st = inner_st.clone();
            inner_st.set_named_params(named_params);
            return Ok(inner_st);
        }

        let inner_st = self._true_prepare(query, named_params)?;

        if self.stmt_cache.get_cap() > 0 {
            if let Some(old_st) = self.stmt_cache.put(query.into(), inner_st.clone()) {
                let com_stmt_close = ComStmtClose::new(old_st.id());
                self.write_command_raw(com_stmt_close)?;
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
    /// You should wrap such parameters to a proper json value. For example:
    ///
    /// ```ignore
    /// pool.prep_exec(r#"SELECT JSON_REPLACE('{"a": true}', '$.a', ?)"#, (Value::Bool(false),));
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
    pub fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt<'_>> {
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
    pub fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult<'_>>
    where
        A: AsRef<str>,
        T: Into<Params>,
    {
        self.prepare(query)?.prep_exec(params.into())
    }

    /// Executes statement and returns first row.
    pub fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        T: FromRow,
    {
        self.prep_exec(query, params).and_then(|mut result| {
            if let Some(row) = result.next() {
                row.map(|x| Some(from_row(x)))
            } else {
                Ok(None)
            }
        })
    }

    fn connect(&mut self) -> MyResult<()> {
        if self.connected {
            return Ok(());
        }
        self.do_handshake()
            .and_then(|_| {
                Ok(from_value_opt::<usize>(
                    self.get_system_var("max_allowed_packet").unwrap_or(NULL),
                )
                .unwrap_or(0))
            })
            .and_then(|max_allowed_packet| {
                if max_allowed_packet == 0 {
                    Err(DriverError(SetupError))
                } else {
                    self.stream_mut().codec_mut().max_allowed_packet = max_allowed_packet;
                    self.connected = true;
                    Ok(())
                }
            })
    }

    fn get_system_var(&mut self, name: &str) -> Option<Value> {
        for row in self.query(format!("SELECT @@{}", name)).unwrap() {
            if let Ok(mut r) = row {
                if r.len() > 0 {
                    return r.take(0);
                }
            }
        }
        None
    }

    fn next_bin(&mut self, columns: &[Column]) -> MyResult<Option<Vec<Value>>> {
        if !self.has_results {
            return Ok(None);
        }
        let pld = self.read_packet()?;
        if pld[0] == 0xfe && pld.len() < 0xfe {
            self.has_results = false;
            let p = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
            self.handle_ok(&p);
            return Ok(None);
        }
        let values = read_bin_values::<ServerSide>(&*pld, columns)?;
        Ok(Some(values))
    }

    fn next_text(&mut self, col_count: usize) -> MyResult<Option<Vec<Value>>> {
        if !self.has_results {
            return Ok(None);
        }
        let pld = self.read_packet()?;
        if pld[0] == 0xfe && pld.len() < 0xfe {
            self.has_results = false;
            let p = parse_ok_packet(pld.as_ref(), self.capability_flags)?;
            self.handle_ok(&p);
            return Ok(None);
        }
        let values = read_text_values(&*pld, col_count)?;
        Ok(Some(values))
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
        self.status_flags
            .contains(StatusFlags::SERVER_STATUS_NO_BACKSLASH_ESCAPES)
    }
}

impl GenericConnection for Conn {
    fn query<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult<'_>> {
        self.query(query)
    }

    fn first<T: AsRef<str>, U: FromRow>(&mut self, query: T) -> MyResult<Option<U>> {
        self.first(query)
    }

    fn prepare<T: AsRef<str>>(&mut self, query: T) -> MyResult<Stmt<'_>> {
        self.prepare(query)
    }

    fn prep_exec<A, T>(&mut self, query: A, params: T) -> MyResult<QueryResult<'_>>
    where
        A: AsRef<str>,
        T: Into<Params>,
    {
        self.prep_exec(query, params)
    }

    fn first_exec<Q, P, T>(&mut self, query: Q, params: P) -> MyResult<Option<T>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        T: FromRow,
    {
        self.first_exec(query, params)
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        let stmt_cache = mem::replace(&mut self.stmt_cache, StmtCache::new(0));
        for (_, inner_st) in stmt_cache.into_iter() {
            let _ = self.write_command_raw(ComStmtClose::new(inner_st.id()));
        }
        if self.stream.is_some() {
            let _ = self.write_command(Command::COM_QUIT, &[]);
        }
    }
}

/// Possible ways to pass conn to a statement or transaction
#[derive(Debug)]
pub enum ConnRef<'a> {
    ViaConnRef(&'a mut Conn),
    ViaPooledConn(pool::PooledConn),
}

impl<'a> Deref for ConnRef<'a> {
    type Target = Conn;

    fn deref(&self) -> &Conn {
        match *self {
            ConnRef::ViaConnRef(ref conn_ref) => conn_ref,
            ConnRef::ViaPooledConn(ref conn) => conn.as_ref(),
        }
    }
}

impl<'a> DerefMut for ConnRef<'a> {
    fn deref_mut(&mut self) -> &mut Conn {
        match *self {
            ConnRef::ViaConnRef(ref mut conn_ref) => conn_ref,
            ConnRef::ViaPooledConn(ref mut conn) => conn.as_mut(),
        }
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    mod my_conn {
        use std::borrow::ToOwned;
        use std::collections::HashMap;
        use std::io::Write;
        use std::{fs, iter, process};

        use crate::prelude::{FromValue, ToValue};
        use crate::test_misc::get_opts;
        use crate::time::{now, Tm};
        use crate::DriverError::{MissingNamedParameter, NamedParamsForPositionalQuery};
        use crate::Error::DriverError;
        use crate::Value::{Bytes, Date, Int, NULL};
        use crate::{
            from_row, from_value, params, Conn, LocalInfileHandler, Opts, OptsBuilder, Params,
        };

        fn get_system_variable<T>(conn: &mut Conn, name: &str) -> T
        where
            T: FromValue,
        {
            let row = conn
                .first(format!("show variables like '{}'", name))
                .unwrap()
                .unwrap();
            let (_, value): (String, T) = from_row(row);
            value
        }

        #[test]
        fn should_connect() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mode = conn
                .query("SELECT @@GLOBAL.sql_mode")
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .take(0)
                .unwrap();
            let mode = from_value::<String>(mode);
            assert!(mode.contains("TRADITIONAL"));
            assert!(conn.ping());

            if crate::test_misc::test_compression() {
                assert!(format!("{:?}", conn.stream).contains("Compression"));
            }

            if crate::test_misc::test_ssl() {
                assert!(!conn.is_insecure());
            }
        }

        #[test]
        #[should_panic(expected = "Could not connect to address")]
        fn should_fail_on_wrong_socket_path() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.socket(Some("/foo/bar/baz"));
            let _ = Conn::new(opts).unwrap();
        }
        #[test]
        fn should_fallback_to_tcp_if_cant_switch_to_socket() {
            let mut opts = Opts::from(get_opts());
            opts.0.injected_socket = Some("/foo/bar/baz".into());
            let _ = Conn::new(opts).unwrap();
        }
        #[test]
        fn should_connect_with_database() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.db_name(Some("mysql"));
            let mut conn = Conn::new(opts).unwrap();
            assert_eq!(
                conn.query("SELECT DATABASE()")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"mysql".to_vec())]
            );
        }
        #[test]
        fn should_connect_by_hostname() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.db_name(Some("mysql"));
            opts.ip_or_hostname(Some("localhost"));
            let mut conn = Conn::new(opts).unwrap();
            assert_eq!(
                conn.query("SELECT DATABASE()")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"mysql".to_vec())]
            );
        }
        #[test]
        fn should_select_db() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.db_name(Some("mysql"));
            opts.ip_or_hostname(Some("localhost"));
            let mut conn = Conn::new(opts).unwrap();
            assert!(conn
                .query("CREATE DATABASE IF NOT EXISTS t_select_db")
                .is_ok());
            assert!(conn.select_db("t_select_db"));
            assert_eq!(
                conn.query("SELECT DATABASE()")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"t_select_db".to_vec())]
            );
            assert!(conn.query("DROP DATABASE t_select_db").is_ok());
        }
        #[test]
        fn should_execute_queryes_and_parse_results() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn
                .query(
                    "CREATE TEMPORARY TABLE mysql.tbl(\
                                    a TEXT,\
                                    b INT,\
                                    c INT UNSIGNED,\
                                    d DATE,\
                                    e FLOAT
                                )"
                )
                .is_ok());
            assert!(conn
                .query(
                    "INSERT INTO mysql.tbl(a, b, c, d, e) VALUES (\
                     'hello',\
                     -123,\
                     123,\
                     '2014-05-05',\
                     123.123\
                     )"
                )
                .is_ok());
            assert!(conn
                .query(
                    "INSERT INTO mysql.tbl(a, b, c, d, e) VALUES (\
                     'world',\
                     -321,\
                     321,\
                     '2014-06-06',\
                     321.321\
                     )"
                )
                .is_ok());
            assert!(conn.query("SELECT * FROM unexisted").is_err());
            assert!(conn.query("SELECT * FROM mysql.tbl").is_ok());
            // Drop
            assert!(conn.query("UPDATE mysql.tbl SET a = 'foo'").is_ok());
            assert_eq!(conn.affected_rows, 2);
            assert!(conn
                .query("SELECT * FROM mysql.tbl WHERE a = 'bar'")
                .unwrap()
                .next()
                .is_none());
            for (i, row) in conn.query("SELECT * FROM mysql.tbl").unwrap().enumerate() {
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
                conn.query("SELECT REPEAT('A', 20000000)")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(iter::repeat(b'A').take(20_000_000).collect())]
            );
        }
        #[test]
        fn should_execute_statements_and_parse_results() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn
                .query(
                    "CREATE TEMPORARY TABLE mysql.tbl(\
                     a TEXT,\
                     b INT,\
                     c INT UNSIGNED,\
                     d DATE,\
                     e DOUBLE\
                     )"
                )
                .is_ok());
            let _ = conn
                .prepare(
                    "INSERT INTO mysql.tbl(a, b, c, d, e)\
                     VALUES (?, ?, ?, ?, ?)",
                )
                .and_then(|mut stmt| {
                    let tm = Tm {
                        tm_year: 114,
                        tm_mon: 4,
                        tm_mday: 5,
                        tm_hour: 0,
                        tm_min: 0,
                        tm_sec: 0,
                        tm_nsec: 0,
                        ..now()
                    };
                    let hello = b"hello".to_vec();
                    assert!(stmt
                        .execute((&hello, -123, 123, tm.to_timespec(), 123.123f64))
                        .is_ok());
                    stmt.execute(
                        &[
                            &b"".to_vec() as &dyn ToValue,
                            &NULL as &dyn ToValue,
                            &NULL as &dyn ToValue,
                            &NULL as &dyn ToValue,
                            &321.321f64 as &dyn ToValue,
                        ][..],
                    )
                    .unwrap();
                    Ok(())
                })
                .unwrap();
            let _ = conn
                .prepare("SELECT * from mysql.tbl")
                .and_then(|mut stmt| {
                    for (i, row) in stmt.execute(()).unwrap().enumerate() {
                        let mut row = row.unwrap();
                        if i == 0 {
                            assert_eq!(row[0], Bytes(b"hello".to_vec()));
                            assert_eq!(row[1], Int(-123i64));
                            assert_eq!(row[2], Int(123i64));
                            assert_eq!(row[3], Date(2014u16, 5u8, 5u8, 0u8, 0u8, 0u8, 0u32));
                            assert_eq!(row.take::<f64, _>(4).unwrap(), 123.123f64);
                        } else if i == 1 {
                            assert_eq!(row[0], Bytes(b"".to_vec()));
                            assert_eq!(row[1], NULL);
                            assert_eq!(row[2], NULL);
                            assert_eq!(row[3], NULL);
                            assert_eq!(row.take::<f64, _>(4).unwrap(), 321.321f64);
                        } else {
                            unreachable!();
                        }
                    }
                    Ok(())
                })
                .unwrap();
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
            assert!(conn
                .query("CREATE TEMPORARY TABLE mysql.tbl(a INT)")
                .is_ok());
            let _ = conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(1)").is_ok());
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(2)").is_ok());
                    assert!(t.commit().is_ok());
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from mysql.tbl")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"2".to_vec())]
            );
            let _ = conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    t.query("INSERT INTO tbl2(a) VALUES(1)").unwrap_err();
                    Ok(())
                    // implicit rollback
                })
                .unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from mysql.tbl")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"2".to_vec())]
            );
            let _ = conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(1)").is_ok());
                    assert!(t.query("INSERT INTO mysql.tbl(a) VALUES(2)").is_ok());
                    assert!(t.rollback().is_ok());
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from mysql.tbl")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"2".to_vec())]
            );
            let _ = conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    let _ = t
                        .prepare("INSERT INTO mysql.tbl(a) VALUES(?)")
                        .and_then(|mut stmt| {
                            assert!(stmt.execute((3,)).is_ok());
                            assert!(stmt.execute((4,)).is_ok());
                            Ok(())
                        })
                        .unwrap();
                    assert!(t.commit().is_ok());
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                conn.query("SELECT COUNT(a) from mysql.tbl")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"4".to_vec())]
            );
            let _ = conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    t.prep_exec("INSERT INTO mysql.tbl(a) VALUES(?)", (5,))
                        .unwrap();
                    t.prep_exec("INSERT INTO mysql.tbl(a) VALUES(?)", (6,))
                        .unwrap();
                    Ok(())
                })
                .unwrap();
        }
        #[test]
        fn should_handle_LOCAL_INFILE() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn
                .query("CREATE TEMPORARY TABLE mysql.tbl(a TEXT)")
                .is_ok());
            let path = ::std::path::PathBuf::from("local_infile.txt");
            {
                let mut file = fs::File::create(&path).unwrap();
                let _ = file.write(b"AAAAAA\n");
                let _ = file.write(b"BBBBBB\n");
                let _ = file.write(b"CCCCCC\n");
            }
            let query = format!(
                "LOAD DATA LOCAL INFILE '{}' INTO TABLE mysql.tbl",
                path.to_str().unwrap().to_owned()
            );

            match conn.query(query) {
                Ok(_) => {}
                Err(ref err) if format!("{}", err).find("not allowed").is_some() => {
                    let _ = fs::remove_file(&path);
                    return;
                }
                Err(err) => panic!("ERROR {}", err),
            }

            for (i, row) in conn.query("SELECT * FROM mysql.tbl").unwrap().enumerate() {
                let row = row.unwrap();
                match i {
                    0 => assert_eq!(row.unwrap(), vec![Bytes(b"AAAAAA".to_vec())]),
                    1 => assert_eq!(row.unwrap(), vec![Bytes(b"BBBBBB".to_vec())]),
                    2 => assert_eq!(row.unwrap(), vec![Bytes(b"CCCCCC".to_vec())]),
                    _ => unreachable!(),
                }
            }
            let _ = fs::remove_file(&path);
        }
        #[test]
        fn should_handle_LOCAL_INFILE_with_custom_handler() {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query("CREATE TEMPORARY TABLE mysql.tbl(a TEXT)")
                .unwrap();
            conn.set_local_infile_handler(Some(LocalInfileHandler::new(|_, stream| {
                let mut cell_data = vec![b'Z'; 65535];
                cell_data.push(b'\n');
                for _ in 0..1536 {
                    stream.write_all(&*cell_data)?;
                }
                Ok(())
            })));
            match conn.query("LOAD DATA LOCAL INFILE 'file_name' INTO TABLE mysql.tbl") {
                Ok(_) => {}
                Err(ref err) if format!("{}", err).find("not allowed").is_some() => {
                    return;
                }
                Err(err) => panic!("ERROR {}", err),
            }
            let count = conn
                .query("SELECT * FROM mysql.tbl")
                .unwrap()
                .map(|row| {
                    assert_eq!(from_row::<(Vec<u8>,)>(row.unwrap()).0.len(), 65535);
                    1
                })
                .sum::<usize>();
            assert_eq!(count, 1536);
        }

        #[test]
        fn should_reset_connection() {
            let mut conn = Conn::new(get_opts()).unwrap();
            assert!(conn
                .query(
                    "CREATE TEMPORARY TABLE `mysql`.`test` \
                     (`test` VARCHAR(255) NULL);"
                )
                .is_ok());
            assert!(conn.query("SELECT * FROM `mysql`.`test`;").is_ok());
            assert!(conn.reset().is_ok());
            assert!(conn.query("SELECT * FROM `mysql`.`test`;").is_err());
        }

        #[test]
        fn should_connect_via_socket_for_127_0_0_1() {
            #[allow(unused_mut)]
            let mut opts = OptsBuilder::from_opts(get_opts());
            let conn = Conn::new(opts).unwrap();
            if conn.is_insecure() {
                assert!(conn.is_socket());
            }
        }

        #[test]
        fn should_connect_via_socket_localhost() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.ip_or_hostname(Some("localhost"));
            let conn = Conn::new(opts).unwrap();
            if conn.is_insecure() {
                assert!(conn.is_socket());
            }
        }

        #[test]
        fn should_drop_multi_result_set() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.db_name(Some("mysql"));
            let mut conn = Conn::new(opts).unwrap();
            conn.query("CREATE TEMPORARY TABLE TEST_TABLE ( name varchar(255) )")
                .unwrap();
            conn.prep_exec("SELECT * FROM TEST_TABLE", ()).unwrap();
            conn.query(
                r"
                INSERT INTO TEST_TABLE (name) VALUES ('one');
                INSERT INTO TEST_TABLE (name) VALUES ('two');
                INSERT INTO TEST_TABLE (name) VALUES ('three');",
            )
            .unwrap();
            conn.prep_exec("SELECT * FROM TEST_TABLE", ()).unwrap();
        }

        #[test]
        fn should_handle_multi_resultset() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.db_name(Some("mysql"));
            let mut conn = Conn::new(opts).unwrap();
            conn.query("DROP PROCEDURE IF EXISTS multi").unwrap();
            conn.query(
                r#"CREATE PROCEDURE multi() BEGIN
                        SELECT 1 UNION ALL SELECT 2;
                        DO 1;
                        SELECT 3 UNION ALL SELECT 4;
                        DO 1;
                        DO 1;
                        SELECT REPEAT('A', 17000000);
                        SELECT REPEAT('A', 17000000);
                    END"#,
            )
            .unwrap();
            {
                let mut query_result = conn.query("CALL multi()").unwrap();
                let result_set = query_result
                    .by_ref()
                    .map(|row| row.unwrap().unwrap().pop().unwrap())
                    .collect::<Vec<crate::Value>>();
                assert_eq!(result_set, vec![Bytes(b"1".to_vec()), Bytes(b"2".to_vec())]);
                assert!(query_result.more_results_exists());
                let result_set = query_result
                    .by_ref()
                    .map(|row| row.unwrap().unwrap().pop().unwrap())
                    .collect::<Vec<crate::Value>>();
                assert_eq!(result_set, vec![Bytes(b"3".to_vec()), Bytes(b"4".to_vec())]);
            }
            let mut result = conn.query("SELECT 1; SELECT 2; SELECT 3;").unwrap();
            let mut i = 0;
            while {
                i += 1;
                result.more_results_exists()
            } {
                for row in result.by_ref() {
                    match i {
                        1 => assert_eq!(row.unwrap().unwrap(), vec![Bytes(b"1".to_vec())]),
                        2 => assert_eq!(row.unwrap().unwrap(), vec![Bytes(b"2".to_vec())]),
                        3 => assert_eq!(row.unwrap().unwrap(), vec![Bytes(b"3".to_vec())]),
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
                let mut result = stmt
                    .execute(params! {"a" => 1, "b" => 2, "c" => 3})
                    .unwrap();
                let row = result.next().unwrap().unwrap();
                assert_eq!((1, 2, 1, 3), from_row(row));
            }

            let mut result = conn
                .prep_exec(
                    "SELECT :a, :b, :a + :b, :c",
                    params! {
                        "a" => 1,
                        "b" => 2,
                        "c" => 3,
                    },
                )
                .unwrap();
            let row = result.next().unwrap().unwrap();
            assert_eq!((1, 2, 3, 3), from_row(row));
        }

        #[test]
        fn should_return_error_on_missing_named_parameter() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT :a, :b, :a, :c, :d").unwrap();
            let result = stmt.execute(params! {"a" => 1, "b" => 2, "c" => 3,});
            match result {
                Err(DriverError(MissingNamedParameter(ref x))) if x == "d" => (),
                _ => assert!(false),
            }
        }

        #[test]
        fn should_return_error_on_named_params_for_positional_statement() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
            let result = stmt.execute(params! {"a" => 1, "b" => 2, "c" => 3,});
            match result {
                Err(DriverError(NamedParamsForPositionalQuery)) => (),
                _ => assert!(false),
            }
        }

        #[test]
        #[should_panic]
        fn should_panic_on_named_param_redefinition() {
            let _: Params = params! {"a" => 1, "b" => 2, "a" => 3}.into();
        }

        #[test]
        fn should_handle_tcp_connect_timeout() {
            use crate::error::{DriverError::ConnectTimeout, Error::DriverError};

            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            assert!(Conn::new(opts).unwrap().ping());

            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            opts.ip_or_hostname(Some("192.168.255.255"));
            match Conn::new(opts).unwrap_err() {
                DriverError(ConnectTimeout) => {}
                err => panic!("Unexpected error: {}", err),
            }
        }

        #[test]
        fn should_set_additional_capabilities() {
            use crate::consts::CapabilityFlags;

            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.additional_capabilities(CapabilityFlags::CLIENT_FOUND_ROWS);

            let mut conn = Conn::new(opts).unwrap();
            conn.query("CREATE TEMPORARY TABLE mysql.tbl (a INT, b TEXT)")
                .unwrap();
            conn.query("INSERT INTO mysql.tbl (a, b) VALUES (1, 'foo')")
                .unwrap();
            let result = conn
                .query("UPDATE mysql.tbl SET b = 'foo' WHERE a = 1")
                .unwrap();
            assert_eq!(result.affected_rows(), 1);
        }

        #[test]
        fn should_bind_before_connect() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.ip_or_hostname(Some("127.0.0.1"));
            opts.bind_address(Some(([127, 0, 0, 1], 27272)));
            let conn = Conn::new(opts).unwrap();
            let debug_format: String = dbg!(format!("{:?}", conn));
            assert!(debug_format.contains("addr: V4(127.0.0.1:27272)"));
        }

        #[test]
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
            let row = conn
                .first("SHOW SESSION STATUS LIKE 'Com_stmt_close';")
                .unwrap()
                .unwrap();
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
            let row = conn
                .first("SHOW SESSION STATUS LIKE 'Com_stmt_close';")
                .unwrap()
                .unwrap();
            assert_eq!(from_row::<(String, usize)>(row).1, 3);
            let mut order = conn
                .stmt_cache
                .iter()
                .map(|(stmt, i)| (stmt.as_ref(), i))
                .collect::<Vec<(&str, u64)>>();
            order.sort();
            assert_eq!(order, &[("DO 3", 5), ("DO 5", 6), ("DO 6", 7)]);
        }

        #[test]
        fn should_handle_json_columns() {
            use crate::{Deserialized, Serialized};
            use serde_json::Value as Json;
            use std::str::FromStr;

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
            if conn
                .query("CREATE TEMPORARY TABLE mysql.tbl(a VARCHAR(32), b JSON)")
                .is_err()
            {
                conn.query("CREATE TEMPORARY TABLE mysql.tbl(a VARCHAR(32), b TEXT)")
                    .unwrap();
            }
            conn.prep_exec(
                r#"INSERT INTO mysql.tbl VALUES ('hello', ?)"#,
                (Serialized(&decodable),),
            )
            .unwrap();

            let row = conn.first("SELECT a, b FROM mysql.tbl").unwrap().unwrap();
            let (a, b): (String, Json) = from_row(row);
            assert_eq!(
                (a, b),
                (
                    "hello".into(),
                    Json::from_str(r#"{"foo": "bar", "quux": [42, "hello"]}"#).unwrap()
                )
            );

            let row = conn
                .first_exec("SELECT a, b FROM mysql.tbl WHERE a = ?", ("hello",))
                .unwrap()
                .unwrap();
            let (a, Deserialized(b)) = from_row(row);
            assert_eq!((a, b), (String::from("hello"), decodable));
        }

        #[test]
        fn should_set_connect_attrs() {
            let opts = OptsBuilder::from_opts(get_opts());
            let mut conn = Conn::new(opts).unwrap();

            let support_connect_attrs = match (conn.server_version, conn.mariadb_server_version) {
                (Some(ref version), _) if *version >= (5, 6, 0) => true,
                (_, Some(ref version)) if *version >= (10, 0, 0) => true,
                _ => false,
            };

            if support_connect_attrs {
                // MySQL >= 5.6 or MariaDB >= 10.0

                if get_system_variable::<String>(&mut conn, "performance_schema") != "ON" {
                    panic!("The system variable `performance_schema` is off. Restart the MySQL server with `--performance_schema=on` to pass the test.");
                }
                let attrs_size: i32 =
                    get_system_variable(&mut conn, "performance_schema_session_connect_attrs_size");
                if attrs_size >= 0 && attrs_size <= 128 {
                    panic!("The system variable `performance_schema_session_connect_attrs_size` is {}. Restart the MySQL server with `--performance_schema_session_connect_attrs_size=-1` to pass the test.", attrs_size);
                }

                fn assert_connect_attrs(conn: &mut Conn, expected_values: &[(&str, &str)]) {
                    let mut actual_values = HashMap::new();
                    for row in conn.query("SELECT attr_name, attr_value FROM performance_schema.session_account_connect_attrs WHERE processlist_id = connection_id()").unwrap() {
                        let (name, value) = from_row::<(String, String)>(row.unwrap());
                        actual_values.insert(name, value);
                    }

                    for (name, value) in expected_values {
                        assert_eq!(
                            actual_values.get(&name.to_string()),
                            Some(&value.to_string())
                        );
                    }
                }

                let pid = process::id().to_string();
                let progname = std::env::args_os()
                    .next()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                let mut expected_values = vec![
                    ("_client_name", "rust-mysql-simple"),
                    ("_client_version", env!("CARGO_PKG_VERSION")),
                    ("_os", env!("CARGO_CFG_TARGET_OS")),
                    ("_pid", &pid),
                    ("_platform", env!("CARGO_CFG_TARGET_ARCH")),
                    ("program_name", &progname),
                ];

                // No connect attributes are added.
                assert_connect_attrs(&mut conn, &expected_values);

                // Connect attributes are added.
                let mut opts = OptsBuilder::from_opts(get_opts());
                let mut connect_attrs = HashMap::with_capacity(3);
                connect_attrs.insert("foo", "foo val");
                connect_attrs.insert("bar", "bar val");
                connect_attrs.insert("program_name", "my program name");
                opts.connect_attrs(connect_attrs);
                let mut conn = Conn::new(opts).unwrap();
                expected_values.pop(); // remove program_name at the last
                expected_values.push(("foo", "foo val"));
                expected_values.push(("bar", "bar val"));
                expected_values.push(("program_name", "my program name"));
                assert_connect_attrs(&mut conn, &expected_values);
            }
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;

        use crate::test_misc::get_opts;
        use crate::{params, Conn, Value::NULL};

        #[bench]
        fn simple_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| {
                let _ = conn.query("DO 1");
            })
        }

        #[bench]
        fn prepared_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("DO 1").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(());
            })
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
            bencher.iter(|| {
                let _ = conn.query("SELECT 1");
            })
        }

        #[bench]
        fn simple_prepared_query_row(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT 1").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(());
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_param(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute((0,));
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_named_param(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT :a").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(params! {"a" => 0});
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_5_params(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
            let params = (42i8, b"123456".to_vec(), 1.618f64, NULL, 1i8);
            bencher.iter(|| {
                let _ = stmt.execute(&params);
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_5_named_params(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn
                .prepare("SELECT :one, :two, :three, :four, :five")
                .unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(params! {
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
            bencher.iter(|| {
                let _ = conn.query("SELECT REPEAT('A', 10000)");
            })
        }

        #[bench]
        fn select_prepared_large_string(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let mut stmt = conn.prepare("SELECT REPEAT('A', 10000)").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(());
            })
        }

        #[bench]
        fn many_small_rows(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query("CREATE TEMPORARY TABLE mysql.x (id INT)")
                .unwrap();
            for _ in 0..512 {
                conn.query("INSERT INTO mysql.x VALUES (256)").unwrap();
            }
            let mut stmt = conn.prepare("SELECT * FROM mysql.x").unwrap();
            bencher.iter(|| {
                let _ = stmt.execute(());
            });
        }
    }
}
