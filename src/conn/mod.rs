use mysql_common::crypto;
use mysql_common::io::ReadMysqlExt;
use mysql_common::named_params::parse_named_params;
use mysql_common::packets::{
    column_from_payload, parse_auth_switch_request, parse_err_packet, parse_handshake_packet,
    parse_ok_packet, AuthPlugin, AuthSwitchRequest, Column, ComStmtClose,
    ComStmtExecuteRequestBuilder, ComStmtSendLongData, HandshakePacket, HandshakeResponse,
    OkPacket, SslRequest,
};
use mysql_common::proto::{codec::Compression, sync_framed::MySyncFramed};
use mysql_common::value::{read_bin_values, read_text_values, ServerSide};

use std::{
    borrow::Borrow,
    cmp,
    collections::HashMap,
    io::{self, Read, Write as _},
    mem,
    ops::{Deref, DerefMut},
    process,
    sync::Arc,
};

use crate::conn::local_infile::LocalInfile;
use crate::conn::stmt::{InnerStmt, Statement};
use crate::conn::stmt_cache::StmtCache;
use crate::conn::transaction::IsolationLevel;
use crate::consts::{CapabilityFlags, Command, StatusFlags, MAX_PAYLOAD_LEN};
use crate::io::Stream;
use crate::prelude::*;
use crate::DriverError::{
    MismatchedStmtParams, NamedParamsForPositionalQuery, Protocol41NotSet,
    ReadOnlyTransNotSupported, SetupError, TlsNotSupported, UnexpectedPacket, UnknownAuthPlugin,
    UnsupportedProtocol,
};
use crate::Error::{DriverError, MySqlError};
use crate::Value::{self, Bytes, NULL};
use crate::{
    from_value, from_value_opt, LocalInfileHandler, Opts, OptsBuilder, Params, QueryResult,
    Result as MyResult, SslOpts, Transaction,
};

pub mod local_infile;
pub mod opts;
pub mod pool;
pub mod query_result;
pub mod stmt;
mod stmt_cache;
pub mod transaction;

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
    /// Returns connection identifier.
    pub fn connection_id(&self) -> u32 {
        self.connection_id
    }

    /// Returns number of rows affected by the last query.
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Returns last insert id of the last query.
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

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
    fn can_improved(&mut self) -> MyResult<Option<Opts>> {
        if self.opts.get_prefer_socket() && self.opts.addr_is_loopback() {
            let mut socket = None;
            #[cfg(test)]
            {
                socket = self.opts.0.injected_socket.clone();
            }
            if socket.is_none() {
                socket = self.get_system_var("socket")?.map(from_value::<String>);
            }
            if let Some(socket) = socket {
                if self.opts.get_socket().is_none() {
                    let mut socket_opts = OptsBuilder::from_opts(self.opts.clone());
                    if !socket.is_empty() {
                        socket_opts.socket(Some(socket));
                        return Ok(Some(socket_opts.into()));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Creates new `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> MyResult<Conn> {
        let mut conn = Conn::empty(opts);
        conn.connect_stream()?;
        conn.connect()?;
        let mut conn = {
            if let Some(new_opts) = conn.can_improved()? {
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
            conn.query_drop(cmd)?;
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
        let stream = stream.make_secure(self.opts.get_host(), ssl_opts)?;
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
        } else {
            let port = self.opts.get_tcp_port();
            let ip_or_hostname = match self.opts.get_host() {
                url::Host::Domain(domain) => domain,
                url::Host::Ipv4(ip) => ip.to_string(),
                url::Host::Ipv6(ip) => ip.to_string(),
            };
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

    fn send_long_data(&mut self, stmt_id: u32, params: &[Value]) -> MyResult<()> {
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
                        let com = ComStmtSendLongData::new(stmt_id, i, chunk);
                        self.write_command_raw(com)?;
                    }
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn _execute(&mut self, stmt: &Statement, params: Params) -> MyResult<Vec<Column>> {
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
                    self.send_long_data(stmt.id(), &*params)?;
                }

                body
            }
            Params::Named(_) => {
                if stmt.named_params.is_none() {
                    return Err(DriverError(NamedParamsForPositionalQuery));
                }
                let named_params = stmt.named_params.as_ref().unwrap();
                return self._execute(stmt, params.into_positional(named_params)?);
            }
        };
        self.write_command_raw(exec_request)?;
        self.handle_result_set()
    }

    fn _start_transaction(
        &mut self,
        consistent_snapshot: bool,
        isolation_level: Option<IsolationLevel>,
        readonly: Option<bool>,
    ) -> MyResult<()> {
        if let Some(i_level) = isolation_level {
            self.query_drop(format!("SET TRANSACTION ISOLATION LEVEL {}", i_level))?;
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
            if readonly {
                self.query_drop("SET TRANSACTION READ ONLY")?;
            } else {
                self.query_drop("SET TRANSACTION READ WRITE")?;
            }
        }
        if consistent_snapshot {
            self.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")?;
        } else {
            self.query_drop("START TRANSACTION")?;
        };
        Ok(())
    }

    fn send_local_infile(&mut self, file_name: &[u8]) -> MyResult<()> {
        {
            let buffer_size = cmp::min(
                MAX_PAYLOAD_LEN - 4,
                self.stream_ref().codec().max_allowed_packet - 4,
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
            }
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

    fn _true_prepare(&mut self, query: &str) -> MyResult<InnerStmt> {
        self.write_command(Command::COM_STMT_PREPARE, query.as_bytes())?;
        let pld = self.read_packet()?;
        let mut stmt = InnerStmt::from_payload(pld.as_ref(), self.connection_id())?;
        if stmt.num_params() > 0 {
            let mut params: Vec<Column> = Vec::with_capacity(stmt.num_params() as usize);
            for _ in 0..stmt.num_params() {
                let pld = self.read_packet()?;
                params.push(column_from_payload(pld)?);
            }
            stmt = stmt.with_params(Some(params));
            self.read_packet()?;
        }
        if stmt.num_columns() > 0 {
            let mut columns: Vec<Column> = Vec::with_capacity(stmt.num_columns() as usize);
            for _ in 0..stmt.num_columns() {
                let pld = self.read_packet()?;
                columns.push(column_from_payload(pld)?);
            }
            stmt = stmt.with_columns(Some(columns));
            self.read_packet()?;
        }
        Ok(stmt)
    }

    fn _prepare(&mut self, query: &str) -> MyResult<Arc<InnerStmt>> {
        if let Some(entry) = self.stmt_cache.by_query(query) {
            return Ok(entry.stmt.clone());
        }

        let inner_st = Arc::new(self._true_prepare(query)?);

        if let Some(old_stmt) = self
            .stmt_cache
            .put(Arc::new(query.into()), inner_st.clone())
        {
            self.close(Statement::new(old_stmt, None))?;
        }

        Ok(inner_st)
    }

    fn connect(&mut self) -> MyResult<()> {
        if self.connected {
            return Ok(());
        }
        self.do_handshake()
            .and_then(|_| {
                Ok(from_value_opt::<usize>(
                    self.get_system_var("max_allowed_packet")?.unwrap_or(NULL),
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

    fn get_system_var(&mut self, name: &str) -> MyResult<Option<Value>> {
        self.query_first(format!("SELECT @@{}", name))
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
        self.stmt_cache.contains_query(query)
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

impl crate::prelude::Queryable for Conn {
    fn query_iter<T: AsRef<str>>(&mut self, query: T) -> MyResult<QueryResult<'_>> {
        match self._query(query.as_ref()) {
            Ok(columns) => Ok(QueryResult::new(self, columns, false)),
            Err(err) => Err(err),
        }
    }

    fn prep<T: AsRef<str>>(&mut self, query: T) -> MyResult<Statement> {
        let query = query.as_ref();
        let (named_params, real_query) = parse_named_params(query)?;
        self._prepare(real_query.borrow())
            .map(|inner| Statement::new(inner, named_params))
    }

    fn close(&mut self, stmt: Statement) -> MyResult<()> {
        self.stmt_cache.remove(stmt.id());
        let com_stmt_close = ComStmtClose::new(stmt.id());
        self.write_command_raw(com_stmt_close)?;
        Ok(())
    }

    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> MyResult<QueryResult<'_>>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        let statement = stmt.as_statement(self)?;
        let columns = self._execute(&*statement, params.into())?;
        Ok(QueryResult::new(self, columns, true))
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        let stmt_cache = mem::replace(&mut self.stmt_cache, StmtCache::new(0));

        for (_, entry) in stmt_cache.into_iter() {
            let _ = self.close(Statement::new(entry.stmt, None));
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
        use std::{collections::HashMap, io::Write, iter, process};

        use crate::{
            from_row, from_value, params,
            prelude::*,
            test_misc::get_opts,
            time::Timespec,
            Conn,
            DriverError::{MissingNamedParameter, NamedParamsForPositionalQuery},
            Error::DriverError,
            LocalInfileHandler, Opts, OptsBuilder, Params,
            Value::{self, Bytes, Date, Float, Int, NULL},
        };

        fn get_system_variable<T>(conn: &mut Conn, name: &str) -> T
        where
            T: FromValue,
        {
            conn.query_first::<_, (String, T)>(format!("show variables like '{}'", name))
                .unwrap()
                .unwrap()
                .1
        }

        #[test]
        fn should_connect() {
            let mut conn = Conn::new(get_opts()).unwrap();

            let mode: String = conn
                .query_first("SELECT @@GLOBAL.sql_mode")
                .unwrap()
                .unwrap();
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
            const DB_NAME: &str = "mysql";

            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.db_name(Some(DB_NAME));

            let mut conn = Conn::new(opts).unwrap();

            let db_name: String = conn.query_first("SELECT DATABASE()").unwrap().unwrap();
            assert_eq!(db_name, DB_NAME);
        }

        #[test]
        fn should_connect_by_hostname() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.ip_or_hostname(Some("localhost"));
            let mut conn = Conn::new(opts).unwrap();
            assert!(conn.ping());
        }

        #[test]
        fn should_select_db() {
            const DB_NAME: &str = "t_select_db";

            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {}", DB_NAME))
                .unwrap();
            assert!(conn.select_db(DB_NAME));

            let db_name: String = conn.query_first("SELECT DATABASE()").unwrap().unwrap();
            assert_eq!(db_name, DB_NAME);

            conn.query_drop(format!("DROP DATABASE {}", DB_NAME))
                .unwrap();
        }

        #[test]
        fn should_execute_queryes_and_parse_results() {
            type TestRow = (String, String, String, String, String, String);

            const CREATE_QUERY: &str = r"CREATE TEMPORARY TABLE mysql.tbl
                (id SERIAL, a TEXT, b INT, c INT UNSIGNED, d DATE, e FLOAT)";
            const INSERT_QUERY_1: &str = r"INSERT
                INTO mysql.tbl(a, b, c, d, e)
                VALUES ('hello', -123, 123, '2014-05-05', 123.123)";
            const INSERT_QUERY_2: &str = r"INSERT
                INTO mysql.tbl(a, b, c, d, e)
                VALUES ('world', -321, 321, '2014-06-06', 321.321)";

            let mut conn = Conn::new(get_opts()).unwrap();

            conn.query_drop(CREATE_QUERY).unwrap();
            assert_eq!(conn.affected_rows(), 0);
            assert_eq!(conn.last_insert_id(), 0);

            conn.query_drop(INSERT_QUERY_1).unwrap();
            assert_eq!(conn.affected_rows(), 1);
            assert_eq!(conn.last_insert_id(), 1);

            conn.query_drop(INSERT_QUERY_2).unwrap();
            assert_eq!(conn.affected_rows(), 1);
            assert_eq!(conn.last_insert_id(), 2);

            conn.query_drop("SELECT * FROM unexisted").unwrap_err();
            conn.query_iter("SELECT * FROM mysql.tbl").unwrap(); // Drop::drop for QueryResult

            conn.query_drop("UPDATE mysql.tbl SET a = 'foo'").unwrap();
            assert_eq!(conn.affected_rows(), 2);
            assert_eq!(conn.last_insert_id(), 0);

            assert!(conn
                .query_first::<_, TestRow>("SELECT * FROM mysql.tbl WHERE a = 'bar'")
                .unwrap()
                .is_none());

            let rows: Vec<TestRow> = conn.query("SELECT * FROM mysql.tbl").unwrap();
            assert_eq!(
                rows,
                vec![
                    (
                        "1".into(),
                        "foo".into(),
                        "-123".into(),
                        "123".into(),
                        "2014-05-05".into(),
                        "123.123".into()
                    ),
                    (
                        "2".into(),
                        "foo".into(),
                        "-321".into(),
                        "321".into(),
                        "2014-06-06".into(),
                        "321.321".into()
                    )
                ]
            );
        }

        #[test]
        fn should_parse_large_text_result() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let value: Value = conn
                .query_first("SELECT REPEAT('A', 20000000)")
                .unwrap()
                .unwrap();
            assert_eq!(value, Bytes(iter::repeat(b'A').take(20_000_000).collect()));
        }

        #[test]
        fn should_execute_statements_and_parse_results() {
            const CREATE_QUERY: &str = r"CREATE TEMPORARY TABLE
                mysql.tbl (a TEXT, b INT, c INT UNSIGNED, d DATE, e DOUBLE)";
            const INSERT_SMTM: &str = r"INSERT
                INTO mysql.tbl (a, b, c, d, e)
                VALUES (?, ?, ?, ?, ?)";

            type RowType = (Value, Value, Value, Value, Value);

            let row1 = (
                Bytes(b"hello".to_vec()),
                Int(-123_i64),
                Int(123_i64),
                Date(2014_u16, 5_u8, 5_u8, 0_u8, 0_u8, 0_u8, 0_u32),
                Float(123.123_f64),
            );
            let row2 = (Bytes(b"".to_vec()), NULL, NULL, NULL, Float(321.321_f64));

            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query_drop(CREATE_QUERY).unwrap();

            let insert_stmt = conn.prep(INSERT_SMTM).unwrap();
            assert_eq!(insert_stmt.connection_id(), conn.connection_id());
            conn.exec_drop(
                &insert_stmt,
                (
                    from_value::<String>(row1.0.clone()),
                    from_value::<i32>(row1.1.clone()),
                    from_value::<u32>(row1.2.clone()),
                    from_value::<Timespec>(row1.3.clone()),
                    from_value::<f64>(row1.4.clone()),
                ),
            )
            .unwrap();
            conn.exec_drop(
                &insert_stmt,
                (
                    from_value::<String>(row2.0.clone()),
                    row2.1.clone(),
                    row2.2.clone(),
                    row2.3.clone(),
                    from_value::<f64>(row2.4.clone()),
                ),
            )
            .unwrap();

            let select_stmt = conn.prep("SELECT * from mysql.tbl").unwrap();
            let rows: Vec<RowType> = conn.exec(&select_stmt, ()).unwrap();

            assert_eq!(rows, vec![row1, row2]);
        }

        #[test]
        fn should_parse_large_binary_result() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT REPEAT('A', 20000000)").unwrap();
            let value: Value = conn.exec_first(&stmt, ()).unwrap().unwrap();
            assert_eq!(value, Bytes(iter::repeat(b'A').take(20_000_000).collect()));
        }

        #[test]
        fn manually_closed_stmt() {
            let mut opts = OptsBuilder::from(get_opts());
            opts.stmt_cache_size(1);
            let mut conn = Conn::new(opts).unwrap();
            let stmt = conn.prep("SELECT 1").unwrap();
            conn.exec_drop(&stmt, ()).unwrap();
            conn.close(stmt).unwrap();
            let stmt = conn.prep("SELECT 1").unwrap();
            conn.exec_drop(&stmt, ()).unwrap();
            conn.close(stmt).unwrap();
            let stmt = conn.prep("SELECT 2").unwrap();
            conn.exec_drop(&stmt, ()).unwrap();
        }

        #[test]
        fn should_start_commit_and_rollback_transactions() {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query_drop("CREATE TEMPORARY TABLE mysql.tbl(a INT)")
                .unwrap();
            let _ = conn
                .start_transaction(false, None, None)
                .and_then(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.commit().unwrap();
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                conn.query_iter("SELECT COUNT(a) from mysql.tbl")
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
                    t.query_drop("INSERT INTO tbl2(a) VALUES(1)").unwrap_err();
                    Ok(())
                    // implicit rollback
                })
                .unwrap();
            assert_eq!(
                conn.query_iter("SELECT COUNT(a) from mysql.tbl")
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
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.rollback().unwrap();
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                conn.query_iter("SELECT COUNT(a) from mysql.tbl")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"2".to_vec())]
            );
            let mut tx = conn.start_transaction(false, None, None).unwrap();
            tx.exec_drop("INSERT INTO mysql.tbl(a) VALUES(?)", (3,))
                .unwrap();
            tx.exec_drop("INSERT INTO mysql.tbl(a) VALUES(?)", (4,))
                .unwrap();
            tx.commit().unwrap();
            assert_eq!(
                conn.query_iter("SELECT COUNT(a) from mysql.tbl")
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![Bytes(b"4".to_vec())]
            );
            let mut tx = conn.start_transaction(false, None, None).unwrap();
            tx.exec_drop("INSERT INTO mysql.tbl(a) VALUES(?)", (5,))
                .unwrap();
            tx.exec_drop("INSERT INTO mysql.tbl(a) VALUES(?)", (6,))
                .unwrap();
            drop(tx);
            assert_eq!(
                conn.query_first("SELECT COUNT(a) from mysql.tbl").unwrap(),
                Some(4_usize),
            );
        }
        #[test]
        fn should_handle_LOCAL_INFILE_with_custom_handler() {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query_drop("CREATE TEMPORARY TABLE mysql.tbl(a TEXT)")
                .unwrap();
            conn.set_local_infile_handler(Some(LocalInfileHandler::new(|_, stream| {
                let mut cell_data = vec![b'Z'; 65535];
                cell_data.push(b'\n');
                for _ in 0..1536 {
                    stream.write_all(&*cell_data)?;
                }
                Ok(())
            })));
            match conn.query_drop("LOAD DATA LOCAL INFILE 'file_name' INTO TABLE mysql.tbl") {
                Ok(_) => {}
                Err(ref err) if format!("{}", err).find("not allowed").is_some() => {
                    return;
                }
                Err(err) => panic!("ERROR {}", err),
            }
            let count = conn
                .query_iter("SELECT * FROM mysql.tbl")
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
            conn.query_drop(
                "CREATE TEMPORARY TABLE `mysql`.`test` \
                 (`test` VARCHAR(255) NULL);",
            )
            .unwrap();
            conn.query_drop("SELECT * FROM `mysql`.`test`;").unwrap();
            conn.reset().unwrap();
            conn.query_drop("SELECT * FROM `mysql`.`test`;")
                .unwrap_err();
        }

        #[test]
        fn prep_exec() {
            let mut conn = Conn::new(get_opts()).unwrap();

            let stmt1 = conn.prep("SELECT :foo").unwrap();
            let stmt2 = conn.prep("SELECT :bar").unwrap();
            assert_eq!(
                conn.exec::<_, _, String>(&stmt1, params! { "foo" => "foo" })
                    .unwrap(),
                vec!["foo".to_string()],
            );
            assert_eq!(
                conn.exec::<_, _, String>(&stmt2, params! { "bar" => "bar" })
                    .unwrap(),
                vec!["bar".to_string()],
            );
        }

        #[test]
        fn should_connect_via_socket_for_127_0_0_1() {
            let opts = OptsBuilder::from_opts(get_opts());
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
            conn.query_drop("CREATE TEMPORARY TABLE TEST_TABLE ( name varchar(255) )")
                .unwrap();
            conn.exec_drop("SELECT * FROM TEST_TABLE", ()).unwrap();
            conn.query_drop(
                r"
                INSERT INTO TEST_TABLE (name) VALUES ('one');
                INSERT INTO TEST_TABLE (name) VALUES ('two');
                INSERT INTO TEST_TABLE (name) VALUES ('three');",
            )
            .unwrap();
            conn.exec_drop("SELECT * FROM TEST_TABLE", ()).unwrap();
        }

        #[test]
        fn should_handle_multi_resultset() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.db_name(Some("mysql"));
            let mut conn = Conn::new(opts).unwrap();
            conn.query_drop("DROP PROCEDURE IF EXISTS multi").unwrap();
            conn.query_drop(
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
                let mut query_result = conn.query_iter("CALL multi()").unwrap();
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
            let mut result = conn.query_iter("SELECT 1; SELECT 2; SELECT 3;").unwrap();
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
                let stmt = conn.prep("SELECT :a, :b, :a, :c").unwrap();
                let result = conn
                    .exec_first(&stmt, params! {"a" => 1, "b" => 2, "c" => 3})
                    .unwrap()
                    .unwrap();
                assert_eq!((1_u8, 2_u8, 1_u8, 3_u8), result);
            }

            let result = conn
                .exec_first(
                    "SELECT :a, :b, :a + :b, :c",
                    params! {
                        "a" => 1,
                        "b" => 2,
                        "c" => 3,
                    },
                )
                .unwrap()
                .unwrap();
            assert_eq!((1_u8, 2_u8, 3_u8, 3_u8), result);
        }

        #[test]
        fn should_return_error_on_missing_named_parameter() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT :a, :b, :a, :c, :d").unwrap();
            let result =
                conn.exec_first::<_, _, crate::Row>(&stmt, params! {"a" => 1, "b" => 2, "c" => 3,});
            match result {
                Err(DriverError(MissingNamedParameter(ref x))) if x == "d" => (),
                _ => assert!(false),
            }
        }

        #[test]
        fn should_return_error_on_named_params_for_positional_statement() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT ?, ?, ?, ?, ?").unwrap();
            let result = conn.exec_drop(&stmt, params! {"a" => 1, "b" => 2, "c" => 3,});
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
            conn.query_drop("CREATE TEMPORARY TABLE mysql.tbl (a INT, b TEXT)")
                .unwrap();
            conn.query_drop("INSERT INTO mysql.tbl (a, b) VALUES (1, 'foo')")
                .unwrap();
            let result = conn
                .query_iter("UPDATE mysql.tbl SET b = 'foo' WHERE a = 1")
                .unwrap();
            assert_eq!(result.affected_rows(), 1);
        }

        #[test]
        fn should_bind_before_connect() {
            let port = 27200 + (rand::random::<u16>() % 100);
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.ip_or_hostname(Some("127.0.0.1"));
            opts.bind_address(Some(([127, 0, 0, 1], port)));
            let conn = Conn::new(opts).unwrap();
            let debug_format: String = format!("{:?}", conn);
            assert!(debug_format.contains(&*format!("addr: V4(127.0.0.1:{})", port)));
        }

        #[test]
        fn should_bind_before_connect_with_timeout() {
            let port = 27300 + (rand::random::<u16>() % 100);
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.prefer_socket(false);
            opts.ip_or_hostname(Some("127.0.0.1"));
            opts.bind_address(Some(([127, 0, 0, 1], port)));
            opts.tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            let mut conn = Conn::new(opts).unwrap();
            assert!(conn.ping());
            let debug_format: String = format!("{:?}", conn);
            assert!(debug_format.contains(&*format!("addr: V4(127.0.0.1:{})", port)));
        }

        #[test]
        fn should_not_cache_statements_if_stmt_cache_size_is_zero() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.stmt_cache_size(0);
            let mut conn = Conn::new(opts).unwrap();

            let stmt1 = conn.prep("DO 1").unwrap();
            let stmt2 = conn.prep("DO 2").unwrap();
            let stmt3 = conn.prep("DO 3").unwrap();

            conn.close(stmt1).unwrap();
            conn.close(stmt2).unwrap();
            conn.close(stmt3).unwrap();

            let status: (Value, u8) = conn
                .query_first("SHOW SESSION STATUS LIKE 'Com_stmt_close';")
                .unwrap()
                .unwrap();
            assert_eq!(status.1, 3);
        }

        #[test]
        fn should_hold_stmt_cache_size_bounds() {
            let mut opts = OptsBuilder::from_opts(get_opts());
            opts.stmt_cache_size(3);

            let mut conn = Conn::new(opts).unwrap();

            conn.prep("DO 1").unwrap();
            conn.prep("DO 2").unwrap();
            conn.prep("DO 3").unwrap();
            conn.prep("DO 1").unwrap();
            conn.prep("DO 4").unwrap();
            conn.prep("DO 3").unwrap();
            conn.prep("DO 5").unwrap();
            conn.prep("DO 6").unwrap();

            let status: (String, usize) = conn
                .query_first("SHOW SESSION STATUS LIKE 'Com_stmt_close'")
                .unwrap()
                .unwrap();

            assert_eq!(status.1, 3);

            let mut order = conn
                .stmt_cache
                .iter()
                .map(|(_, entry)| &**entry.query.0.as_ref())
                .collect::<Vec<&str>>();
            order.sort();
            assert_eq!(order, &["DO 3", "DO 5", "DO 6"]);
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
                .query_drop("CREATE TEMPORARY TABLE mysql.tbl(a VARCHAR(32), b JSON)")
                .is_err()
            {
                conn.query_drop("CREATE TEMPORARY TABLE mysql.tbl(a VARCHAR(32), b TEXT)")
                    .unwrap();
            }
            conn.exec_drop(
                r#"INSERT INTO mysql.tbl VALUES ('hello', ?)"#,
                (Serialized(&decodable),),
            )
            .unwrap();

            let (a, b): (String, Json) = conn
                .query_first("SELECT a, b FROM mysql.tbl")
                .unwrap()
                .unwrap();
            assert_eq!(
                (a, b),
                (
                    "hello".into(),
                    Json::from_str(r#"{"foo": "bar", "quux": [42, "hello"]}"#).unwrap()
                )
            );

            let row = conn
                .exec_first("SELECT a, b FROM mysql.tbl WHERE a = ?", ("hello",))
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
                    for row in conn.query_iter("SELECT attr_name, attr_value FROM performance_schema.session_account_connect_attrs WHERE processlist_id = connection_id()").unwrap() {
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
