// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::{Buf, BufMut};
#[cfg(feature = "binlog")]
use mysql_common::packets::binlog_request::BinlogRequest;
use mysql_common::{
    constants::UTF8MB4_GENERAL_CI,
    crypto,
    io::{ParseBuf, ReadMysqlExt},
    named_params::ParsedNamedParams,
    packets::{
        AuthPlugin, AuthSwitchRequest, Column, ComChangeUser, ComChangeUserMoreData, ComStmtClose,
        ComStmtExecuteRequestBuilder, ComStmtSendLongData, CommonOkPacket, ErrPacket,
        HandshakePacket, HandshakeResponse, OkPacket, OkPacketDeserializer, OkPacketKind,
        OldAuthSwitchRequest, OldEofPacket, ResultSetTerminator, SessionStateInfo,
    },
    proto::{codec::Compression, sync_framed::MySyncFramed, MySerialize},
};

use mysql_common::{
    constants::{DEFAULT_MAX_ALLOWED_PACKET, UTF8_GENERAL_CI},
    packets::SslRequest,
};

use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    convert::TryFrom,
    io::{self, Write as _},
    mem,
    ops::{Deref, DerefMut},
    process,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};

use crate::{
    buffer_pool::{get_buffer, Buffer},
    conn::{
        local_infile::LocalInfile,
        pool::{Pool, PooledConn},
        query_result::{Binary, Or, Text},
        stmt::{InnerStmt, Statement},
        stmt_cache::StmtCache,
        transaction::{AccessMode, TxOpts},
    },
    consts::{CapabilityFlags, Command, StatusFlags, MAX_PAYLOAD_LEN},
    from_value, from_value_opt,
    io::Stream,
    prelude::*,
    ChangeUserOpts,
    DriverError::{
        CleartextPluginDisabled, MismatchedStmtParams, NamedParamsForPositionalQuery,
        OldMysqlPasswordDisabled, Protocol41NotSet, ReadOnlyTransNotSupported, SetupError,
        UnexpectedPacket, UnknownAuthPlugin, UnsupportedProtocol,
    },
    Error::{self, DriverError, MySqlError},
    LocalInfileHandler, Opts, OptsBuilder, Params, QueryResult, Result, Transaction,
    Value::{self, Bytes, NULL},
};

use crate::DriverError::TlsNotSupported;
use crate::SslOpts;

#[cfg(feature = "binlog")]
use self::binlog_stream::BinlogStream;

#[cfg(feature = "binlog")]
pub mod binlog_stream;
pub mod local_infile;
pub mod opts;
pub mod pool;
pub mod query;
pub mod query_result;
pub mod queryable;
pub mod stmt;
mod stmt_cache;
pub mod transaction;

/// Mutable connection.
#[derive(Debug)]
pub enum ConnMut<'c, 't, 'tc> {
    Mut(&'c mut Conn),
    TxMut(&'t mut Transaction<'tc>),
    Owned(Conn),
    Pooled(PooledConn),
}

impl From<Conn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: Conn) -> Self {
        ConnMut::Owned(conn)
    }
}

impl From<PooledConn> for ConnMut<'static, 'static, 'static> {
    fn from(conn: PooledConn) -> Self {
        ConnMut::Pooled(conn)
    }
}

impl<'a> From<&'a mut Conn> for ConnMut<'a, 'static, 'static> {
    fn from(conn: &'a mut Conn) -> Self {
        ConnMut::Mut(conn)
    }
}

impl<'a> From<&'a mut PooledConn> for ConnMut<'a, 'static, 'static> {
    fn from(conn: &'a mut PooledConn) -> Self {
        ConnMut::Mut(conn.as_mut())
    }
}

impl<'t, 'tc> From<&'t mut Transaction<'tc>> for ConnMut<'static, 't, 'tc> {
    fn from(tx: &'t mut Transaction<'tc>) -> Self {
        ConnMut::TxMut(tx)
    }
}

impl TryFrom<&Pool> for ConnMut<'static, 'static, 'static> {
    type Error = Error;

    fn try_from(pool: &Pool) -> Result<Self> {
        pool.get_conn().map(From::from)
    }
}

impl Deref for ConnMut<'_, '_, '_> {
    type Target = Conn;

    fn deref(&self) -> &Conn {
        match self {
            ConnMut::Mut(conn) => conn,
            ConnMut::TxMut(tx) => &tx.conn,
            ConnMut::Owned(conn) => conn,
            ConnMut::Pooled(conn) => conn.as_ref(),
        }
    }
}

impl DerefMut for ConnMut<'_, '_, '_> {
    fn deref_mut(&mut self) -> &mut Conn {
        match self {
            ConnMut::Mut(conn) => conn,
            ConnMut::TxMut(tx) => &mut tx.conn,
            ConnMut::Owned(ref mut conn) => conn,
            ConnMut::Pooled(ref mut conn) => conn.as_mut(),
        }
    }
}

/// Connection internals.
#[derive(Debug)]
struct ConnInner {
    opts: Opts,
    stream: Option<MySyncFramed<Stream>>,
    stmt_cache: StmtCache,

    // TODO: clean this up
    server_version: Option<(u16, u16, u16)>,
    mariadb_server_version: Option<(u16, u16, u16)>,

    /// Last Ok packet, if any.
    ok_packet: Option<OkPacket<'static>>,
    capability_flags: CapabilityFlags,
    connection_id: u32,
    status_flags: StatusFlags,
    character_set: u8,
    last_command: u8,
    connected: bool,
    has_results: bool,
    local_infile_handler: Option<LocalInfileHandler>,

    auth_plugin: AuthPlugin<'static>,
    nonce: Vec<u8>,

    /// This flag is to opt-in/opt-out from reset upon return to a pool.
    pub(crate) reset_upon_return: bool,
}

impl ConnInner {
    fn empty(opts: Opts) -> Self {
        ConnInner {
            stmt_cache: StmtCache::new(opts.get_stmt_cache_size()),
            stream: None,
            capability_flags: CapabilityFlags::empty(),
            status_flags: StatusFlags::empty(),
            connection_id: 0u32,
            character_set: 0u8,
            ok_packet: None,
            last_command: 0u8,
            connected: false,
            has_results: false,
            server_version: None,
            mariadb_server_version: None,
            local_infile_handler: None,
            auth_plugin: AuthPlugin::MysqlNativePassword,
            nonce: Vec::new(),
            reset_upon_return: opts.get_pool_opts().reset_connection(),

            opts,
        }
    }
}

/// Mysql connection.
#[derive(Debug)]
pub struct Conn(Box<ConnInner>);

impl Conn {
    /// Must not be called before handle_handshake.
    const fn has_capability(&self, flag: CapabilityFlags) -> bool {
        self.0.capability_flags.contains(flag)
    }

    /// Returns version number reported by the server.
    pub fn server_version(&self) -> (u16, u16, u16) {
        self.0
            .server_version
            .or(self.0.mariadb_server_version)
            .unwrap()
    }

    /// Returns connection identifier.
    pub fn connection_id(&self) -> u32 {
        self.0.connection_id
    }

    /// Returns number of rows affected by the last query.
    pub fn affected_rows(&self) -> u64 {
        self.0
            .ok_packet
            .as_ref()
            .map(OkPacket::affected_rows)
            .unwrap_or_default()
    }

    /// Returns last insert id of the last query.
    ///
    /// Returns zero if there was no last insert id.
    pub fn last_insert_id(&self) -> u64 {
        self.0
            .ok_packet
            .as_ref()
            .and_then(OkPacket::last_insert_id)
            .unwrap_or_default()
    }

    /// Returns number of warnings, reported by the server.
    pub fn warnings(&self) -> u16 {
        self.0
            .ok_packet
            .as_ref()
            .map(OkPacket::warnings)
            .unwrap_or_default()
    }

    /// [Info], reported by the server.
    ///
    /// Will be empty if not defined.
    ///
    /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    pub fn info_ref(&self) -> &[u8] {
        self.0
            .ok_packet
            .as_ref()
            .and_then(OkPacket::info_ref)
            .unwrap_or_default()
    }

    /// [Info], reported by the server.
    ///
    /// Will be empty if not defined.
    ///
    /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    pub fn info_str(&self) -> Cow<str> {
        self.0
            .ok_packet
            .as_ref()
            .and_then(OkPacket::info_str)
            .unwrap_or_default()
    }

    pub fn session_state_changes(&self) -> io::Result<Vec<SessionStateInfo<'_>>> {
        self.0
            .ok_packet
            .as_ref()
            .map(|ok| ok.session_state_info())
            .transpose()
            .map(Option::unwrap_or_default)
    }

    fn stream_ref(&self) -> &MySyncFramed<Stream> {
        self.0.stream.as_ref().expect("incomplete connection")
    }

    fn stream_mut(&mut self) -> &mut MySyncFramed<Stream> {
        self.0.stream.as_mut().expect("incomplete connection")
    }

    fn is_insecure(&self) -> bool {
        self.stream_ref().get_ref().is_insecure()
    }

    fn is_socket(&self) -> bool {
        self.stream_ref().get_ref().is_socket()
    }

    /// Check the connection can be improved.
    #[allow(unused_assignments)]
    fn can_improved(&mut self) -> Result<Option<Opts>> {
        if self.0.opts.get_prefer_socket() && self.0.opts.addr_is_loopback() {
            let mut socket = None;
            #[cfg(test)]
            {
                socket = self.0.opts.0.injected_socket.clone();
            }
            if socket.is_none() {
                socket = self.get_system_var("socket")?.map(from_value::<String>);
            }
            if let Some(socket) = socket {
                if self.0.opts.get_socket().is_none() {
                    let socket_opts = OptsBuilder::from_opts(self.0.opts.clone());
                    if !socket.is_empty() {
                        return Ok(Some(socket_opts.socket(Some(socket)).into()));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Creates new `Conn`.
    pub fn new<T, E>(opts: T) -> Result<Conn>
    where
        Opts: TryFrom<T, Error = E>,
        crate::Error: From<E>,
    {
        let opts = Opts::try_from(opts)?;
        let mut conn = Conn(Box::new(ConnInner::empty(opts)));
        conn.connect_stream()?;
        conn.connect()?;
        let mut conn = {
            if let Some(new_opts) = conn.can_improved()? {
                let mut improved_conn = Conn(Box::new(ConnInner::empty(new_opts)));
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
        for cmd in conn.0.opts.get_init() {
            conn.query_drop(cmd)?;
        }
        Ok(conn)
    }

    fn exec_com_reset_connection(&mut self) -> Result<()> {
        self.write_command(Command::COM_RESET_CONNECTION, &[])?;
        let packet = self.read_packet()?;
        self.handle_ok::<CommonOkPacket>(&packet)?;
        self.0.last_command = 0;
        self.0.stmt_cache.clear();
        Ok(())
    }

    fn exec_com_change_user(&mut self, opts: ChangeUserOpts) -> Result<()> {
        opts.update_opts(&mut self.0.opts);
        let com_change_user = ComChangeUser::new()
            .with_user(self.0.opts.get_user().map(|x| x.as_bytes()))
            .with_database(self.0.opts.get_db_name().map(|x| x.as_bytes()))
            .with_auth_plugin_data(
                self.0
                    .auth_plugin
                    .gen_data(self.0.opts.get_pass(), &self.0.nonce)
                    .as_deref(),
            )
            .with_more_data(Some(
                ComChangeUserMoreData::new(if self.server_version() >= (5, 5, 3) {
                    UTF8MB4_GENERAL_CI
                } else {
                    UTF8_GENERAL_CI
                })
                .with_auth_plugin(Some(self.0.auth_plugin.clone()))
                .with_connect_attributes(self.0.opts.get_connect_attrs().cloned()),
            ))
            .into_owned();
        self.write_command_raw(&com_change_user)?;
        self.0.last_command = 0;
        self.0.stmt_cache.clear();
        self.continue_auth(false)
    }

    /// Tries to reset the connection.
    ///
    /// This function will try to invoke COM_RESET_CONNECTION with
    /// a fall back to COM_CHANGE_USER on older servers.
    ///
    /// ## Warning
    ///
    /// There is a long-standing bug in mysql 5.6 that kills this functionality in presence
    /// of connection attributes (see [Bug #92954](https://bugs.mysql.com/bug.php?id=92954)).
    ///
    /// ## Note
    ///
    /// Re-executes [`Opts::get_init`].
    pub fn reset(&mut self) -> Result<()> {
        let reset_result = match (self.0.server_version, self.0.mariadb_server_version) {
            (Some(ref version), _) if *version > (5, 7, 3) => self.exec_com_reset_connection(),
            (_, Some(ref version)) if *version >= (10, 2, 7) => self.exec_com_reset_connection(),
            _ => return self.exec_com_change_user(ChangeUserOpts::DEFAULT),
        };

        match reset_result {
            Ok(_) => (),
            Err(crate::Error::MySqlError(_)) => {
                // fallback to COM_CHANGE_USER if server reports an error for COM_RESET_CONNECTION
                self.exec_com_change_user(ChangeUserOpts::DEFAULT)?;
            }
            Err(e) => return Err(e),
        }

        for cmd in self.0.opts.get_init() {
            self.query_drop(cmd)?;
        }

        Ok(())
    }

    /// Executes [`COM_CHANGE_USER`][1].
    ///
    /// This might be used as an older and slower alternative to `COM_RESET_CONNECTION` that
    /// works on MySql prior to 5.7.3 (MariaDb prior ot 10.2.4).
    ///
    /// ## Note
    ///
    /// * Using non-default `opts` for a pooled connection is discouraging.
    /// * Connection options will be updated permanently.
    ///
    /// ## Warning
    ///
    /// There is a long-standing bug in mysql 5.6 that kills this functionality in presence
    /// of connection attributes (see [Bug #92954](https://bugs.mysql.com/bug.php?id=92954)).
    ///
    /// [1]: https://dev.mysql.com/doc/c-api/5.7/en/mysql-change-user.html
    pub fn change_user(&mut self, opts: ChangeUserOpts) -> Result<()> {
        self.exec_com_change_user(opts)
    }

    fn switch_to_ssl(&mut self, ssl_opts: SslOpts) -> Result<()> {
        let stream = self.0.stream.take().expect("incomplete conn");
        let (in_buf, out_buf, codec, stream) = stream.destruct();
        let stream = stream.make_secure(self.0.opts.get_host(), ssl_opts)?;
        let stream = MySyncFramed::construct(in_buf, out_buf, codec, stream);
        self.0.stream = Some(stream);
        Ok(())
    }

    fn connect_stream(&mut self) -> Result<()> {
        let opts = &self.0.opts;
        let read_timeout = opts.get_read_timeout().cloned();
        let write_timeout = opts.get_write_timeout().cloned();
        let tcp_keepalive_time = opts.get_tcp_keepalive_time_ms();
        #[cfg(any(target_os = "linux", target_os = "macos",))]
        let tcp_keepalive_probe_interval_secs = opts.get_tcp_keepalive_probe_interval_secs();
        #[cfg(any(target_os = "linux", target_os = "macos",))]
        let tcp_keepalive_probe_count = opts.get_tcp_keepalive_probe_count();
        #[cfg(target_os = "linux")]
        let tcp_user_timeout = opts.get_tcp_user_timeout_ms();
        let tcp_nodelay = opts.get_tcp_nodelay();
        let tcp_connect_timeout = opts.get_tcp_connect_timeout();
        let bind_address = opts.bind_address().cloned();
        let stream = if let Some(socket) = opts.get_socket() {
            Stream::connect_socket(socket, read_timeout, write_timeout)?
        } else {
            let port = opts.get_tcp_port();
            let ip_or_hostname = match opts.get_host() {
                url::Host::Domain(domain) => domain,
                url::Host::Ipv4(ip) => ip.to_string(),
                url::Host::Ipv6(ip) => ip.to_string(),
            };
            Stream::connect_tcp(
                &ip_or_hostname,
                port,
                read_timeout,
                write_timeout,
                tcp_keepalive_time,
                #[cfg(any(target_os = "linux", target_os = "macos",))]
                tcp_keepalive_probe_interval_secs,
                #[cfg(any(target_os = "linux", target_os = "macos",))]
                tcp_keepalive_probe_count,
                #[cfg(target_os = "linux")]
                tcp_user_timeout,
                tcp_nodelay,
                tcp_connect_timeout,
                bind_address,
            )?
        };
        self.0.stream = Some(MySyncFramed::new(stream));
        Ok(())
    }

    fn raw_read_packet(&mut self, buffer: &mut Vec<u8>) -> Result<()> {
        if !self.stream_mut().next_packet(buffer)? {
            Err(Error::server_disconnected())
        } else {
            Ok(())
        }
    }

    fn read_packet(&mut self) -> Result<Buffer> {
        loop {
            let mut buffer = get_buffer();
            match self.raw_read_packet(buffer.as_mut()) {
                Ok(()) if buffer.first() == Some(&0xff) => {
                    match ParseBuf(&buffer).parse(self.0.capability_flags)? {
                        ErrPacket::Error(server_error) => {
                            self.handle_err();
                            return Err(MySqlError(From::from(server_error)));
                        }
                        ErrPacket::Progress(_progress_report) => {
                            // TODO: Report progress
                            continue;
                        }
                    }
                }
                Ok(()) => return Ok(buffer),
                Err(e) => {
                    self.handle_err();
                    return Err(e);
                }
            }
        }
    }

    fn drop_packet(&mut self) -> Result<()> {
        self.read_packet().map(drop)
    }

    fn write_struct<T: MySerialize>(&mut self, s: &T) -> Result<()> {
        let mut buf = get_buffer();
        s.serialize(buf.as_mut());
        self.write_packet(&mut &*buf)
    }

    fn write_packet<T: Buf>(&mut self, data: &mut T) -> Result<()> {
        self.stream_mut().send(data)?;
        Ok(())
    }

    fn handle_handshake(&mut self, hp: &HandshakePacket<'_>) {
        self.0.capability_flags = hp.capabilities() & self.get_client_flags();
        self.0.status_flags = hp.status_flags();
        self.0.connection_id = hp.connection_id();
        self.0.character_set = hp.default_collation();
        self.0.server_version = hp.server_version_parsed();
        self.0.mariadb_server_version = hp.maria_db_server_version_parsed();
    }

    fn handle_ok<'a, T: OkPacketKind>(
        &mut self,
        buffer: &'a Buffer,
    ) -> crate::Result<OkPacket<'a>> {
        let ok = ParseBuf(buffer)
            .parse::<OkPacketDeserializer<T>>(self.0.capability_flags)?
            .into_inner();
        self.0.status_flags = ok.status_flags();
        self.0.ok_packet = Some(ok.clone().into_owned());
        Ok(ok)
    }

    fn handle_err(&mut self) {
        self.0.status_flags = StatusFlags::empty();
        self.0.has_results = false;
        self.0.ok_packet = None;
    }

    fn more_results_exists(&self) -> bool {
        self.0
            .status_flags
            .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }

    fn perform_auth_switch(&mut self, auth_switch_request: AuthSwitchRequest<'_>) -> Result<()> {
        if matches!(
            auth_switch_request.auth_plugin(),
            AuthPlugin::MysqlOldPassword
        ) && self.0.opts.get_secure_auth()
        {
            return Err(DriverError(OldMysqlPasswordDisabled));
        }

        if matches!(
            auth_switch_request.auth_plugin(),
            AuthPlugin::Other(Cow::Borrowed(b"mysql_clear_password"))
        ) && !self.0.opts.get_enable_cleartext_plugin()
        {
            return Err(DriverError(CleartextPluginDisabled));
        }

        self.0.nonce = auth_switch_request.plugin_data().to_vec();
        self.0.auth_plugin = auth_switch_request.auth_plugin().into_owned();
        let plugin_data = match self.0.auth_plugin {
            ref x @ AuthPlugin::MysqlOldPassword => {
                if self.0.opts.get_secure_auth() {
                    return Err(DriverError(OldMysqlPasswordDisabled));
                }
                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            ref x @ AuthPlugin::MysqlNativePassword => {
                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            ref x @ AuthPlugin::CachingSha2Password => {
                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            ref x @ AuthPlugin::MysqlClearPassword => {
                if !self.0.opts.get_enable_cleartext_plugin() {
                    return Err(DriverError(UnknownAuthPlugin(
                        "mysql_clear_password".into(),
                    )));
                }

                x.gen_data(self.0.opts.get_pass(), &self.0.nonce)
            }
            ref x @ AuthPlugin::Ed25519 => x.gen_data(self.0.opts.get_pass(), &self.0.nonce),
            AuthPlugin::Other(_) => None,
        };

        if let Some(plugin_data) = plugin_data {
            self.write_struct(&plugin_data.into_owned())?;
        } else {
            self.write_packet(&mut &[0_u8; 0][..])?;
        }

        self.continue_auth(true)
    }

    fn do_handshake(&mut self) -> Result<()> {
        let payload = self.read_packet()?;
        let handshake = ParseBuf(&payload).parse::<HandshakePacket>(())?;

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
            if let Some(ssl_opts) = self.0.opts.get_ssl_opts().cloned() {
                if !self.has_capability(CapabilityFlags::CLIENT_SSL) {
                    return Err(DriverError(TlsNotSupported));
                } else {
                    self.do_ssl_request()?;
                    self.switch_to_ssl(ssl_opts)?;
                }
            }
        }

        // Handshake scramble is always 21 bytes length (20 + zero terminator)
        self.0.nonce = {
            let mut nonce = Vec::from(handshake.scramble_1_ref());
            nonce.extend_from_slice(handshake.scramble_2_ref().unwrap_or(&[][..]));
            // Trim zero terminator. Fill with zeroes if nonce
            // is somehow smaller than 20 bytes (this matches the server behavior).
            nonce.resize(20, 0);
            nonce
        };

        // Allow only CachingSha2Password and MysqlNativePassword here
        // because sha256_password is deprecated and other plugins won't
        // appear here.
        self.0.auth_plugin = match handshake.auth_plugin() {
            Some(x @ AuthPlugin::CachingSha2Password) => x.into_owned(),
            _ => AuthPlugin::MysqlNativePassword,
        };

        self.write_handshake_response()?;
        self.continue_auth(false)?;

        if self.has_capability(CapabilityFlags::CLIENT_COMPRESS) {
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
            | (self.0.capability_flags & CapabilityFlags::CLIENT_LONG_FLAG);
        if self.0.opts.get_compress().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_COMPRESS);
        }
        if self.0.opts.get_connect_attrs().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_CONNECT_ATTRS);
        }
        if let Some(db_name) = self.0.opts.get_db_name() {
            if !db_name.is_empty() {
                client_flags.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
            }
        }
        if self.is_insecure() && self.0.opts.get_ssl_opts().is_some() {
            client_flags.insert(CapabilityFlags::CLIENT_SSL);
        }
        client_flags | self.0.opts.get_additional_capabilities()
    }

    fn connect_attrs(&self) -> Option<HashMap<String, String>> {
        if let Some(attrs) = self.0.opts.get_connect_attrs() {
            let program_name = match attrs.get("program_name") {
                Some(program_name) => program_name.clone(),
                None => {
                    let arg0 = std::env::args_os().next();
                    let arg0 = arg0.as_ref().map(|x| x.to_string_lossy());
                    arg0.unwrap_or_else(|| "".into()).into_owned()
                }
            };

            let mut attrs_to_send = HashMap::new();

            attrs_to_send.insert("_client_name".into(), "rust-mysql-simple".into());
            attrs_to_send.insert("_client_version".into(), env!("CARGO_PKG_VERSION").into());
            attrs_to_send.insert("_os".into(), env!("CARGO_CFG_TARGET_OS").into());
            attrs_to_send.insert("_pid".into(), process::id().to_string());
            attrs_to_send.insert("_platform".into(), env!("CARGO_CFG_TARGET_ARCH").into());
            attrs_to_send.insert("program_name".into(), program_name);

            for (name, value) in attrs.clone() {
                attrs_to_send.insert(name, value);
            }

            Some(attrs_to_send)
        } else {
            None
        }
    }

    fn do_ssl_request(&mut self) -> Result<()> {
        let charset = if self.server_version() >= (5, 5, 3) {
            UTF8MB4_GENERAL_CI
        } else {
            UTF8_GENERAL_CI
        };

        let ssl_request = SslRequest::new(
            self.get_client_flags(),
            DEFAULT_MAX_ALLOWED_PACKET as u32,
            charset as u8,
        );
        self.write_struct(&ssl_request)
    }

    fn write_handshake_response(&mut self) -> Result<()> {
        let auth_data = self
            .0
            .auth_plugin
            .gen_data(self.0.opts.get_pass(), &self.0.nonce)
            .map(|x| x.into_owned());

        let handshake_response = HandshakeResponse::new(
            auth_data.as_deref(),
            self.0.server_version.unwrap_or((0, 0, 0)),
            self.0.opts.get_user().map(str::as_bytes),
            self.0.opts.get_db_name().map(str::as_bytes),
            Some(self.0.auth_plugin.clone()),
            self.0.capability_flags,
            self.connect_attrs(),
            self.0
                .opts
                .get_max_allowed_packet()
                .unwrap_or(DEFAULT_MAX_ALLOWED_PACKET) as u32,
        );

        let mut buf = get_buffer();
        handshake_response.serialize(buf.as_mut());
        self.write_packet(&mut &*buf)
    }

    fn continue_auth(&mut self, auth_switched: bool) -> Result<()> {
        match self.0.auth_plugin {
            AuthPlugin::CachingSha2Password => {
                self.continue_caching_sha2_password_auth(auth_switched)?;
                Ok(())
            }
            AuthPlugin::MysqlNativePassword | AuthPlugin::MysqlOldPassword => {
                self.continue_mysql_native_password_auth(auth_switched)?;
                Ok(())
            }
            AuthPlugin::MysqlClearPassword => {
                if !self.0.opts.get_enable_cleartext_plugin() {
                    return Err(DriverError(CleartextPluginDisabled));
                }
                self.continue_mysql_native_password_auth(auth_switched)?;
                Ok(())
            }
            AuthPlugin::Ed25519 => {
                self.continue_ed25519_auth(auth_switched)?;
                Ok(())
            }
            AuthPlugin::Other(ref name) => {
                let plugin_name = String::from_utf8_lossy(name).into();
                Err(DriverError(UnknownAuthPlugin(plugin_name)))
            }
        }
    }

    fn continue_mysql_native_password_auth(&mut self, auth_switched: bool) -> Result<()> {
        let payload = self.read_packet()?;

        match payload[0] {
            // auth ok
            0x00 => self.handle_ok::<CommonOkPacket>(&payload).map(drop),
            // auth switch
            0xfe if !auth_switched => {
                let auth_switch = if payload.len() > 1 {
                    ParseBuf(&payload).parse(())?
                } else {
                    let _ = ParseBuf(&payload).parse::<OldAuthSwitchRequest>(())?;
                    // we'll map OldAuthSwitchRequest to an AuthSwitchRequest with mysql_old_password plugin.
                    AuthSwitchRequest::new("mysql_old_password".as_bytes(), &*self.0.nonce)
                        .into_owned()
                };
                self.perform_auth_switch(auth_switch)
            }
            _ => Err(DriverError(UnexpectedPacket)),
        }
    }

    fn continue_caching_sha2_password_auth(&mut self, auth_switched: bool) -> Result<()> {
        let payload = self.read_packet()?;

        match payload[0] {
            0x00 => {
                // ok packet for empty password
                Ok(())
            }
            0x01 => match payload[1] {
                0x03 => {
                    let payload = self.read_packet()?;
                    self.handle_ok::<CommonOkPacket>(&payload).map(drop)
                }
                0x04 => {
                    if !self.is_insecure() || self.is_socket() {
                        let mut pass = self.0.opts.get_pass().map(Vec::from).unwrap_or_default();
                        pass.push(0);
                        self.write_packet(&mut pass.as_slice())?;
                    } else {
                        self.write_packet(&mut &[0x02][..])?;
                        let payload = self.read_packet()?;
                        let key = &payload[1..];
                        let mut pass = self.0.opts.get_pass().map(Vec::from).unwrap_or_default();
                        pass.push(0);
                        for (i, c) in pass.iter_mut().enumerate() {
                            *(c) ^= self.0.nonce[i % self.0.nonce.len()];
                        }
                        let encrypted_pass = crypto::encrypt(&pass, key);
                        self.write_packet(&mut encrypted_pass.as_slice())?;
                    }

                    let payload = self.read_packet()?;
                    self.handle_ok::<CommonOkPacket>(&payload).map(drop)
                }
                _ => Err(DriverError(UnexpectedPacket)),
            },
            0xfe if !auth_switched => {
                let auth_switch_request = ParseBuf(&payload).parse(())?;
                self.perform_auth_switch(auth_switch_request)
            }
            _ => Err(DriverError(UnexpectedPacket)),
        }
    }

    fn continue_ed25519_auth(&mut self, auth_switched: bool) -> Result<()> {
        let payload = self.read_packet()?;
        match payload[0] {
            // ok packet for empty password
            0x00 => Ok(()),
            0xfe if !auth_switched => {
                let auth_switch_request = ParseBuf(&payload).parse(())?;
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

    fn write_command_raw<T: MySerialize>(&mut self, cmd: &T) -> Result<()> {
        let mut buf = get_buffer();
        cmd.serialize(buf.as_mut());
        self.reset_seq_id();
        debug_assert!(!buf.is_empty());
        self.0.last_command = buf[0];
        self.write_packet(&mut &*buf)
    }

    fn write_command(&mut self, cmd: Command, data: &[u8]) -> Result<()> {
        let mut buf = get_buffer();
        buf.as_mut().put_u8(cmd as u8);
        buf.as_mut().extend_from_slice(data);

        self.reset_seq_id();
        self.0.last_command = buf[0];
        self.write_packet(&mut &*buf)
    }

    fn send_long_data(&mut self, stmt_id: u32, params: &[Value]) -> Result<()> {
        for (i, value) in params.iter().enumerate() {
            if let Bytes(bytes) = value {
                let chunks = bytes.chunks(MAX_PAYLOAD_LEN - 6);
                let chunks = chunks.chain(if bytes.is_empty() {
                    Some(&[][..])
                } else {
                    None
                });
                for chunk in chunks {
                    let cmd = ComStmtSendLongData::new(stmt_id, i as u16, Cow::Borrowed(chunk));
                    self.write_command_raw(&cmd)?;
                }
            }
        }

        Ok(())
    }

    fn _execute(
        &mut self,
        stmt: &Statement,
        params: Params,
    ) -> Result<Or<Vec<Column>, OkPacket<'static>>> {
        let exec_request = match &params {
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
                    ComStmtExecuteRequestBuilder::new(stmt.id()).build(params);

                if as_long_data {
                    self.send_long_data(stmt.id(), params)?;
                }

                body
            }
            Params::Named(_) => {
                if let Some(named_params) = stmt.named_params.as_ref() {
                    return self._execute(stmt, params.into_positional(named_params)?);
                } else {
                    return Err(DriverError(NamedParamsForPositionalQuery));
                }
            }
        };
        self.write_command_raw(&exec_request)?;
        self.handle_result_set()
    }

    fn _start_transaction(&mut self, tx_opts: TxOpts) -> Result<()> {
        if let Some(i_level) = tx_opts.isolation_level() {
            self.query_drop(format!("SET TRANSACTION ISOLATION LEVEL {}", i_level))?;
        }
        if let Some(mode) = tx_opts.access_mode() {
            let supported = match (self.0.server_version, self.0.mariadb_server_version) {
                (Some(ref version), _) if *version >= (5, 6, 5) => true,
                (_, Some(ref version)) if *version >= (10, 0, 0) => true,
                _ => false,
            };
            if !supported {
                return Err(DriverError(ReadOnlyTransNotSupported));
            }
            match mode {
                AccessMode::ReadOnly => self.query_drop("SET TRANSACTION READ ONLY")?,
                AccessMode::ReadWrite => self.query_drop("SET TRANSACTION READ WRITE")?,
            }
        }
        if tx_opts.with_consistent_snapshot() {
            self.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .unwrap();
        } else {
            self.query_drop("START TRANSACTION")?;
        };
        Ok(())
    }

    fn send_local_infile(&mut self, file_name: &[u8]) -> Result<OkPacket<'static>> {
        {
            let mut buffer = [0_u8; LocalInfile::BUFFER_SIZE];
            let maybe_handler = self
                .0
                .local_infile_handler
                .clone()
                .or_else(|| self.0.opts.get_local_infile_handler().cloned());
            let mut local_infile = LocalInfile::new(&mut buffer, self);
            if let Some(handler) = maybe_handler {
                // Unwrap won't panic because we have exclusive access to `self` and this
                // method is not re-entrant, because `LocalInfile` does not expose the
                // connection.
                let handler_fn = &mut *handler.0.lock()?;
                handler_fn(file_name, &mut local_infile)?;
            }
            local_infile.flush()?;
        }
        self.write_packet(&mut &[][..])?;
        let payload = self.read_packet()?;
        let ok = self.handle_ok::<CommonOkPacket>(&payload)?;
        Ok(ok.into_owned())
    }

    fn handle_result_set(&mut self) -> Result<Or<Vec<Column>, OkPacket<'static>>> {
        if self.more_results_exists() {
            self.sync_seq_id();
        }

        let pld = self.read_packet()?;
        match pld[0] {
            0x00 => {
                let ok = self.handle_ok::<CommonOkPacket>(&pld)?;
                Ok(Or::B(ok.into_owned()))
            }
            0xfb => match self.send_local_infile(&pld[1..]) {
                Ok(ok) => Ok(Or::B(ok)),
                Err(err) => Err(err),
            },
            _ => {
                let mut reader = &pld[..];
                let column_count = reader.read_lenenc_int()?;
                let mut columns: Vec<Column> = Vec::with_capacity(column_count as usize);
                for _ in 0..column_count {
                    let pld = self.read_packet()?;
                    let column = ParseBuf(&pld).parse(())?;
                    columns.push(column);
                }
                // skip eof packet
                self.drop_packet()?;
                self.0.has_results = column_count > 0;
                Ok(Or::A(columns))
            }
        }
    }

    fn _query(&mut self, query: &str) -> Result<Or<Vec<Column>, OkPacket<'static>>> {
        self.write_command(Command::COM_QUERY, query.as_bytes())?;
        self.handle_result_set()
    }

    /// Executes [`COM_PING`](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_ping.html)
    /// on `Conn`. Return `true` on success or `false` on error.
    pub fn ping(&mut self) -> Result<(), Error> {
        self.write_command(Command::COM_PING, &[])?;
        self.drop_packet()
    }

    /// Executes [`COM_INIT_DB`](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_init_db.html)
    /// on `Conn`.
    pub fn select_db(&mut self, schema: &str) -> Result<(), Error> {
        self.write_command(Command::COM_INIT_DB, schema.as_bytes())?;
        self.drop_packet()
    }

    /// Starts new transaction with provided options.
    /// `readonly` is only available since MySQL 5.6.5.
    pub fn start_transaction(&mut self, tx_opts: TxOpts) -> Result<Transaction> {
        self._start_transaction(tx_opts)?;
        Ok(Transaction::new(self.into()))
    }

    fn _true_prepare(&mut self, query: &[u8]) -> Result<InnerStmt> {
        self.write_command(Command::COM_STMT_PREPARE, query)?;
        let pld = self.read_packet()?;
        let mut stmt = ParseBuf(&pld).parse::<InnerStmt>(self.connection_id())?;
        if stmt.num_params() > 0 {
            let mut params: Vec<Column> = Vec::with_capacity(stmt.num_params() as usize);
            for _ in 0..stmt.num_params() {
                let pld = self.read_packet()?;
                params.push(ParseBuf(&pld).parse(())?);
            }
            stmt = stmt.with_params(Some(params));
            self.drop_packet()?;
        }
        if stmt.num_columns() > 0 {
            let mut columns: Vec<Column> = Vec::with_capacity(stmt.num_columns() as usize);
            for _ in 0..stmt.num_columns() {
                let pld = self.read_packet()?;
                columns.push(ParseBuf(&pld).parse(())?);
            }
            stmt = stmt.with_columns(Some(columns));
            self.drop_packet()?;
        }
        Ok(stmt)
    }

    fn _prepare(&mut self, query: &[u8]) -> Result<Arc<InnerStmt>> {
        if let Some(entry) = self.0.stmt_cache.by_query(query) {
            return Ok(entry.stmt.clone());
        }

        let inner_st = Arc::new(self._true_prepare(query)?);

        if let Some(old_stmt) = self
            .0
            .stmt_cache
            .put(Arc::new(query.into()), inner_st.clone())
        {
            self.close(Statement::new(old_stmt, None))?;
        }

        Ok(inner_st)
    }

    fn connect(&mut self) -> Result<()> {
        if self.0.connected {
            return Ok(());
        }
        self.do_handshake()
            .and_then(|_| match self.0.opts.get_max_allowed_packet() {
                Some(x) => Ok(x),
                None => Ok(from_value_opt::<usize>(
                    self.get_system_var("max_allowed_packet")?.unwrap_or(NULL),
                )
                .unwrap_or(0)),
            })
            .and_then(|max_allowed_packet| {
                if max_allowed_packet == 0 {
                    Err(DriverError(SetupError))
                } else {
                    self.stream_mut().codec_mut().max_allowed_packet = max_allowed_packet;
                    self.0.connected = true;
                    Ok(())
                }
            })
    }

    fn get_system_var(&mut self, name: &str) -> Result<Option<Value>> {
        self.query_first(format!("SELECT @@{}", name))
    }

    fn next_row_packet(&mut self) -> Result<Option<Buffer>> {
        if !self.0.has_results {
            return Ok(None);
        }

        let pld = self.read_packet()?;

        if self.has_capability(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
            if pld[0] == 0xfe && pld.len() < MAX_PAYLOAD_LEN {
                self.0.has_results = false;
                self.handle_ok::<ResultSetTerminator>(&pld)?;
                return Ok(None);
            }
        } else if pld[0] == 0xfe && pld.len() < 8 {
            self.0.has_results = false;
            self.handle_ok::<OldEofPacket>(&pld)?;
            return Ok(None);
        }

        Ok(Some(pld))
    }

    fn has_stmt(&self, query: &[u8]) -> bool {
        self.0.stmt_cache.contains_query(query)
    }

    /// Sets a callback to handle requests for local files. These are
    /// caused by using `LOAD DATA LOCAL INFILE` queries. The
    /// callback is passed the filename, and a `Write`able object
    /// to receive the contents of that file.
    /// Specifying `None` will reset the handler to the one specified
    /// in the `Opts` for this connection.
    pub fn set_local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) {
        self.0.local_infile_handler = handler;
    }

    pub fn no_backslash_escape(&self) -> bool {
        self.0
            .status_flags
            .contains(StatusFlags::SERVER_STATUS_NO_BACKSLASH_ESCAPES)
    }

    #[cfg(feature = "binlog")]
    fn register_as_slave(&mut self, server_id: u32) -> Result<()> {
        use mysql_common::packets::ComRegisterSlave;

        self.query_drop("SET @master_binlog_checksum='ALL'")?;
        self.write_command_raw(&ComRegisterSlave::new(server_id))?;

        // Server will respond with OK.
        self.read_packet()?;

        Ok(())
    }

    #[cfg(feature = "binlog")]
    fn request_binlog(&mut self, request: BinlogRequest<'_>) -> Result<()> {
        self.register_as_slave(request.server_id())?;
        self.write_command_raw(&request.as_cmd())?;
        Ok(())
    }

    /// Turns this connection into a binlog stream.
    ///
    /// You can use `SHOW BINARY LOGS` to get the current log file and position from the master.
    /// If the request's `filename` is empty, the server will send the binlog-stream
    /// of the first known binlog.
    #[cfg(feature = "binlog")]
    #[cfg_attr(docsrs, doc(cfg(feature = "binlog")))]
    pub fn get_binlog_stream(mut self, request: BinlogRequest<'_>) -> Result<BinlogStream> {
        self.request_binlog(request)?;
        Ok(BinlogStream::new(self))
    }

    fn cleanup_for_pool(&mut self) -> Result<()> {
        self.set_local_infile_handler(None);
        if self.0.reset_upon_return {
            self.reset()?;
        }

        self.0.reset_upon_return = self.0.opts.get_pool_opts().reset_connection();

        Ok(())
    }
}

#[cfg(unix)]
impl AsRawFd for Conn {
    fn as_raw_fd(&self) -> RawFd {
        self.stream_ref().get_ref().as_raw_fd()
    }
}

impl Queryable for Conn {
    fn query_iter<T: AsRef<str>>(&mut self, query: T) -> Result<QueryResult<'_, '_, '_, Text>> {
        let meta = self._query(query.as_ref())?;
        Ok(QueryResult::new(ConnMut::Mut(self), meta))
    }

    fn prep<T: AsRef<str>>(&mut self, query: T) -> Result<Statement> {
        let query = query.as_ref();
        let parsed = ParsedNamedParams::parse(query.as_bytes())?;
        let named_params: Vec<Vec<u8>> =
            parsed.params().iter().map(|param| param.to_vec()).collect();
        let named_params = if named_params.is_empty() {
            None
        } else {
            Some(named_params)
        };
        self._prepare(parsed.borrow().query())
            .map(|inner| Statement::new(inner, named_params))
    }

    fn close(&mut self, stmt: Statement) -> Result<()> {
        self.0.stmt_cache.remove(stmt.id());
        let cmd = ComStmtClose::new(stmt.id());
        self.write_command_raw(&cmd)
    }

    fn exec_iter<S, P>(&mut self, stmt: S, params: P) -> Result<QueryResult<'_, '_, '_, Binary>>
    where
        S: AsStatement,
        P: Into<Params>,
    {
        let statement = stmt.as_statement(self)?;
        let meta = self._execute(&statement, params.into())?;
        Ok(QueryResult::new(ConnMut::Mut(self), meta))
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        let stmt_cache = mem::replace(&mut self.0.stmt_cache, StmtCache::new(0));

        for (_, entry) in stmt_cache.into_iter() {
            let _ = self.close(Statement::new(entry.stmt, None));
        }

        if self.0.stream.is_some() {
            let _ = self.write_command(Command::COM_QUIT, &[]);
        }
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    mod my_conn {
        use std::{
            collections::HashMap,
            io::Write,
            process,
            sync::mpsc::{channel, sync_channel},
            thread::spawn,
            time::Duration,
        };

        #[cfg(feature = "binlog")]
        use mysql_common::{binlog::events::EventData, packets::binlog_request::BinlogRequest};
        use rand::Fill;
        #[cfg(feature = "time")]
        use time::PrimitiveDateTime;

        use crate::{
            conn::ConnInner,
            from_row, from_value, params,
            prelude::*,
            test_misc::get_opts,
            Conn,
            DriverError::{MissingNamedParameter, NamedParamsForPositionalQuery},
            Error::DriverError,
            LocalInfileHandler, Opts, OptsBuilder, Pool, TxOpts,
            Value::{self, Bytes, Date, Float, Int, NULL},
        };

        fn get_system_variable<T>(conn: &mut Conn, name: &str) -> T
        where
            T: FromValue,
        {
            conn.query_first::<(String, T), _>(format!("show variables like '{}'", name))
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
            assert!(conn.ping().is_ok());

            if crate::test_misc::test_compression() {
                assert!(format!("{:?}", conn.0.stream).contains("Compression"));
            }

            if crate::test_misc::test_ssl() {
                assert!(!conn.is_insecure());
            }
        }

        #[test]
        fn mysql_async_issue_107() -> crate::Result<()> {
            let mut conn = Conn::new(get_opts())?;
            conn.query_drop(
                r"CREATE TEMPORARY TABLE mysql.issue (
                        a BIGINT(20) UNSIGNED,
                        b VARBINARY(16),
                        c BINARY(32),
                        d BIGINT(20) UNSIGNED,
                        e BINARY(32)
                    )",
            )?;
            conn.query_drop(
                r"INSERT INTO mysql.issue VALUES (
                        0,
                        0xC066F966B0860000,
                        0x7939DA98E524C5F969FC2DE8D905FD9501EBC6F20001B0A9C941E0BE6D50CF44,
                        0,
                        ''
                    ), (
                        1,
                        '',
                        0x076311DF4D407B0854371BA13A5F3FB1A4555AC22B361375FD47B263F31822F2,
                        0,
                        ''
                    )",
            )?;

            let q = "SELECT b, c, d, e FROM mysql.issue";
            let result = conn.query_iter(q)?;

            let loaded_structs = result
                .map(|row| crate::from_row::<(Vec<u8>, Vec<u8>, u64, Vec<u8>)>(row.unwrap()))
                .collect::<Vec<_>>();

            assert_eq!(loaded_structs.len(), 2);

            Ok(())
        }

        #[test]
        fn query_traits() -> Result<(), Box<dyn std::error::Error>> {
            macro_rules! test_query {
                ($conn : expr) => {
                    "CREATE TABLE IF NOT EXISTS tmplak (a INT)"
                        .run($conn)
                        .unwrap();
                    "DELETE FROM tmplak".run($conn).unwrap();

                    "INSERT INTO tmplak (a) VALUES (?)"
                        .with((42,))
                        .run($conn)
                        .unwrap();

                    "INSERT INTO tmplak (a) VALUES (?)"
                        .with((43..=44).map(|x| (x,)))
                        .batch($conn)?;

                    let first: Option<u8> = "SELECT a FROM tmplak LIMIT 1".first($conn).unwrap();
                    assert_eq!(first, Some(42), "first text");

                    let first: Option<u8> = "SELECT a FROM tmplak LIMIT 1"
                        .with(())
                        .first($conn)
                        .unwrap();
                    assert_eq!(first, Some(42), "first bin");

                    let count = "SELECT a FROM tmplak".run($conn).unwrap().count();
                    assert_eq!(count, 3, "run text");

                    let count = "SELECT a FROM tmplak".with(()).run($conn).unwrap().count();
                    assert_eq!(count, 3, "run bin");

                    let all: Vec<u8> = "SELECT a FROM tmplak".fetch($conn).unwrap();
                    assert_eq!(all, vec![42, 43, 44], "fetch text");

                    let all: Vec<u8> = "SELECT a FROM tmplak".with(()).fetch($conn).unwrap();
                    assert_eq!(all, vec![42, 43, 44], "fetch bin");

                    let mapped = "SELECT a FROM tmplak".map($conn, |x: u8| x + 1).unwrap();
                    assert_eq!(mapped, vec![43, 44, 45], "map text");

                    let mapped = "SELECT a FROM tmplak"
                        .with(())
                        .map($conn, |x: u8| x + 1)
                        .unwrap();
                    assert_eq!(mapped, vec![43, 44, 45], "map bin");

                    let sum = "SELECT a FROM tmplak"
                        .fold($conn, 0_u8, |acc, x: u8| acc + x)
                        .unwrap();
                    assert_eq!(sum, 42 + 43 + 44, "fold text");

                    let sum = "SELECT a FROM tmplak"
                        .with(())
                        .fold($conn, 0_u8, |acc, x: u8| acc + x)
                        .unwrap();
                    assert_eq!(sum, 42 + 43 + 44, "fold bin");

                    "DROP TABLE tmplak".run($conn).unwrap();
                };
            }

            let mut conn = Conn::new(get_opts())?;

            let mut tx = conn.start_transaction(TxOpts::default())?;
            test_query!(&mut tx);
            tx.rollback()?;

            test_query!(&mut conn);

            let pool = Pool::new(get_opts())?;
            let mut pooled_conn = pool.get_conn()?;

            let mut tx = pool.start_transaction(TxOpts::default())?;
            test_query!(&mut tx);
            tx.rollback()?;

            test_query!(&mut pooled_conn);

            Ok(())
        }

        #[test]
        #[should_panic(expected = "Could not connect to address")]
        fn should_fail_on_wrong_socket_path() {
            let opts = OptsBuilder::from_opts(get_opts()).socket(Some("/foo/bar/baz"));
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

            let opts = OptsBuilder::from_opts(get_opts()).db_name(Some(DB_NAME));

            let mut conn = Conn::new(opts).unwrap();

            let db_name: String = conn.query_first("SELECT DATABASE()").unwrap().unwrap();
            assert_eq!(db_name, DB_NAME);
        }

        #[test]
        fn should_connect_by_hostname() {
            let opts = OptsBuilder::from_opts(get_opts()).ip_or_hostname(Some("localhost"));
            let mut conn = Conn::new(opts).unwrap();
            assert!(conn.ping().is_ok());
        }

        #[test]
        fn should_select_db() {
            const DB_NAME: &str = "t_select_db";

            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {}", DB_NAME))
                .unwrap();
            assert!(conn.select_db(DB_NAME).is_ok());

            let db_name: String = conn.query_first("SELECT DATABASE()").unwrap().unwrap();
            assert_eq!(db_name, DB_NAME);

            conn.query_drop(format!("DROP DATABASE {}", DB_NAME))
                .unwrap();
        }

        #[test]
        fn should_execute_queries_and_parse_results() {
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

            conn.query_drop("SELECT * FROM nonexistent").unwrap_err();
            conn.query_iter("SELECT * FROM mysql.tbl").unwrap(); // Drop::drop for QueryResult

            conn.query_drop("UPDATE mysql.tbl SET a = 'foo'").unwrap();
            assert_eq!(conn.affected_rows(), 2);
            assert_eq!(conn.last_insert_id(), 0);

            assert!(conn
                .query_first::<TestRow, _>("SELECT * FROM mysql.tbl WHERE a = 'bar'")
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
            assert_eq!(
                value,
                Bytes(std::iter::repeat_n(b'A', 20_000_000).collect())
            );
        }

        #[test]
        fn should_execute_statements_and_parse_results() {
            const CREATE_QUERY: &str = r"CREATE TEMPORARY TABLE
                mysql.tbl (a TEXT, b INT, c INT UNSIGNED, d DATE, e FLOAT)";
            const INSERT_STMT: &str = r"INSERT
                INTO mysql.tbl (a, b, c, d, e)
                VALUES (?, ?, ?, ?, ?)";

            type RowType = (Value, Value, Value, Value, Value);

            let row1 = (
                Bytes(b"hello".to_vec()),
                Int(-123_i64),
                Int(123_i64),
                Date(2014_u16, 5_u8, 5_u8, 0_u8, 0_u8, 0_u8, 0_u32),
                Float(123.123_f32),
            );
            let row2 = (Bytes(b"".to_vec()), NULL, NULL, NULL, Float(321.321_f32));

            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query_drop(CREATE_QUERY).unwrap();

            let insert_stmt = conn.prep(INSERT_STMT).unwrap();
            assert_eq!(insert_stmt.connection_id(), conn.connection_id());
            conn.exec_drop(
                &insert_stmt,
                (
                    from_value::<String>(row1.0.clone()),
                    from_value::<i32>(row1.1.clone()),
                    from_value::<u32>(row1.2.clone()),
                    from_value::<time::PrimitiveDateTime>(row1.3.clone()),
                    from_value::<f32>(row1.4.clone()),
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
                    from_value::<f32>(row2.4.clone()),
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
            assert_eq!(
                value,
                Bytes(std::iter::repeat_n(b'A', 20_000_000).collect())
            );
        }

        #[test]
        fn manually_closed_stmt() {
            let opts = get_opts().stmt_cache_size(1);
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
            conn.query_drop(
                "CREATE TEMPORARY TABLE mysql.tbl(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, a INT)",
            )
            .unwrap();
            conn.start_transaction(TxOpts::default())
                .map(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    assert_eq!(t.last_insert_id(), Some(1));
                    assert_eq!(t.affected_rows(), 1);
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.commit().unwrap();
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
            conn.start_transaction(TxOpts::default())
                .map(|mut t| {
                    t.query_drop("INSERT INTO tbl2(a) VALUES(1)").unwrap_err();
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
            conn.start_transaction(TxOpts::default())
                .map(|mut t| {
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(1)").unwrap();
                    t.query_drop("INSERT INTO mysql.tbl(a) VALUES(2)").unwrap();
                    t.rollback().unwrap();
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
            let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
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
            let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
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
                    stream.write_all(&cell_data)?;
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
            conn.query_drop("INSERT INTO `mysql`.`test` (`test`) VALUES ('foo');")
                .unwrap();
            assert_eq!(conn.affected_rows(), 1);
            conn.reset().unwrap();
            assert_eq!(conn.affected_rows(), 0);
            conn.query_drop("SELECT * FROM `mysql`.`test`;")
                .unwrap_err();
        }

        #[test]
        fn should_change_user() -> crate::Result<()> {
            /// Whether particular authentication plugin should be tested on the current database.
            type ShouldRunFn = fn(bool, (u16, u16, u16)) -> bool;
            /// Generates `CREATE USER` and `SET PASSWORD` statements
            type CreateUserFn = fn(bool, (u16, u16, u16), &str) -> Vec<String>;

            #[allow(clippy::type_complexity)]
            const TEST_MATRIX: [(&str, ShouldRunFn, CreateUserFn); 4] = [
                (
                    "mysql_old_password",
                    |is_mariadb, version| is_mariadb || version < (5, 7, 0),
                    |is_mariadb, version, pass| {
                        if is_mariadb {
                            vec![
                                "CREATE USER '__mats'@'%' IDENTIFIED WITH mysql_old_password"
                                    .into(),
                                "SET old_passwords=1".into(),
                                format!("ALTER USER '__mats'@'%' IDENTIFIED BY '{pass}'"),
                                "SET old_passwords=0".into(),
                            ]
                        } else if matches!(version, (5, 6, _)) {
                            vec![
                                "CREATE USER '__mats'@'%' IDENTIFIED WITH mysql_old_password"
                                    .into(),
                                format!("SET PASSWORD FOR '__mats'@'%' = OLD_PASSWORD('{pass}')"),
                            ]
                        } else {
                            vec![
                                "CREATE USER '__mats'@'%'".into(),
                                format!("SET PASSWORD FOR '__mats'@'%' = PASSWORD('{pass}')"),
                            ]
                        }
                    },
                ),
                (
                    "mysql_native_password",
                    |is_mariadb, version| is_mariadb || version < (8, 4, 0),
                    |is_mariadb, version, pass| {
                        if is_mariadb {
                            vec![
                                format!("CREATE USER '__mats'@'%' IDENTIFIED WITH mysql_native_password AS PASSWORD('{pass}')")
                            ]
                        } else if version < (8, 0, 0) {
                            vec![
                                "CREATE USER '__mats'@'%' IDENTIFIED WITH mysql_native_password"
                                    .into(),
                                "SET old_passwords = 0".into(),
                                format!("SET PASSWORD FOR '__mats'@'%' = PASSWORD('{pass}')"),
                            ]
                        } else {
                            vec![
                                format!("CREATE USER '__mats'@'%' IDENTIFIED WITH mysql_native_password BY '{pass}'")
                            ]
                        }
                    },
                ),
                (
                    "caching_sha2_password",
                    |is_mariadb, version| !is_mariadb && version >= (5, 8, 0),
                    |_is_mariadb, _version, pass| {
                        vec![
                            format!("CREATE USER '__mats'@'%' IDENTIFIED WITH caching_sha2_password BY '{pass}'")
                        ]
                    },
                ),
                (
                    "client_ed25519",
                    |is_mariadb, version| is_mariadb && version >= (10, 4, 0),
                    |_is_mariadb, _version, pass| {
                        vec![
                            format!("CREATE USER '__mats'@'%' IDENTIFIED WITH ed25519 AS PASSWORD('{pass}')")
                        ]
                    },
                ),
            ];

            fn random_pass() -> String {
                let mut rng = rand::thread_rng();
                let mut pass = [0u8; 10];
                pass.try_fill(&mut rng).unwrap();
                IntoIterator::into_iter(pass)
                    .map(|x| ((x % (123 - 97)) + 97) as char)
                    .collect()
            }

            let mut conn = Conn::new(get_opts()).unwrap();

            assert_eq!(
                conn.query_first::<Value, _>("SELECT @foo")
                    .unwrap()
                    .unwrap(),
                Value::NULL
            );

            conn.query_drop("SET @foo = 'foo'").unwrap();

            assert_eq!(
                conn.query_first::<String, _>("SELECT @foo")
                    .unwrap()
                    .unwrap(),
                "foo",
            );

            conn.change_user(Default::default()).unwrap();
            assert_eq!(
                conn.query_first::<Value, _>("SELECT @foo")
                    .unwrap()
                    .unwrap(),
                Value::NULL
            );

            for (plugin, should_run, create_statements) in TEST_MATRIX {
                dbg!(plugin);
                let is_mariadb = conn.0.mariadb_server_version.is_some();
                let version = conn.server_version();

                if should_run(is_mariadb, version) {
                    let pass = random_pass();

                    // (M)!50700 IF EXISTS: 5.7.0 (also on MariaDB) is minimum version that sees this clause
                    let statement =
                        "DROP USER /*!50700 IF EXISTS */ /*M!50700 IF EXISTS */ '__mats'";
                    // No IF EXISTS before 5.7 so the query may fail otherwise
                    _ = conn.query_drop(dbg!(statement));

                    for statement in create_statements(is_mariadb, version, &pass) {
                        conn.query_drop(dbg!(statement)).unwrap();
                    }

                    let mut conn2 = Conn::new(get_opts().secure_auth(false)).unwrap();
                    conn2
                        .change_user(
                            crate::ChangeUserOpts::default()
                                .with_db_name(None)
                                .with_user(Some("__mats".into()))
                                .with_pass(Some(pass)),
                        )
                        .unwrap();

                    let (db, user) = conn2
                        .query_first::<(Option<String>, String), _>("SELECT DATABASE(), USER();")
                        .unwrap()
                        .unwrap();
                    assert_eq!(db, None);
                    assert!(user.starts_with("__mats"));
                }
            }

            Ok(())
        }

        #[test]
        fn prep_exec() {
            let mut conn = Conn::new(get_opts()).unwrap();

            let stmt1 = conn.prep("SELECT :foo").unwrap();
            let stmt2 = conn.prep("SELECT :bar").unwrap();
            assert_eq!(
                conn.exec::<String, _, _>(&stmt1, params! { "foo" => "foo" })
                    .unwrap(),
                vec![String::from("foo")],
            );
            assert_eq!(
                conn.exec::<String, _, _>(&stmt2, params! { "bar" => "bar" })
                    .unwrap(),
                vec![String::from("bar")],
            );
        }

        #[test]
        fn should_connect_via_socket_for_127_0_0_1() {
            let opts = OptsBuilder::from_opts(get_opts());
            let mut conn = Conn::new(opts).unwrap();
            if conn.is_insecure() {
                assert!(
                    conn.is_socket(),
                    "Did not reconnect via socket {:?}",
                    (
                        conn.0.opts.get_prefer_socket(),
                        conn.0.opts.addr_is_loopback(),
                        conn.can_improved().and_then(|opts| {
                            opts.map(|opts| {
                                let mut new = crate::conn::Conn(Box::new(ConnInner::empty(opts)));
                                new.connect_stream().and_then(|_| {
                                    new.connect()?;
                                    Ok(new)
                                })
                            })
                            .transpose()
                        }),
                    )
                );
            }
        }

        #[test]
        fn should_connect_via_socket_localhost() {
            let opts = OptsBuilder::from_opts(get_opts()).ip_or_hostname(Some("localhost"));
            let mut conn = Conn::new(opts).unwrap();
            if conn.is_insecure() {
                assert!(
                    conn.is_socket(),
                    "Did not reconnect via socket {:?}",
                    (
                        conn.0.opts.get_prefer_socket(),
                        conn.0.opts.addr_is_loopback(),
                        conn.can_improved().and_then(|opts| {
                            opts.map(|opts| {
                                let mut new = crate::conn::Conn(Box::new(ConnInner::empty(opts)));
                                new.connect_stream().and_then(|_| {
                                    new.connect()?;
                                    Ok(new)
                                })
                            })
                            .transpose()
                        }),
                    )
                );
            }
        }

        /// QueryResult::drop hangs on connectivity errors (see [blackbeam/rust-mysql-simple#306][1]).
        ///
        /// [1]: https://github.com/blackbeam/rust-mysql-simple/issues/306
        #[test]
        fn issue_306() {
            let (tx, rx) = channel::<()>();
            let handle = spawn(move || {
                let mut c1 = Conn::new(get_opts()).unwrap();
                let c1_id = c1.connection_id();
                let mut c2 = Conn::new(get_opts()).unwrap();
                let query_result = c1.query_iter("DO 1; SELECT SLEEP(1); DO 2;").unwrap();
                c2.query_drop(format!("KILL {c1_id}")).unwrap();
                drop(c2);
                drop(query_result);
                tx.send(()).unwrap();
            });
            std::thread::sleep(Duration::from_secs(2));
            assert!(rx.try_recv().is_ok());
            handle.join().unwrap();
        }

        #[test]
        fn reset_does_work() {
            let mut c = Conn::new(get_opts()).unwrap();
            let cid = c.connection_id();
            c.query_drop("SET @foo = 'foo'").unwrap();
            assert_eq!(
                c.query_first::<String, _>("SELECT @foo").unwrap().unwrap(),
                "foo",
            );
            c.reset().unwrap();
            assert_eq!(cid, c.connection_id());
            assert_eq!(
                c.query_first::<Value, _>("SELECT @foo").unwrap().unwrap(),
                Value::NULL
            );
        }

        #[test]
        fn should_drop_multi_result_set() {
            let opts = OptsBuilder::from_opts(get_opts()).db_name(Some("mysql"));
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

            let mut query_result = conn
                .query_iter(
                    r"
                SELECT * FROM TEST_TABLE;
                INSERT INTO TEST_TABLE (name) VALUES ('one');
                DO 0;",
                )
                .unwrap();

            while let Some(result) = query_result.iter() {
                result.affected_rows();
            }
        }

        #[test]
        fn should_handle_multi_result_set() {
            let opts = OptsBuilder::from_opts(get_opts())
                .prefer_socket(false)
                .db_name(Some("mysql"));
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
                let result_set = query_result
                    .by_ref()
                    .map(|row| row.unwrap().unwrap().pop().unwrap())
                    .collect::<Vec<crate::Value>>();
                assert_eq!(result_set, vec![Bytes(b"3".to_vec()), Bytes(b"4".to_vec())]);
            }
            let mut result = conn.query_iter("SELECT 1; SELECT 2; SELECT 3;").unwrap();
            let mut i = 0;
            while let Some(result_set) = result.iter() {
                i += 1;
                for row in result_set {
                    match i {
                        1 => assert_eq!(row.unwrap().unwrap(), vec![Bytes(b"1".to_vec())]),
                        2 => assert_eq!(row.unwrap().unwrap(), vec![Bytes(b"2".to_vec())]),
                        3 => assert_eq!(row.unwrap().unwrap(), vec![Bytes(b"3".to_vec())]),
                        _ => unreachable!(),
                    }
                }
            }
            assert_eq!(i, 3);
        }

        #[test]
        fn issue_273() {
            let opts = OptsBuilder::from_opts(get_opts()).prefer_socket(false);
            let mut conn = Conn::new(opts).unwrap();

            "DROP FUNCTION IF EXISTS f1".run(&mut conn).unwrap();
            r"CREATE DEFINER=`root`@`localhost` FUNCTION `f1`(p_arg INT, p_arg2 INT) RETURNS int
            DETERMINISTIC
            BEGIN
                RETURN p_arg + p_arg2;
            END"
            .run(&mut conn)
            .unwrap();

            "SELECT f1(?, ?)"
                .with((100u8, 100u8))
                .run(&mut conn)
                .unwrap();
        }

        #[test]
        fn issue_285() {
            let (tx, rx) = sync_channel::<()>(0);

            let handle = std::thread::spawn(move || {
                let mut conn = Conn::new(get_opts()).unwrap();
                const INVALID_SQL: &str = r#"
                CREATE TEMPORARY TABLE IF NOT EXISTS `user_details` (
                    `user_id` int(11) NOT NULL AUTO_INCREMENT,
                    `username` varchar(255) DEFAULT NULL,
                    `first_name` varchar(50) DEFAULT NULL,
                    `last_name` varchar(50) DEFAULT NULL,
                    PRIMARY KEY (`user_id`)
                );

                INSERT INTO `user_details` (`user_id`, `username`, `first_name`, `last_name`)
                VALUES (1, 'rogers63', 'david')
                "#;

                conn.query_iter(INVALID_SQL).unwrap();
                tx.send(()).unwrap();
            });

            match rx.recv_timeout(Duration::from_secs(100_000)) {
                Ok(_) => handle.join().unwrap(),
                Err(_) => panic!("test failed"),
            }
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
                conn.exec_first::<crate::Row, _, _>(&stmt, params! {"a" => 1, "b" => 2, "c" => 3,});
            match result {
                Err(DriverError(MissingNamedParameter(ref x))) if x == "d" => (),
                _ => panic!("MissingNamedParameter error expected"),
            }
        }

        #[test]
        fn should_return_error_on_named_params_for_positional_statement() {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT ?, ?, ?, ?, ?").unwrap();
            let result = conn.exec_drop(&stmt, params! {"a" => 1, "b" => 2, "c" => 3,});
            match result {
                Err(DriverError(NamedParamsForPositionalQuery)) => (),
                _ => panic!("NamedParamsForPositionalQuery error expected"),
            }
        }

        #[test]
        fn should_handle_tcp_connect_timeout() {
            use crate::error::{DriverError::ConnectTimeout, Error::DriverError};

            let opts = OptsBuilder::from_opts(get_opts())
                .prefer_socket(false)
                .tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            assert!(Conn::new(opts).unwrap().ping().is_ok());

            let opts = OptsBuilder::from_opts(get_opts())
                .prefer_socket(false)
                .tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)))
                .ip_or_hostname(Some("192.168.255.255"));
            match Conn::new(opts).unwrap_err() {
                DriverError(ConnectTimeout) => {}
                err => panic!("Unexpected error: {}", err),
            }
        }

        #[test]
        fn should_set_additional_capabilities() {
            use crate::consts::CapabilityFlags;

            let opts = OptsBuilder::from_opts(get_opts())
                .additional_capabilities(CapabilityFlags::CLIENT_FOUND_ROWS);

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
            let port = 28000 + (rand::random::<u16>() % 2000);
            let opts = OptsBuilder::from_opts(get_opts())
                .prefer_socket(false)
                .ip_or_hostname(Some("localhost"))
                .bind_address(Some(([127, 0, 0, 1], port)));
            let conn = Conn::new(opts).unwrap();
            let debug_format: String = format!("{:?}", conn);
            let expected_1 = format!("addr: V4(127.0.0.1:{})", port);
            let expected_2 = format!("addr: 127.0.0.1:{}", port);
            assert!(
                debug_format.contains(&expected_1) || debug_format.contains(&expected_2),
                "debug_format: {}",
                debug_format
            );
        }

        #[test]
        fn should_bind_before_connect_with_timeout() {
            let port = 30000 + (rand::random::<u16>() % 2000);
            let opts = OptsBuilder::from_opts(get_opts())
                .prefer_socket(false)
                .ip_or_hostname(Some("localhost"))
                .bind_address(Some(([127, 0, 0, 1], port)))
                .tcp_connect_timeout(Some(::std::time::Duration::from_millis(1000)));
            let mut conn = Conn::new(opts).unwrap();
            assert!(conn.ping().is_ok());
            let debug_format: String = format!("{:?}", conn);
            let expected_1 = format!("addr: V4(127.0.0.1:{})", port);
            let expected_2 = format!("addr: 127.0.0.1:{}", port);
            assert!(
                debug_format.contains(&expected_1) || debug_format.contains(&expected_2),
                "debug_format: {}",
                debug_format
            );
        }

        #[test]
        fn should_not_cache_statements_if_stmt_cache_size_is_zero() {
            let opts = OptsBuilder::from_opts(get_opts()).stmt_cache_size(0);
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
            let opts = OptsBuilder::from_opts(get_opts()).stmt_cache_size(3);
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
                .0
                .stmt_cache
                .iter()
                .map(|(_, entry)| &**entry.query.0.as_ref())
                .collect::<Vec<&[u8]>>();
            order.sort();
            assert_eq!(order, &[b"DO 3", b"DO 5", b"DO 6"]);
        }

        #[test]
        fn should_handle_json_columns() {
            use crate::{Deserialized, Serialized};
            use serde::{Deserialize, Serialize};
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
            let opts = OptsBuilder::from_opts(
                get_opts().connect_attrs::<String, String>(Some(Default::default())),
            );
            let mut conn = Conn::new(opts).unwrap();

            let support_connect_attrs = match (conn.0.server_version, conn.0.mariadb_server_version)
            {
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
                if (0..=128).contains(&attrs_size) {
                    panic!("The system variable `performance_schema_session_connect_attrs_size` is {}. Restart the MySQL server with `--performance_schema_session_connect_attrs_size=-1` to pass the test.", attrs_size);
                }

                fn assert_connect_attrs(conn: &mut Conn, expected_values: &[(&str, &str)]) {
                    let mut actual_values = HashMap::new();
                    for row in conn.query_iter("SELECT attr_name, attr_value FROM performance_schema.session_account_connect_attrs WHERE processlist_id = connection_id()").unwrap() {
                        let (name, value) = from_row::<(String, String)>(row.unwrap());
                        actual_values.insert(name, value);
                    }

                    for (name, value) in expected_values {
                        assert_eq!(actual_values.get(*name), Some(&value.to_string()));
                    }
                }

                let pid = process::id().to_string();
                let prog_name = std::env::args_os()
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
                    ("program_name", &prog_name),
                ];

                // No connect attributes are added.
                assert_connect_attrs(&mut conn, &expected_values);

                // Connect attributes are added.
                let opts = OptsBuilder::from_opts(get_opts());
                let mut connect_attrs = HashMap::with_capacity(3);
                connect_attrs.insert("foo", "foo val");
                connect_attrs.insert("bar", "bar val");
                connect_attrs.insert("program_name", "my program name");
                let mut conn = Conn::new(opts.connect_attrs(Some(connect_attrs))).unwrap();
                expected_values.pop(); // remove program_name at the last
                expected_values.push(("foo", "foo val"));
                expected_values.push(("bar", "bar val"));
                expected_values.push(("program_name", "my program name"));
                assert_connect_attrs(&mut conn, &expected_values);
            }
        }

        #[test]
        #[cfg(feature = "binlog")]
        fn should_read_binlog() -> crate::Result<()> {
            use std::{
                collections::HashMap, sync::mpsc::sync_channel, thread::spawn, time::Duration,
            };

            fn gen_dummy_data() -> crate::Result<()> {
                let mut conn = Conn::new(get_opts())?;

                "CREATE TABLE IF NOT EXISTS customers (customer_id int not null)".run(&mut conn)?;

                for i in 0_u8..100 {
                    "INSERT INTO customers(customer_id) VALUES (?)"
                        .with((i,))
                        .run(&mut conn)?;
                }

                "DROP TABLE customers".run(&mut conn)?;

                Ok(())
            }

            fn get_conn() -> crate::Result<(Conn, Vec<u8>, u64)> {
                let mut conn = Conn::new(get_opts())?;

                if let Ok(Some(gtid_mode)) =
                    "SELECT @@GLOBAL.GTID_MODE".first::<String, _>(&mut conn)
                {
                    if !gtid_mode.starts_with("ON") {
                        panic!(
                            "GTID_MODE is disabled \
                                (enable using --gtid_mode=ON --enforce_gtid_consistency=ON)"
                        );
                    }
                }

                let row: crate::Row = "SHOW BINARY LOGS".first(&mut conn)?.unwrap();
                let filename = row.get(0).unwrap();
                let position = row.get(1).unwrap();

                gen_dummy_data().unwrap();
                Ok((conn, filename, position))
            }

            // iterate using COM_BINLOG_DUMP
            let (conn, filename, pos) = get_conn().unwrap();
            let is_mariadb = conn.0.mariadb_server_version.is_some();

            let binlog_stream = conn
                .get_binlog_stream(BinlogRequest::new(12).with_filename(filename).with_pos(pos))
                .unwrap();

            let mut events_num = 0;
            let (tx, rx) = sync_channel(0);
            spawn(move || {
                for event in binlog_stream {
                    tx.send(event).unwrap();
                }
            });
            let mut tmes = HashMap::new();
            while let Ok(event) = rx.recv_timeout(Duration::from_secs(1)) {
                let event = event.unwrap();
                events_num += 1;

                // assert that event type is known
                event.header().event_type().unwrap();

                // iterate over rows of an event
                match event.read_data()?.unwrap() {
                    EventData::TableMapEvent(tme) => {
                        tmes.insert(tme.table_id(), tme.into_owned());
                    }
                    EventData::RowsEvent(re) => {
                        for row in re.rows(&tmes[&re.table_id()]) {
                            row.unwrap();
                        }
                    }
                    _ => (),
                }
            }
            assert!(events_num > 0);

            if !is_mariadb {
                // iterate using COM_BINLOG_DUMP_GTID
                let (conn, filename, pos) = get_conn().unwrap();

                let binlog_stream = conn
                    .get_binlog_stream(
                        BinlogRequest::new(13)
                            .with_use_gtid(true)
                            .with_filename(filename)
                            .with_pos(pos),
                    )
                    .unwrap();

                let mut events_num = 0;
                let (tx, rx) = sync_channel(0);
                spawn(move || {
                    for event in binlog_stream {
                        tx.send(event).unwrap();
                    }
                });
                let mut tmes = HashMap::new();
                while let Ok(event) = rx.recv_timeout(Duration::from_secs(1)) {
                    let event = event.unwrap();
                    events_num += 1;

                    // assert that event type is known
                    event.header().event_type().unwrap();

                    // iterate over rows of an event
                    match event.read_data()?.unwrap() {
                        EventData::TableMapEvent(tme) => {
                            tmes.insert(tme.table_id(), tme.into_owned());
                        }
                        EventData::RowsEvent(re) => {
                            for row in re.rows(&tmes[&re.table_id()]) {
                                row.unwrap();
                            }
                        }
                        _ => (),
                    }
                }
                assert!(events_num > 0);
            }

            // iterate using COM_BINLOG_DUMP with BINLOG_DUMP_NON_BLOCK flag
            let (conn, filename, pos) = get_conn().unwrap();

            let binlog_stream = conn
                .get_binlog_stream(
                    BinlogRequest::new(14)
                        .with_filename(filename)
                        .with_pos(pos)
                        .with_flags(crate::BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK),
                )
                .unwrap();

            events_num = 0;
            for event in binlog_stream {
                let event = event.unwrap();
                events_num += 1;
                event.header().event_type().unwrap();
                event.read_data()?;
            }
            assert!(events_num > 0);

            Ok(())
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;

        use crate::{params, prelude::*, test_misc::get_opts, Conn, Value::NULL};

        #[bench]
        fn simple_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| {
                let _ = conn.query_drop("DO 1");
            })
        }

        #[bench]
        fn prepared_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("DO 1").unwrap();
            bencher.iter(|| {
                let _ = conn.exec_drop(&stmt, ()).unwrap();
            })
        }

        #[bench]
        fn prepare_and_exec(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| {
                let stmt = conn.prep("SELECT ?").unwrap();
                let _ = conn.exec_drop(&stmt, (0,)).unwrap();
            })
        }

        #[bench]
        fn simple_query_row(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| {
                let _ = conn.query_drop("SELECT 1").unwrap();
            })
        }

        #[bench]
        fn simple_prepared_query_row(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT 1").unwrap();
            bencher.iter(|| {
                let _ = conn.exec_drop(&stmt, ()).unwrap();
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_param(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT ?").unwrap();
            bencher.iter(|| {
                let _ = conn.exec_drop(&stmt, (0,)).unwrap();
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_named_param(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT :a").unwrap();
            bencher.iter(|| {
                let _ = conn.exec_drop(&stmt, params! {"a" => 0}).unwrap();
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_5_params(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT ?, ?, ?, ?, ?").unwrap();
            let params = (42i8, b"123456".to_vec(), 1.618f64, NULL, 1i8);
            bencher.iter(|| {
                let _ = conn.exec_drop(&stmt, &params).unwrap();
            })
        }

        #[bench]
        fn simple_prepared_query_row_with_5_named_params(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn
                .prep("SELECT :one, :two, :three, :four, :five")
                .unwrap();
            bencher.iter(|| {
                let _ = conn.exec_drop(
                    &stmt,
                    params! {
                        "one" => 42i8,
                        "two" => b"123456",
                        "three" => 1.618f64,
                        "four" => NULL,
                        "five" => 1i8,
                    },
                );
            })
        }

        #[bench]
        fn select_large_string(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            bencher.iter(|| {
                let _ = conn.query_drop("SELECT REPEAT('A', 10000)").unwrap();
            })
        }

        #[bench]
        fn select_prepared_large_string(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            let stmt = conn.prep("SELECT REPEAT('A', 10000)").unwrap();
            bencher.iter(|| {
                let _ = conn.exec_drop(&stmt, ()).unwrap();
            })
        }

        #[bench]
        fn many_small_rows(bencher: &mut test::Bencher) {
            let mut conn = Conn::new(get_opts()).unwrap();
            conn.query_drop("CREATE TEMPORARY TABLE mysql.x (id INT)")
                .unwrap();
            for _ in 0..512 {
                conn.query_drop("INSERT INTO mysql.x VALUES (256)").unwrap();
            }
            let stmt = conn.prep("SELECT * FROM mysql.x").unwrap();
            bencher.iter(|| {
                let _ = conn.exec_drop(&stmt, ()).unwrap();
            });
        }
    }
}
