// Copyright (c) 2020 rust-mysql-simple contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use percent_encoding::{percent_decode, percent_decode_str};
use url::Url;

use std::{
    borrow::Cow, collections::HashMap, hash::Hash, net::SocketAddr, path::Path, str::FromStr,
    time::Duration,
};

use crate::{consts::CapabilityFlags, LocalInfileHandler, UrlError};

/// Default value for client side per-connection statement cache.
pub const DEFAULT_STMT_CACHE_SIZE: usize = 32;

/// Ssl Options.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct SslOpts {
    pkcs12_path: Option<Cow<'static, Path>>,
    password: Option<Cow<'static, str>>,
    root_cert_path: Option<Cow<'static, Path>>,
    root_cert_format: Option<CertificateFormat>,
    skip_domain_validation: bool,
    accept_invalid_certs: bool,
}

impl SslOpts {
    /// Sets path to the pkcs12 archive.
    ///
    /// If you have the client's private key and certificate in PEM format, you
    /// can translate them to pkcs12 using `openssl`:
    ///
    /// ```text
    /// openssl pkcs12 -password pass: -export -out path.p12 -inkey privatekey.pem -in cert.pem -no-CAfile
    /// ```
    pub fn with_pkcs12_path<T: Into<Cow<'static, Path>>>(mut self, pkcs12_path: Option<T>) -> Self {
        self.pkcs12_path = pkcs12_path.map(Into::into);
        self
    }

    /// Sets the password for a pkcs12 archive (defaults to `None`).
    pub fn with_password<T: Into<Cow<'static, str>>>(mut self, password: Option<T>) -> Self {
        self.password = password.map(Into::into);
        self
    }

    /// Sets path to a der certificate of the root that connector will trust.
    ///
    /// If you have a certificate in PEM format, you can translate it to der
    /// using `openssl`:
    ///
    /// ```text
    /// openssl x509 -outform der -in rootca.pem -out rootca.der
    /// ```
    pub fn with_root_cert_path<T: Into<Cow<'static, Path>>>(
        mut self,
        root_cert_path: Option<T>,
    ) -> Self {
        self.root_cert_path = root_cert_path.map(Into::into);
        self.root_cert_format = Some(CertificateFormat::Der);
        self
    }

    /// Sets path to a pem certificate of the root that connector will trust.
    pub fn with_pem_root_cert_path<T: Into<Cow<'static, Path>>>(
        mut self,
        root_cert_path: Option<T>,
    ) -> Self {
        self.root_cert_path = root_cert_path.map(Into::into);
        self.root_cert_format = Some(CertificateFormat::Pem);
        self
    }

    /// The way to not validate the server's domain
    /// name against its certificate (defaults to `false`).
    pub fn with_danger_skip_domain_validation(mut self, value: bool) -> Self {
        self.skip_domain_validation = value;
        self
    }

    /// If `true` then client will accept invalid certificate (expired, not trusted, ..)
    /// (defaults to `false`).
    pub fn with_danger_accept_invalid_certs(mut self, value: bool) -> Self {
        self.accept_invalid_certs = value;
        self
    }

    pub fn pkcs12_path(&self) -> Option<&Path> {
        self.pkcs12_path.as_ref().map(|x| x.as_ref())
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(AsRef::as_ref)
    }

    pub fn root_cert_path(&self) -> Option<&Path> {
        self.root_cert_path.as_ref().map(AsRef::as_ref)
    }

    pub fn root_cert_format(&self) -> Option<CertificateFormat> {
        self.root_cert_format.clone()
    }

    pub fn skip_domain_validation(&self) -> bool {
        self.skip_domain_validation
    }

    pub fn accept_invalid_certs(&self) -> bool {
        self.accept_invalid_certs
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CertificateFormat {
    Der,
    Pem,
}

/// Options structure is quite large so we'll store it separately.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct InnerOpts {
    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    ip_or_hostname: url::Host,
    /// TCP port of mysql server (defaults to `3306`).
    tcp_port: u16,
    /// Path to unix socket on unix or pipe name on windows (defaults to `None`).
    ///
    /// Can be defined using `socket` connection url parameter.
    socket: Option<String>,
    /// User (defaults to `None`).
    user: Option<String>,
    /// Password (defaults to `None`).
    pass: Option<String>,
    /// Database name (defaults to `None`).
    db_name: Option<String>,

    /// The timeout for each attempt to read from the server.
    read_timeout: Option<Duration>,

    /// The timeout for each attempt to write to the server.
    write_timeout: Option<Duration>,

    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket (or named pipe on windows) after TCP
    /// connection to `127.0.0.1` if `true`.
    ///
    /// Will fall back to TCP on error. Use `socket` option to enforce socket connection.
    ///
    /// Can be defined using `prefer_socket` connection url parameter.
    prefer_socket: bool,

    /// Whether to enable `TCP_NODELAY` (defaults to `true`).
    ///
    /// This option disables Nagle's algorithm, which can cause unusually high latency (~40ms) at
    /// some cost to maximum throughput. See #132.
    tcp_nodelay: bool,

    /// TCP keep alive time for mysql connection.
    ///
    /// Can be defined using `tcp_keepalive_time_ms` connection url parameter.
    tcp_keepalive_time: Option<u32>,

    /// Commands to execute on each new database connection.
    init: Vec<String>,

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    ssl_opts: Option<SslOpts>,

    /// Callback to handle requests for local files.
    ///
    /// These are caused by using `LOAD DATA LOCAL INFILE` queries.
    /// The callback is passed the filename, and a `Write`able object
    /// to receive the contents of that file.
    ///
    /// If unset, the default callback will read files relative to
    /// the current directory.
    local_infile_handler: Option<LocalInfileHandler>,

    /// Tcp connect timeout (defaults to `None`).
    ///
    /// Can be defined using `tcp_connect_timeout_ms` connection url parameter.
    tcp_connect_timeout: Option<Duration>,

    /// Bind address for a client (defaults to `None`).
    ///
    /// Use carefully. Will probably make pool unusable because of *address already in use*
    /// errors.
    bind_address: Option<SocketAddr>,

    /// Number of prepared statements cached on the client side (per connection).
    /// Defaults to [`DEFAULT_STMT_CACHE_SIZE`].
    ///
    /// Can be defined using `stmt_cache_size` connection url parameter.
    stmt_cache_size: usize,

    /// If not `None`, then client will ask for compression if server supports it
    /// (defaults to `None`).
    ///
    /// Can be defined using `compress` connection url parameter with values `true`, `fast`, `best`,
    /// `0`, `1`, ..., `9`.
    ///
    /// Note that compression level defined here will affect only outgoing packets.
    compress: Option<crate::Compression>,

    /// Additional client capabilities to set (defaults to empty).
    ///
    /// This value will be OR'ed with other client capabilities during connection initialisation.
    ///
    /// ### Note
    ///
    /// It is a good way to set something like `CLIENT_FOUND_ROWS` but you should note that it
    /// won't let you to interfere with capabilities managed by other options (like
    /// `CLIENT_SSL` or `CLIENT_COMPRESS`). Also note that some capabilities are reserved,
    /// pointless or may broke the connection, so this option should be used with caution.
    additional_capabilities: CapabilityFlags,

    /// Connect attributes
    connect_attrs: HashMap<String, String>,

    /// For tests only
    #[cfg(test)]
    pub injected_socket: Option<String>,
}

impl Default for InnerOpts {
    fn default() -> Self {
        InnerOpts {
            ip_or_hostname: url::Host::Domain(String::from("localhost")),
            tcp_port: 3306,
            socket: None,
            user: None,
            pass: None,
            db_name: None,
            read_timeout: None,
            write_timeout: None,
            prefer_socket: true,
            init: vec![],
            ssl_opts: None,
            tcp_keepalive_time: None,
            tcp_nodelay: true,
            local_infile_handler: None,
            tcp_connect_timeout: None,
            bind_address: None,
            stmt_cache_size: DEFAULT_STMT_CACHE_SIZE,
            compress: None,
            additional_capabilities: CapabilityFlags::empty(),
            connect_attrs: HashMap::new(),
            #[cfg(test)]
            injected_socket: None,
        }
    }
}

/// Mysql connection options.
///
/// Build one with [`OptsBuilder`](struct.OptsBuilder.html).
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct Opts(pub(crate) Box<InnerOpts>);

impl Opts {
    #[doc(hidden)]
    pub fn addr_is_loopback(&self) -> bool {
        match self.0.ip_or_hostname {
            url::Host::Domain(ref name) => name == "localhost",
            url::Host::Ipv4(ref addr) => addr.is_loopback(),
            url::Host::Ipv6(ref addr) => addr.is_loopback(),
        }
    }

    pub fn from_url(url: &str) -> Result<Opts, UrlError> {
        from_url(url)
    }

    pub(crate) fn get_host(&self) -> url::Host {
        self.0.ip_or_hostname.clone()
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn get_ip_or_hostname(&self) -> Cow<str> {
        self.0.ip_or_hostname.to_string().into()
    }
    /// TCP port of mysql server (defaults to `3306`).
    pub fn get_tcp_port(&self) -> u16 {
        self.0.tcp_port
    }
    /// Socket path on unix or pipe name on windows (defaults to `None`).
    pub fn get_socket(&self) -> Option<&str> {
        self.0.socket.as_ref().map(|x| &**x)
    }
    /// User (defaults to `None`).
    pub fn get_user(&self) -> Option<&str> {
        self.0.user.as_ref().map(|x| &**x)
    }
    /// Password (defaults to `None`).
    pub fn get_pass(&self) -> Option<&str> {
        self.0.pass.as_ref().map(|x| &**x)
    }
    /// Database name (defaults to `None`).
    pub fn get_db_name(&self) -> Option<&str> {
        self.0.db_name.as_ref().map(|x| &**x)
    }

    /// The timeout for each attempt to write to the server.
    pub fn get_read_timeout(&self) -> Option<&Duration> {
        self.0.read_timeout.as_ref()
    }

    /// The timeout for each attempt to write to the server.
    pub fn get_write_timeout(&self) -> Option<&Duration> {
        self.0.write_timeout.as_ref()
    }

    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket (or named pipe on windows) after TCP connection
    /// to `127.0.0.1` if `true`.
    ///
    /// Will fall back to TCP on error. Use `socket` option to enforce socket connection.
    pub fn get_prefer_socket(&self) -> bool {
        self.0.prefer_socket
    }
    // XXX: Wait for keepalive_timeout stabilization
    /// Commands to execute on each new database connection.
    pub fn get_init(&self) -> Vec<String> {
        self.0.init.clone()
    }

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    pub fn get_ssl_opts(&self) -> Option<&SslOpts> {
        self.0.ssl_opts.as_ref()
    }

    fn set_prefer_socket(&mut self, val: bool) {
        self.0.prefer_socket = val;
    }

    /// Whether `TCP_NODELAY` will be set for mysql connection.
    pub fn get_tcp_nodelay(&self) -> bool {
        self.0.tcp_nodelay
    }

    /// TCP keep alive time for mysql connection.
    pub fn get_tcp_keepalive_time_ms(&self) -> Option<u32> {
        self.0.tcp_keepalive_time
    }

    /// Callback to handle requests for local files.
    pub fn get_local_infile_handler(&self) -> Option<&LocalInfileHandler> {
        self.0.local_infile_handler.as_ref()
    }

    /// Tcp connect timeout (defaults to `None`).
    pub fn get_tcp_connect_timeout(&self) -> Option<Duration> {
        self.0.tcp_connect_timeout
    }

    /// Bind address for a client (defaults to `None`).
    ///
    /// Use carefully. Will probably make pool unusable because of *address already in use*
    /// errors.
    pub fn bind_address(&self) -> Option<&SocketAddr> {
        self.0.bind_address.as_ref()
    }

    /// Number of prepared statements cached on the client side (per connection).
    /// Defaults to [`DEFAULT_STMT_CACHE_SIZE`].
    ///
    /// Can be defined using `stmt_cache_size` connection url parameter.
    pub fn get_stmt_cache_size(&self) -> usize {
        self.0.stmt_cache_size
    }

    /// If not `None`, then client will ask for compression if server supports it
    /// (defaults to `None`).
    ///
    /// Can be defined using `compress` connection url parameter with values:
    /// * `true` - library defined default compression level;
    /// * `fast` - library defined fast compression level;
    /// * `best` - library defined best compression level;
    /// * `0`, `1`, ..., `9` - explicitly defined compression level where `0` stands for
    ///   "no compression";
    ///
    /// Note that compression level defined here will affect only outgoing packets.
    pub fn get_compress(&self) -> Option<crate::Compression> {
        self.0.compress
    }

    /// Additional client capabilities to set (defaults to empty).
    ///
    /// This value will be OR'ed with other client capabilities during connection initialisation.
    ///
    /// ### Note
    ///
    /// It is a good way to set something like `CLIENT_FOUND_ROWS` but you should note that it
    /// won't let you to interfere with capabilities managed by other options (like
    /// `CLIENT_SSL` or `CLIENT_COMPRESS`). Also note that some capabilities are reserved,
    /// pointless or may broke the connection, so this option should be used with caution.
    pub fn get_additional_capabilities(&self) -> CapabilityFlags {
        self.0.additional_capabilities
    }

    /// Connect attributes
    ///
    /// This value is sent to the server as custom name-value attributes.
    /// You can see them from performance_schema tables: [`session_account_connect_attrs`
    /// and `session_connect_attrs`][attr_tables] when all of the following conditions
    /// are met.
    ///
    /// * The server is MySQL 5.6 or later, or MariaDB 10.0 or later.
    /// * [`performance_schema`] is on.
    /// * [`performance_schema_session_connect_attrs_size`] is -1 or big enough
    ///   to store specified attributes.
    ///
    /// ### Note
    ///
    /// Attribute names that begin with an underscore (`_`) are not set by
    /// application programs because they are reserved for internal use.
    ///
    /// The following attributes are sent in addition to ones set by programs.
    ///
    /// name            | value
    /// ----------------|--------------------------
    /// _client_name    | The client library name (`rust-mysql-simple`)
    /// _client_version | The client library version
    /// _os             | The operation system (`target_os` cfg feature)
    /// _pid            | The client proces ID
    /// _platform       | The machine platform (`target_arch` cfg feature)
    /// program_name    | The first element of `std::env::args` if program_name isn't set by programs.
    ///
    /// [attr_tables]: https://dev.mysql.com/doc/refman/en/performance-schema-connection-attribute-tables.html
    /// [`performance_schema`]: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-system-variables.html#sysvar_performance_schema
    /// [`performance_schema_session_connect_attrs_size`]: https://dev.mysql.com/doc/refman/en/performance-schema-system-variables.html#sysvar_performance_schema_session_connect_attrs_size
    ///
    pub fn get_connect_attrs(&self) -> &HashMap<String, String> {
        &self.0.connect_attrs
    }
}

/// Provides a way to build [`Opts`](struct.Opts.html).
///
/// ```ignore
/// let mut ssl_opts = SslOpts::default();
/// ssl_opts = ssl_opts.with_pkcs12_path(Some(Path::new("/foo/cert.p12")))
///         .with_root_ca_path(Some(Path::new("/foo/root_ca.der")));
///
/// // You can create new default builder
/// let mut builder = OptsBuilder::new();
/// builder = builder.ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"))
///        .ssl_opts(Some(ssl_opts));
///
/// // Or use existing T: Into<Opts>
/// let builder = OptsBuilder::from_opts(existing_opts)
///        .ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"));
/// ```
///
/// ## Connection URL
///
/// `Opts` also could be constructed using connection URL. See docs on `OptsBuilder`'s methods for
/// the list of options available via URL.
///
/// Example:
///
/// ```ignore
/// let connection_url = "mysql://root:password@localhost:3307/mysql?prefer_socket=false";
/// let pool = my::Pool::new(connection_url).unwrap();
/// ```
#[derive(Debug)]
pub struct OptsBuilder {
    opts: Opts,
}

impl OptsBuilder {
    pub fn new() -> Self {
        OptsBuilder::default()
    }

    pub fn from_opts<T: Into<Opts>>(opts: T) -> Self {
        OptsBuilder { opts: opts.into() }
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    ///
    /// **Note:** IPv6 addresses must be given in square brackets, e.g. `[::1]`.
    pub fn ip_or_hostname<T: Into<String>>(mut self, ip_or_hostname: Option<T>) -> Self {
        let new = ip_or_hostname.map(Into::into).unwrap_or("127.0.0.1".into());
        self.opts.0.ip_or_hostname = url::Host::parse(&new)
            .map(|host| host.to_owned())
            .unwrap_or_else(|_| url::Host::Domain(new.to_owned()));
        self
    }

    /// TCP port of mysql server (defaults to `3306`).
    pub fn tcp_port(mut self, tcp_port: u16) -> Self {
        self.opts.0.tcp_port = tcp_port;
        self
    }

    /// Socket path on unix or pipe name on windows (defaults to `None`).
    ///
    /// Can be defined using `socket` connection url parameter.
    pub fn socket<T: Into<String>>(mut self, socket: Option<T>) -> Self {
        self.opts.0.socket = socket.map(Into::into);
        self
    }

    /// User (defaults to `None`).
    pub fn user<T: Into<String>>(mut self, user: Option<T>) -> Self {
        self.opts.0.user = user.map(Into::into);
        self
    }

    /// Password (defaults to `None`).
    pub fn pass<T: Into<String>>(mut self, pass: Option<T>) -> Self {
        self.opts.0.pass = pass.map(Into::into);
        self
    }

    /// Database name (defaults to `None`).
    pub fn db_name<T: Into<String>>(mut self, db_name: Option<T>) -> Self {
        self.opts.0.db_name = db_name.map(Into::into);
        self
    }

    /// The timeout for each attempt to read from the server (defaults to `None`).
    ///
    /// Note that named pipe connection will ignore duration's `nanos`, and also note that
    /// it is an error to pass the zero `Duration` to this method.
    pub fn read_timeout(mut self, read_timeout: Option<Duration>) -> Self {
        self.opts.0.read_timeout = read_timeout;
        self
    }

    /// The timeout for each attempt to write to the server (defaults to `None`).
    ///
    /// Note that named pipe connection will ignore duration's `nanos`, and also note that
    /// it is likely error to pass the zero `Duration` to this method.
    pub fn write_timeout(mut self, write_timeout: Option<Duration>) -> Self {
        self.opts.0.write_timeout = write_timeout;
        self
    }

    /// TCP keep alive time for mysql connection (defaults to `None`). Available as
    /// `tcp_keepalive_time_ms` url parameter.
    ///
    /// Can be defined using `tcp_keepalive_time_ms` connection url parameter.
    pub fn tcp_keepalive_time_ms(mut self, tcp_keepalive_time_ms: Option<u32>) -> Self {
        self.opts.0.tcp_keepalive_time = tcp_keepalive_time_ms;
        self
    }

    /// Set the `TCP_NODELAY` option for the mysql connection (defaults to `true`).
    ///
    /// Setting this option to false re-enables Nagle's algorithm, which can cause unusually high
    /// latency (~40ms) but may increase maximum throughput. See #132.
    pub fn tcp_nodelay(mut self, nodelay: bool) -> Self {
        self.opts.0.tcp_nodelay = nodelay;
        self
    }

    /// Prefer socket connection (defaults to `true`). Available as `prefer_socket` url parameter
    /// with value `true` or `false`.
    ///
    /// Will reconnect via socket (on named pipe on windows) after TCP connection
    /// to `127.0.0.1` if `true`.
    ///
    /// Will fall back to TCP on error. Use `socket` option to enforce socket connection.
    ///
    /// Can be defined using `prefer_socket` connection url parameter.
    pub fn prefer_socket(mut self, prefer_socket: bool) -> Self {
        self.opts.0.prefer_socket = prefer_socket;
        self
    }

    /// Commands to execute on each new database connection.
    pub fn init<T: Into<String>>(mut self, init: Vec<T>) -> Self {
        self.opts.0.init = init.into_iter().map(Into::into).collect();
        self
    }

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    pub fn ssl_opts<T: Into<Option<SslOpts>>>(mut self, ssl_opts: T) -> Self {
        self.opts.0.ssl_opts = ssl_opts.into();
        self
    }

    /// Callback to handle requests for local files. These are
    /// caused by using `LOAD DATA LOCAL INFILE` queries. The
    /// callback is passed the filename, and a `Write`able object
    /// to receive the contents of that file.
    /// If unset, the default callback will read files relative to
    /// the current directory.
    pub fn local_infile_handler(mut self, handler: Option<LocalInfileHandler>) -> Self {
        self.opts.0.local_infile_handler = handler;
        self
    }

    /// Tcp connect timeout (defaults to `None`). Available as `tcp_connect_timeout_ms`
    /// url parameter.
    ///
    /// Can be defined using `tcp_connect_timeout_ms` connection url parameter.
    pub fn tcp_connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.opts.0.tcp_connect_timeout = timeout;
        self
    }

    /// Bind address for a client (defaults to `None`).
    ///
    /// Use carefully. Will probably make pool unusable because of *address already in use*
    /// errors.
    pub fn bind_address<T>(mut self, bind_address: Option<T>) -> Self
    where
        T: Into<SocketAddr>,
    {
        self.opts.0.bind_address = bind_address.map(Into::into);
        self
    }

    /// Number of prepared statements cached on the client side (per connection).
    /// Defaults to [`DEFAULT_STMT_CACHE_SIZE`].
    ///
    /// Can be defined using `stmt_cache_size` connection url parameter.
    ///
    /// Call with `None` to reset to default.
    pub fn stmt_cache_size<T>(mut self, cache_size: T) -> Self
    where
        T: Into<Option<usize>>,
    {
        self.opts.0.stmt_cache_size = cache_size.into().unwrap_or(128);
        self
    }

    /// If not `None`, then client will ask for compression if server supports it
    /// (defaults to `None`).
    ///
    /// Can be defined using `compress` connection url parameter with values:
    /// * `true` - library defined default compression level;
    /// * `fast` - library defined fast compression level;
    /// * `best` - library defined best compression level;
    /// * `0`, `1`, ..., `9` - explicitly defined compression level where `0` stands for
    ///   "no compression";
    ///
    /// Note that compression level defined here will affect only outgoing packets.
    pub fn compress(mut self, compress: Option<crate::Compression>) -> Self {
        self.opts.0.compress = compress;
        self
    }

    /// Additional client capabilities to set (defaults to empty).
    ///
    /// This value will be OR'ed with other client capabilities during connection initialisation.
    ///
    /// ### Note
    ///
    /// It is a good way to set something like `CLIENT_FOUND_ROWS` but you should note that it
    /// won't let you to interfere with capabilities managed by other options (like
    /// `CLIENT_SSL` or `CLIENT_COMPRESS`). Also note that some capabilities are reserved,
    /// pointless or may broke the connection, so this option should be used with caution.
    pub fn additional_capabilities(mut self, additional_capabilities: CapabilityFlags) -> Self {
        let forbidden_flags: CapabilityFlags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SSL
            | CapabilityFlags::CLIENT_COMPRESS
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_LOCAL_FILES
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PS_MULTI_RESULTS;

        self.opts.0.additional_capabilities = additional_capabilities & !forbidden_flags;
        self
    }

    /// Connect attributes
    ///
    /// This value is sent to the server as custom name-value attributes.
    /// You can see them from performance_schema tables: [`session_account_connect_attrs`
    /// and `session_connect_attrs`][attr_tables] when all of the following conditions
    /// are met.
    ///
    /// * The server is MySQL 5.6 or later, or MariaDB 10.0 or later.
    /// * [`performance_schema`] is on.
    /// * [`performance_schema_session_connect_attrs_size`] is -1 or big enough
    ///   to store specified attributes.
    ///
    /// ### Note
    ///
    /// Attribute names that begin with an underscore (`_`) are not set by
    /// application programs because they are reserved for internal use.
    ///
    /// The following attributes are sent in addition to ones set by programs.
    ///
    /// name            | value
    /// ----------------|--------------------------
    /// _client_name    | The client library name (`rust-mysql-simple`)
    /// _client_version | The client library version
    /// _os             | The operation system (`target_os` cfg feature)
    /// _pid            | The client proces ID
    /// _platform       | The machine platform (`target_arch` cfg feature)
    /// program_name    | The first element of `std::env::args` if program_name isn't set by programs.
    ///
    /// [attr_tables]: https://dev.mysql.com/doc/refman/en/performance-schema-connection-attribute-tables.html
    /// [`performance_schema`]: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-system-variables.html#sysvar_performance_schema
    /// [`performance_schema_session_connect_attrs_size`]: https://dev.mysql.com/doc/refman/en/performance-schema-system-variables.html#sysvar_performance_schema_session_connect_attrs_size
    ///
    pub fn connect_attrs<T1: Into<String> + Eq + Hash, T2: Into<String>>(
        mut self,
        connect_attrs: HashMap<T1, T2>,
    ) -> Self {
        self.opts.0.connect_attrs = HashMap::with_capacity(connect_attrs.len());
        for (name, value) in connect_attrs {
            let name = name.into();
            if !name.starts_with('_') {
                self.opts.0.connect_attrs.insert(name, value.into());
            }
        }
        self
    }
}

impl From<OptsBuilder> for Opts {
    fn from(builder: OptsBuilder) -> Opts {
        builder.opts
    }
}

impl Default for OptsBuilder {
    fn default() -> OptsBuilder {
        OptsBuilder {
            opts: Opts::default(),
        }
    }
}

fn get_opts_user_from_url(url: &Url) -> Option<String> {
    let user = url.username();
    if user != "" {
        Some(
            percent_decode(user.as_ref())
                .decode_utf8_lossy()
                .into_owned(),
        )
    } else {
        None
    }
}

fn get_opts_pass_from_url(url: &Url) -> Option<String> {
    if let Some(pass) = url.password() {
        Some(
            percent_decode(pass.as_ref())
                .decode_utf8_lossy()
                .into_owned(),
        )
    } else {
        None
    }
}

fn get_opts_db_name_from_url(url: &Url) -> Option<String> {
    if let Some(mut segments) = url.path_segments() {
        segments
            .next()
            .filter(|&db_name| !db_name.is_empty())
            .map(|db_name| {
                percent_decode(db_name.as_ref())
                    .decode_utf8_lossy()
                    .into_owned()
            })
    } else {
        None
    }
}

fn from_url_basic(url_str: &str) -> Result<(Opts, Vec<(String, String)>), UrlError> {
    let url = Url::parse(url_str)?;
    if url.scheme() != "mysql" {
        return Err(UrlError::UnsupportedScheme(url.scheme().to_string()));
    }
    if url.cannot_be_a_base() {
        return Err(UrlError::BadUrl);
    }
    let user = get_opts_user_from_url(&url);
    let pass = get_opts_pass_from_url(&url);
    let ip_or_hostname = url
        .host()
        .ok_or(UrlError::BadUrl)
        .and_then(|host| url::Host::parse(&host.to_string()).map_err(|_| UrlError::BadUrl))?;
    let tcp_port = url.port().unwrap_or(3306);
    let db_name = get_opts_db_name_from_url(&url);

    let query_pairs = url.query_pairs().into_owned().collect();
    let opts = Opts(Box::new(InnerOpts {
        user,
        pass,
        ip_or_hostname,
        tcp_port,
        db_name,
        ..InnerOpts::default()
    }));

    Ok((opts, query_pairs))
}

fn from_url(url: &str) -> Result<Opts, UrlError> {
    let (mut opts, query_pairs) = from_url_basic(url)?;
    for (key, value) in query_pairs {
        if key == "prefer_socket" {
            if value == "true" {
                opts.set_prefer_socket(true);
            } else if value == "false" {
                opts.set_prefer_socket(false);
            } else {
                return Err(UrlError::InvalidValue("prefer_socket".into(), value));
            }
        } else if key == "tcp_keepalive_time_ms" {
            match u32::from_str(&*value) {
                Ok(tcp_keepalive_time_ms) => {
                    opts.0.tcp_keepalive_time = Some(tcp_keepalive_time_ms);
                }
                _ => {
                    return Err(UrlError::InvalidValue(
                        "tcp_keepalive_time_ms".into(),
                        value,
                    ));
                }
            }
        } else if key == "tcp_connect_timeout_ms" {
            match u64::from_str(&*value) {
                Ok(tcp_connect_timeout_ms) => {
                    opts.0.tcp_connect_timeout =
                        Some(Duration::from_millis(tcp_connect_timeout_ms));
                }
                _ => {
                    return Err(UrlError::InvalidValue(
                        "tcp_connect_timeout_ms".into(),
                        value,
                    ));
                }
            }
        } else if key == "stmt_cache_size" {
            match usize::from_str(&*value) {
                Ok(stmt_cache_size) => {
                    opts.0.stmt_cache_size = stmt_cache_size;
                }
                _ => {
                    return Err(UrlError::InvalidValue("stmt_cache_size".into(), value));
                }
            }
        } else if key == "compress" {
            if value == "true" {
                opts.0.compress = Some(crate::Compression::default());
            } else if value == "fast" {
                opts.0.compress = Some(crate::Compression::fast());
            } else if value == "best" {
                opts.0.compress = Some(crate::Compression::best());
            } else if value.len() == 1 && 0x30 <= value.as_bytes()[0] && value.as_bytes()[0] <= 0x39
            {
                opts.0.compress =
                    Some(crate::Compression::new((value.as_bytes()[0] - 0x30) as u32));
            } else {
                return Err(UrlError::InvalidValue("compress".into(), value));
            }
        } else if key == "socket" {
            let socket = percent_decode_str(&*value).decode_utf8_lossy().into_owned();
            if !socket.is_empty() {
                opts.0.socket = Some(socket);
            }
        } else {
            return Err(UrlError::UnknownParameter(key));
        }
    }
    Ok(opts)
}

impl<S: AsRef<str>> From<S> for Opts {
    fn from(url: S) -> Opts {
        match from_url(url.as_ref()) {
            Ok(opts) => opts,
            Err(err) => panic!("{}", err),
        }
    }
}

#[cfg(test)]
mod test {
    use mysql_common::proto::codec::Compression;

    use super::{InnerOpts, Opts};

    #[test]
    fn should_report_empty_url_database_as_none() {
        let opt = Opts::from("mysql://localhost/");
        assert_eq!(opt.get_db_name(), None);
    }

    #[test]
    fn should_convert_url_into_opts() {
        let opts = "mysql://us%20r:p%20w@localhost:3308/db%2dname?prefer_socket=false&tcp_keepalive_time_ms=5000&socket=%2Ftmp%2Fmysql.sock&compress=8";
        assert_eq!(
            Opts(Box::new(InnerOpts {
                user: Some("us r".to_string()),
                pass: Some("p w".to_string()),
                ip_or_hostname: url::Host::Domain("localhost".to_string()),
                tcp_port: 3308,
                db_name: Some("db-name".to_string()),
                prefer_socket: false,
                tcp_keepalive_time: Some(5000),
                socket: Some("/tmp/mysql.sock".into()),
                compress: Some(Compression::new(8)),
                ..InnerOpts::default()
            })),
            opts.into()
        );
    }

    #[test]
    #[should_panic]
    fn should_panic_on_invalid_url() {
        let opts = "42";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    fn should_panic_on_invalid_scheme() {
        let opts = "postgres://localhost";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    fn should_panic_on_unknown_query_param() {
        let opts = "mysql://localhost/foo?bar=baz";
        let _: Opts = opts.into();
    }
}
