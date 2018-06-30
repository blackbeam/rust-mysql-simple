use consts::CapabilityFlags;

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
#[cfg(all(feature = "ssl", not(target_os = "windows")))]
use std::path;
use std::str::FromStr;

use std::time::Duration;

use super::super::error::UrlError;
use super::LocalInfileHandler;

use url::percent_encoding::percent_decode;
use url::Url;

/// Ssl options.
///
/// Option<Option<(CERT, PASS, EXTRA)>> where
/// CERT - client certificate path (pkcs12)
/// PASS - pkcs12 password
/// EXTRA - vector of extra certificates in the chain
///
/// This parameters could be omitted using `Some(None)` value.
#[cfg(all(feature = "ssl", target_os = "macos"))]
pub type SslOpts = Option<Option<(path::PathBuf, String, Vec<path::PathBuf>)>>;

#[cfg(all(feature = "ssl", not(target_os = "macos"), unix))]
/// Ssl options: Option<(pem_ca_cert, Option<(pem_client_cert, pem_client_key)>)>.`
pub type SslOpts = Option<(path::PathBuf, Option<(path::PathBuf, path::PathBuf)>)>;

#[cfg(all(feature = "ssl", target_os = "windows"))]
/// Not implemented on Windows
pub type SslOpts = Option<()>;

#[cfg(not(feature = "ssl"))]
/// Requires `ssl` feature
pub type SslOpts = Option<()>;

/// Mysql connection options.
///
/// Build one with [`OptsBuilder`](struct.OptsBuilder.html).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Opts {
    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    ip_or_hostname: Option<String>,
    /// TCP port of mysql server (defaults to `3306`).
    tcp_port: u16,
    /// Path to unix socket on unix or pipe name on windows (defaults to `None`).
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
    prefer_socket: bool,

    /// Whether to enable `TCP_NODELAY` (defaults to `true`).
    ///
    /// This option disables Nagle's algorithm, which can cause unusually high latency (~40ms) at
    /// some cost to maximum throughput. See #132.
    tcp_nodelay: bool,

    /// TCP keep alive time for mysql connection.
    tcp_keepalive_time: Option<u32>,

    /// Commands to execute on each new database connection.
    init: Vec<String>,

    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`).
    /// Only make sense if ssl_opts is not None.
    verify_peer: bool,

    /// Only available if `ssl` feature enabled.
    ssl_opts: SslOpts,

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
    tcp_connect_timeout: Option<Duration>,

    /// Bind address for a client (defaults to `None`).
    ///
    /// Use carefully. Will probably make pool unusable because of *address already in use*
    /// errors.
    bind_address: Option<SocketAddr>,

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    stmt_cache_size: usize,

    /// If `true`, then client will ask for compression if server supports it (defaults to `false`).
    compress: bool,

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
}

impl Opts {
    #[doc(hidden)]
    pub fn addr_is_loopback(&self) -> bool {
        if self.ip_or_hostname.is_some() {
            let v4addr: Option<Ipv4Addr> =
                FromStr::from_str(self.ip_or_hostname.as_ref().unwrap().as_ref()).ok();
            let v6addr: Option<Ipv6Addr> =
                FromStr::from_str(self.ip_or_hostname.as_ref().unwrap().as_ref()).ok();
            if let Some(addr) = v4addr {
                addr.is_loopback()
            } else if let Some(addr) = v6addr {
                addr.is_loopback()
            } else if self.ip_or_hostname.as_ref().unwrap() == "localhost" {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn from_url(url: &str) -> Result<Opts, UrlError> {
        from_url(url)
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn get_ip_or_hostname(&self) -> Option<&str> {
        self.ip_or_hostname.as_ref().map(|x| &**x)
    }
    /// TCP port of mysql server (defaults to `3306`).
    pub fn get_tcp_port(&self) -> u16 {
        self.tcp_port
    }
    /// Socket path on unix or pipe name on windows (defaults to `None`).
    pub fn get_socket(&self) -> Option<&str> {
        self.socket.as_ref().map(|x| &**x)
    }
    /// User (defaults to `None`).
    pub fn get_user(&self) -> Option<&str> {
        self.user.as_ref().map(|x| &**x)
    }
    /// Password (defaults to `None`).
    pub fn get_pass(&self) -> Option<&str> {
        self.pass.as_ref().map(|x| &**x)
    }
    /// Database name (defaults to `None`).
    pub fn get_db_name(&self) -> Option<&str> {
        self.db_name.as_ref().map(|x| &**x)
    }

    /// The timeout for each attempt to write to the server.
    pub fn get_read_timeout(&self) -> Option<&Duration> {
        self.read_timeout.as_ref()
    }

    /// The timeout for each attempt to write to the server.
    pub fn get_write_timeout(&self) -> Option<&Duration> {
        self.write_timeout.as_ref()
    }

    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket (or named pipe on windows) after TCP connection
    /// to `127.0.0.1` if `true`.
    pub fn get_prefer_socket(&self) -> bool {
        self.prefer_socket
    }
    // XXX: Wait for keepalive_timeout stabilization
    /// Commands to execute on each new database connection.
    pub fn get_init(&self) -> Vec<String> {
        self.init.clone()
    }

    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`).
    /// Only make sense if ssl_opts is not None.
    pub fn get_verify_peer(&self) -> bool {
        self.verify_peer
    }

    /// #### Only available if `ssl` feature enabled.
    pub fn get_ssl_opts(&self) -> &SslOpts {
        &self.ssl_opts
    }

    fn set_prefer_socket(&mut self, val: bool) {
        self.prefer_socket = val;
    }

    fn set_verify_peer(&mut self, val: bool) {
        self.verify_peer = val;
    }

    /// Whether `TCP_NODELAY` will be set for mysql connection.
    pub fn get_tcp_nodelay(&self) -> bool {
        self.tcp_nodelay
    }

    /// TCP keep alive time for mysql connection.
    pub fn get_tcp_keepalive_time_ms(&self) -> Option<u32> {
        self.tcp_keepalive_time
    }

    /// Callback to handle requests for local files.
    pub fn get_local_infile_handler(&self) -> Option<&LocalInfileHandler> {
        self.local_infile_handler.as_ref()
    }

    /// Tcp connect timeout (defaults to `None`).
    pub fn get_tcp_connect_timeout(&self) -> Option<Duration> {
        self.tcp_connect_timeout
    }

    /// Bind address for a client (defaults to `None`).
    ///
    /// Use carefully. Will probably make pool unusable because of *address already in use*
    /// errors.
    pub fn bind_address(&self) -> Option<&SocketAddr> {
        self.bind_address.as_ref()
    }

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    pub fn get_stmt_cache_size(&self) -> usize {
        self.stmt_cache_size
    }

    /// If `true`, then client will ask for compression if server supports it (defaults to `false`).
    pub fn get_compress(&self) -> bool {
        self.compress
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
        self.additional_capabilities
    }
}

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: Some("127.0.0.1".to_string()),
            tcp_port: 3306,
            socket: None,
            user: None,
            pass: None,
            db_name: None,
            read_timeout: None,
            write_timeout: None,
            prefer_socket: true,
            init: vec![],
            verify_peer: false,
            ssl_opts: None,
            tcp_keepalive_time: None,
            tcp_nodelay: true,
            local_infile_handler: None,
            tcp_connect_timeout: None,
            bind_address: None,
            stmt_cache_size: 10,
            compress: false,
            additional_capabilities: CapabilityFlags::empty(),
        }
    }
}

/// Provides a way to build [`Opts`](struct.Opts.html).
///
/// ```ignore
/// // You can create new default builder
/// let mut builder = OptsBuilder::new();
/// builder.ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"))
///        .ssl_opts(Some(("/foo/cert.pem", None::<(String, String)>)));
///
/// // Or use existing T: Into<Opts>
/// let mut builder = OptsBuilder::from_opts(existing_opts);
/// builder.ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"));
/// ```
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
    pub fn ip_or_hostname<T: Into<String>>(&mut self, ip_or_hostname: Option<T>) -> &mut Self {
        self.opts.ip_or_hostname = ip_or_hostname.map(Into::into);
        self
    }

    /// TCP port of mysql server (defaults to `3306`).
    pub fn tcp_port(&mut self, tcp_port: u16) -> &mut Self {
        self.opts.tcp_port = tcp_port;
        self
    }

    /// Socket path on unix or pipe name on windows (defaults to `None`).
    pub fn socket<T: Into<String>>(&mut self, socket: Option<T>) -> &mut Self {
        self.opts.socket = socket.map(Into::into);
        self
    }

    /// User (defaults to `None`).
    pub fn user<T: Into<String>>(&mut self, user: Option<T>) -> &mut Self {
        self.opts.user = user.map(Into::into);
        self
    }

    /// Password (defaults to `None`).
    pub fn pass<T: Into<String>>(&mut self, pass: Option<T>) -> &mut Self {
        self.opts.pass = pass.map(Into::into);
        self
    }

    /// Database name (defaults to `None`).
    pub fn db_name<T: Into<String>>(&mut self, db_name: Option<T>) -> &mut Self {
        self.opts.db_name = db_name.map(Into::into);
        self
    }

    /// The timeout for each attempt to read from the server (defaults to `None`).
    ///
    /// Note that named pipe connection will ignore duration's `nanos`, and also note that
    /// it is an error to pass the zero `Duration` to this method.
    pub fn read_timeout(&mut self, read_timeout: Option<Duration>) -> &mut Self {
        self.opts.read_timeout = read_timeout;
        self
    }

    /// The timeout for each attempt to write to the server (defaults to `None`).
    ///
    /// Note that named pipe connection will ignore duration's `nanos`, and also note that
    /// it is likely error to pass the zero `Duration` to this method.
    pub fn write_timeout(&mut self, write_timeout: Option<Duration>) -> &mut Self {
        self.opts.write_timeout = write_timeout;
        self
    }

    /// TCP keep alive time for mysql connection (defaults to `None`). Available as
    /// `tcp_keepalive_time_ms` url parameter.
    pub fn tcp_keepalive_time_ms(&mut self, tcp_keepalive_time_ms: Option<u32>) -> &mut Self {
        self.opts.tcp_keepalive_time = tcp_keepalive_time_ms;
        self
    }

    /// Set the `TCP_NODELAY` option for the mysql connection (defaults to `true`).
    ///
    /// Setting this option to false re-enables Nagle's algorithm, which can cause unusually high
    /// latency (~40ms) but may increase maximum throughput. See #132.
    pub fn tcp_nodelay(&mut self, nodelay: bool) -> &mut Self {
        self.opts.tcp_nodelay = nodelay;
        self
    }

    /// Prefer socket connection (defaults to `true`). Available as `prefer_socket` url parameter
    /// with value `true` or `false`.
    ///
    /// Will reconnect via socket (on named pipe on windows) after TCP connection
    /// to `127.0.0.1` if `true`.
    pub fn prefer_socket(&mut self, prefer_socket: bool) -> &mut Self {
        self.opts.prefer_socket = prefer_socket;
        self
    }

    /// Commands to execute on each new database connection.
    pub fn init<T: Into<String>>(&mut self, init: Vec<T>) -> &mut Self {
        self.opts.init = init.into_iter().map(Into::into).collect();
        self
    }

    /// #### Only available if `ssl` feature enabled.
    /// Perform or not ssl peer verification (defaults to `false`). Available as `verify_peer` url
    /// parameter with value `true` or `false`.
    ///
    /// Only make sense if ssl_opts is not None.
    pub fn verify_peer(&mut self, verify_peer: bool) -> &mut Self {
        self.opts.verify_peer = verify_peer;
        self
    }

    #[cfg(all(feature = "ssl", not(target_os = "macos"), unix))]
    /// SSL certificates and keys in pem format.
    ///
    /// If not None, then ssl connection implied.
    /// `Option<(ca_cert, Option<(client_cert, client_key)>)>.`
    pub fn ssl_opts<A, B, C>(&mut self, ssl_opts: Option<(A, Option<(B, C)>)>) -> &mut Self
    where
        A: Into<path::PathBuf>,
        B: Into<path::PathBuf>,
        C: Into<path::PathBuf>,
    {
        self.opts.ssl_opts = ssl_opts.map(|(ca_cert, rest)| {
            (
                ca_cert.into(),
                rest.map(|(client_cert, client_key)| (client_cert.into(), client_key.into())),
            )
        });
        self
    }

    /// SSL certificates and keys. If not None, then ssl connection implied.
    ///
    /// See `SslOpts`.
    #[cfg(all(feature = "ssl", target_os = "macos"))]
    pub fn ssl_opts<A, B, C>(&mut self, ssl_opts: Option<Option<(A, C, Vec<B>)>>) -> &mut Self
    where
        A: Into<path::PathBuf>,
        B: Into<path::PathBuf>,
        C: Into<String>,
    {
        self.opts.ssl_opts = ssl_opts.map(|opts| {
            opts.map(|(pkcs12_path, pass, certs)| {
                (
                    pkcs12_path.into(),
                    pass.into(),
                    certs.into_iter().map(Into::into).collect(),
                )
            })
        });
        self
    }

    /// Not implemented on windows
    #[cfg(all(feature = "ssl", target_os = "windows"))]
    pub fn ssl_opts<A, B, C>(&mut self, _: Option<SslOpts>) -> &mut Self {
        panic!("OptsBuilder::ssl_opts is not implemented on Windows");
    }

    /// Requires `ssl` feature
    #[cfg(not(feature = "ssl"))]
    pub fn ssl_opts<A, B, C>(&mut self, _: Option<SslOpts>) -> &mut Self {
        panic!("OptsBuilder::ssl_opts requires `ssl` feature");
    }

    /// Callback to handle requests for local files. These are
    /// caused by using `LOAD DATA LOCAL INFILE` queries. The
    /// callback is passed the filename, and a `Write`able object
    /// to receive the contents of that file.
    /// If unset, the default callback will read files relative to
    /// the current directory.
    pub fn local_infile_handler(&mut self, handler: Option<LocalInfileHandler>) -> &mut Self {
        self.opts.local_infile_handler = handler;
        self
    }

    /// Tcp connect timeout (defaults to `None`). Available as `tcp_connect_timeout_ms`
    /// url parameter.
    pub fn tcp_connect_timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        self.opts.tcp_connect_timeout = timeout;
        self
    }

    /// Bind address for a client (defaults to `None`).
    ///
    /// Use carefully. Will probably make pool unusable because of *address already in use*
    /// errors.
    pub fn bind_address<T>(&mut self, bind_address: Option<T>) -> &mut Self
    where
        T: Into<SocketAddr>,
    {
        self.opts.bind_address = bind_address.map(Into::into);
        self
    }

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    ///
    /// Call with `None` to reset to default.
    pub fn stmt_cache_size<T>(&mut self, cache_size: T) -> &mut Self
    where
        T: Into<Option<usize>>,
    {
        self.opts.stmt_cache_size = cache_size.into().unwrap_or(10);
        self
    }

    /// If `true`, then client will ask for compression if server supports it (defaults to `false`).
    pub fn compress(&mut self, compress: bool) -> &mut Self {
        self.opts.compress = compress;
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
    pub fn additional_capabilities(
        &mut self,
        additional_capabilities: CapabilityFlags,
    ) -> &mut Self {
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

        self.opts.additional_capabilities = additional_capabilities & !forbidden_flags;
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
        segments.next().map(|db_name| {
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
    if url.cannot_be_a_base() || !url.has_host() {
        return Err(UrlError::BadUrl);
    }
    let user = get_opts_user_from_url(&url);
    let pass = get_opts_pass_from_url(&url);
    let ip_or_hostname = url.host_str().map(String::from);
    let tcp_port = url.port().unwrap_or(3306);
    let db_name = get_opts_db_name_from_url(&url);

    let query_pairs = url.query_pairs().into_owned().collect();
    let opts = Opts {
        user,
        pass,
        ip_or_hostname,
        tcp_port,
        db_name,
        ..Opts::default()
    };

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
        } else if key == "verify_peer" {
            if cfg!(not(feature = "ssl")) {
                return Err(UrlError::FeatureRequired(
                    "`ssl'".into(),
                    "verify_peer".into(),
                ));
            } else {
                if value == "true" {
                    opts.set_verify_peer(true);
                } else if value == "false" {
                    opts.set_verify_peer(false);
                } else {
                    return Err(UrlError::InvalidValue("verify_peer".into(), value));
                }
            }
        } else if key == "tcp_keepalive_time_ms" {
            match u32::from_str(&*value) {
                Ok(tcp_keepalive_time_ms) => {
                    opts.tcp_keepalive_time = Some(tcp_keepalive_time_ms);
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
                    opts.tcp_connect_timeout = Some(Duration::from_millis(tcp_connect_timeout_ms));
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
                    opts.stmt_cache_size = stmt_cache_size;
                }
                _ => {
                    return Err(UrlError::InvalidValue("stmt_cache_size".into(), value));
                }
            }
        } else if key == "compress" {
            if value == "true" {
                opts.compress = true;
            } else if value == "false" {
                opts.compress = false;
            } else {
                return Err(UrlError::InvalidValue("compress".into(), value));
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
    use super::Opts;

    #[test]
    #[cfg(feature = "ssl")]
    fn should_convert_url_into_opts() {
        let opts = "mysql://us%20r:p%20w@localhost:3308/db%2dname?prefer_socket=false&verify_peer=true&tcp_keepalive_time_ms=5000";
        assert_eq!(
            Opts {
                user: Some("us r".to_string()),
                pass: Some("p w".to_string()),
                ip_or_hostname: Some("localhost".to_string()),
                tcp_port: 3308,
                db_name: Some("db-name".to_string()),
                prefer_socket: false,
                verify_peer: true,
                tcp_keepalive_time: Some(5000),
                ..Opts::default()
            },
            opts.into()
        );
    }

    #[test]
    #[cfg(not(feature = "ssl"))]
    fn should_convert_url_into_opts() {
        let opts = "mysql://usr:pw@192.168.1.1:3309/dbname";
        assert_eq!(
            Opts {
                user: Some("usr".to_string()),
                pass: Some("pw".to_string()),
                ip_or_hostname: Some("192.168.1.1".to_string()),
                tcp_port: 3309,
                db_name: Some("dbname".to_string()),
                ..Opts::default()
            },
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

    #[test]
    #[should_panic]
    #[cfg(not(feature = "ssl"))]
    fn should_panic_if_verify_peer_query_param_requires_feature() {
        let opts = "mysql://usr:pw@localhost:3308/dbname?verify_peer=false";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    #[cfg(feature = "ssl")]
    fn should_panic_on_invalid_verify_peer_param_value() {
        let opts = "mysql://usr:pw@localhost:3308/dbname?verify_peer=invalid";
        let _: Opts = opts.into();
    }
}
