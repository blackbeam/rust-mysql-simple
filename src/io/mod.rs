use bufstream::BufStream;
use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use io_enum::*;
#[cfg(windows)]
use named_pipe as np;
#[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
use openssl::ssl::{self, SslContext, SslStream};
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::certificate::SecCertificate;
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::cipher_suite::CipherSuite;
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::identity::SecIdentity;
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::secure_transport::{
    HandshakeError, SslConnectionType, SslContext, SslProtocolSide, SslStream,
};

use std::fmt;
use std::io::{self, Read as _, Write as _};
use std::net::{self, SocketAddr};
#[cfg(unix)]
use std::os::unix;
use std::time::Duration;

#[cfg(all(feature = "ssl", not(target_os = "windows")))]
use crate::conn::SslOpts;
use crate::consts::Command;
use crate::error::DriverError::{ConnectTimeout, CouldNotConnect};
use crate::error::Error::DriverError;
use crate::error::Result as MyResult;

mod tcp;

pub trait Read: ReadBytesExt + io::BufRead {
    fn read_lenenc_int(&mut self) -> io::Result<u64> {
        let head_byte = self.read_u8()?;
        let length = match head_byte {
            0xfc => 2,
            0xfd => 3,
            0xfe => 8,
            x => return Ok(u64::from(x)),
        };
        let out = self.read_uint::<LE>(length)?;
        Ok(out)
    }

    fn read_lenenc_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_lenenc_int()?;
        let mut out = Vec::with_capacity(len as usize);
        let count = if len > 0 {
            self.take(len).read_to_end(&mut out)?
        } else {
            0
        };
        if count as u64 == len {
            Ok(out)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Unexpected EOF while reading length encoded string",
            ))
        }
    }

    fn read_to_null(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        for c in self.bytes() {
            let c = c?;
            if c == 0 {
                break;
            }
            out.push(c);
        }
        Ok(out)
    }
}

impl<T: ReadBytesExt + io::BufRead> Read for T {}

pub trait Write: WriteBytesExt {
    fn write_le_uint_n(&mut self, x: u64, len: usize) -> io::Result<()> {
        let mut buf = [0u8; 8];
        let mut offset = 0;
        while offset < len {
            buf[offset] = (((0xFF << (offset * 8)) & x) >> (offset * 8)) as u8;
            offset += 1;
        }
        self.write_all(&buf[..len])
    }

    fn write_lenenc_int(&mut self, x: u64) -> io::Result<()> {
        if x < 251 {
            self.write_u8(x as u8)?;
            Ok(())
        } else if x < 65_536 {
            self.write_u8(0xFC)?;
            self.write_le_uint_n(x, 2)
        } else if x < 16_777_216 {
            self.write_u8(0xFD)?;
            self.write_le_uint_n(x, 3)
        } else {
            self.write_u8(0xFE)?;
            self.write_le_uint_n(x, 8)
        }
    }

    fn write_lenenc_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_lenenc_int(bytes.len() as u64)?;
        self.write_all(bytes)
    }
}

impl<T: WriteBytesExt> Write for T {}

#[derive(Debug, Read, Write)]
pub enum Stream {
    #[cfg(unix)]
    SocketStream(BufStream<unix::net::UnixStream>),
    #[cfg(windows)]
    SocketStream(BufStream<np::PipeClient>),
    TcpStream(TcpStream),
}

impl Stream {
    #[cfg(unix)]
    pub fn connect_socket(
        socket: &str,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> MyResult<Stream> {
        match unix::net::UnixStream::connect(socket) {
            Ok(stream) => {
                stream.set_read_timeout(read_timeout)?;
                stream.set_write_timeout(write_timeout)?;
                Ok(Stream::SocketStream(BufStream::new(stream)))
            }
            Err(e) => {
                let addr = socket.to_string();
                let desc = e.to_string();
                Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
            }
        }
    }

    #[cfg(windows)]
    pub fn connect_socket(
        socket: &str,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> MyResult<Stream> {
        let full_name = format!(r"\\.\pipe\{}", socket);
        match np::PipeClient::connect(full_name.clone()) {
            Ok(mut stream) => {
                stream.set_read_timeout(read_timeout);
                stream.set_write_timeout(write_timeout);
                Ok(Stream::SocketStream(BufStream::new(stream)))
            }
            Err(e) => {
                let desc = format!("{}", e);
                Err(DriverError(CouldNotConnect(Some((
                    full_name,
                    desc,
                    e.kind(),
                )))))
            }
        }
    }

    #[cfg(all(not(unix), not(windows)))]
    fn connect_socket(&mut self) -> MyResult<()> {
        unimplemented!("Sockets is not implemented on current platform");
    }

    pub fn connect_tcp(
        ip_or_hostname: &str,
        port: u16,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
        tcp_keepalive_time: Option<u32>,
        nodelay: bool,
        tcp_connect_timeout: Option<Duration>,
        bind_address: Option<SocketAddr>,
    ) -> MyResult<Stream> {
        let mut builder = tcp::MyTcpBuilder::new((ip_or_hostname, port));
        builder
            .connect_timeout(tcp_connect_timeout)
            .read_timeout(read_timeout)
            .write_timeout(write_timeout)
            .keepalive_time_ms(tcp_keepalive_time)
            .nodelay(nodelay)
            .bind_address(bind_address);
        builder
            .connect()
            .map(|stream| Stream::TcpStream(TcpStream::Insecure(BufStream::new(stream))))
            .map_err(|err| {
                if err.kind() == io::ErrorKind::TimedOut {
                    DriverError(ConnectTimeout)
                } else {
                    let addr = format!("{}:{}", ip_or_hostname, port);
                    let desc = format!("{}", err);
                    DriverError(CouldNotConnect(Some((addr, desc, err.kind()))))
                }
            })
    }

    pub fn is_insecure(&self) -> bool {
        match self {
            Stream::TcpStream(TcpStream::Insecure(_)) => true,
            _ => false,
        }
    }

    pub fn is_socket(&self) -> bool {
        match self {
            Stream::SocketStream(_) => true,
            _ => false,
        }
    }
}

#[cfg(all(feature = "ssl", target_os = "macos"))]
impl Stream {
    pub fn make_secure(
        mut self,
        verify_peer: bool,
        ip_or_hostname: Option<&str>,
        ssl_opts: &SslOpts,
    ) -> MyResult<Stream> {
        use std::path::{Path, PathBuf};

        fn load_client_cert(path: &Path, pass: &str) -> MyResult<Option<SecIdentity>> {
            use security_framework::import_export::Pkcs12ImportOptions;
            let mut import = Pkcs12ImportOptions::new();
            import.passphrase(pass);
            let mut client_file = ::std::fs::File::open(path)?;
            let mut client_data = Vec::new();
            client_file.read_to_end(&mut client_data)?;
            let mut identities = import.import(&*client_data)?;
            Ok(identities.pop().and_then(|x| x.identity))
        }

        fn load_extra_certs(files: &[PathBuf]) -> MyResult<Vec<SecCertificate>> {
            let mut extra_certs = Vec::new();
            for path in files {
                let mut cert_file = ::std::fs::File::open(path)?;
                let mut cert_data = Vec::new();
                cert_file.read_to_end(&mut cert_data)?;
                extra_certs.push(SecCertificate::from_der(&*cert_data)?);
            }
            Ok(extra_certs)
        }

        if self.is_insecure() {
            let mut ctx: SslContext =
                SslContext::new(SslProtocolSide::CLIENT, SslConnectionType::STREAM)?;
            match *ssl_opts {
                Some(ref ssl_opts) => {
                    if verify_peer {
                        ctx.set_peer_domain_name(
                            ip_or_hostname.as_ref().unwrap_or(&("localhost".into())),
                        )?;
                    }
                    // Taken from gmail.com
                    ctx.set_enabled_ciphers(&[
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
                        CipherSuite::TLS_ECDHE_RSA_WITH_RC4_128_SHA,
                        CipherSuite::TLS_RSA_WITH_AES_128_GCM_SHA256,
                        CipherSuite::TLS_RSA_WITH_AES_128_CBC_SHA256,
                        CipherSuite::TLS_RSA_WITH_AES_128_CBC_SHA,
                        CipherSuite::TLS_RSA_WITH_RC4_128_SHA,
                        CipherSuite::TLS_RSA_WITH_RC4_128_MD5,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
                        CipherSuite::TLS_RSA_WITH_AES_256_GCM_SHA384,
                        CipherSuite::TLS_RSA_WITH_AES_256_CBC_SHA256,
                        CipherSuite::TLS_RSA_WITH_AES_256_CBC_SHA,
                        CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
                        CipherSuite::TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
                        CipherSuite::TLS_RSA_WITH_3DES_EDE_CBC_SHA,
                    ])?;

                    if let Some((ref path, ref pass, ref certs)) = *ssl_opts {
                        if let Some(identity) = load_client_cert(path, pass)? {
                            let extra_certs = load_extra_certs(certs)?;
                            ctx.set_certificate(&identity, &*extra_certs)?;
                        }
                    }

                    match self {
                        Stream::TcpStream(ref mut opt_stream) if opt_stream.is_some() => {
                            let stream = opt_stream.take().unwrap();
                            match stream {
                                TcpStream::Insecure(mut stream) => {
                                    stream.flush()?;
                                    let s_stream = match ctx.handshake(stream.into_inner().unwrap())
                                    {
                                        Ok(s_stream) => s_stream,
                                        Err(HandshakeError::Failure(err)) => return Err(err.into()),
                                        Err(HandshakeError::Interrupted(_)) => unreachable!(),
                                    };
                                    Ok(Stream::TcpStream(Some(TcpStream::Secure(BufStream::new(
                                        s_stream,
                                    )))))
                                }
                                _ => unreachable!(),
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        } else {
            Ok(self)
        }
    }
}

#[cfg(all(feature = "ssl", not(target_os = "macos"), unix))]
impl Stream {
    pub fn make_secure(
        mut self,
        verify_peer: bool,
        _: Option<&str>,
        ssl_opts: &SslOpts,
    ) -> MyResult<Stream> {
        if self.is_insecure() {
            let mut ctx = SslContext::builder(ssl::SslMethod::tls())?;
            let mode = if verify_peer {
                ssl::SslVerifyMode::PEER
            } else {
                ssl::SslVerifyMode::NONE
            };
            ctx.set_verify(mode);
            match *ssl_opts {
                Some((ref ca_cert, None)) => ctx.set_ca_file(&ca_cert)?,
                Some((ref ca_cert, Some((ref client_cert, ref client_key)))) => {
                    ctx.set_ca_file(&ca_cert)?;
                    ctx.set_certificate_file(&client_cert, ssl::SslFiletype::PEM)?;
                    ctx.set_private_key_file(&client_key, ssl::SslFiletype::PEM)?;
                }
                _ => unreachable!(),
            }
            match self {
                Stream::TcpStream(ref mut opt_stream) if opt_stream.is_some() => {
                    let stream = opt_stream.take().unwrap();
                    match stream {
                        TcpStream::Insecure(stream) => {
                            let ctx = ctx.build();
                            let s_stream = match ssl::Ssl::new(&ctx)?
                                .connect(stream.into_inner().unwrap())
                            {
                                Ok(s_stream) => s_stream,
                                Err(handshake_err) => match handshake_err {
                                    ssl::HandshakeError::SetupFailure(err) => {
                                        return Err(err.into());
                                    }
                                    ssl::HandshakeError::Failure(mid_stream) => {
                                        return Err(mid_stream.into_error().into());
                                    }
                                    ssl::HandshakeError::WouldBlock(_mid_stream) => unreachable!(),
                                },
                            };
                            Ok(Stream::TcpStream(Some(TcpStream::Secure(BufStream::new(
                                s_stream,
                            )))))
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        } else {
            Ok(self)
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let _ = self.write(&[1, 0, 0, 0, Command::COM_QUIT as u8]);
        let _ = self.flush();
    }
}

#[derive(Read, Write)]
pub enum TcpStream {
    #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
    Secure(BufStream<SslStream<net::TcpStream>>),
    Insecure(BufStream<net::TcpStream>),
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
            TcpStream::Secure(_) => write!(f, "Secure stream"),
            TcpStream::Insecure(ref s) => write!(f, "Insecure stream {:?}", s),
        }
    }
}
