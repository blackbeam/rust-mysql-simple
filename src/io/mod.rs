use std::io;
use std::io::Read as StdRead;
use std::io::Write as StdWrite;
use std::net;
use std::net::SocketAddr;
use std::fmt;
use std::time::Duration;

#[cfg(all(feature = "ssl", not(target_os = "windows")))]
use conn::SslOpts;

use super::value::Value;
use super::value::Value::{NULL, Int, UInt, Float, Bytes, Date, Time};
use super::consts;
use super::consts::Command;
use super::consts::ColumnType;
use super::error::Error::DriverError;
use super::error::DriverError::ConnectTimeout;
use super::error::DriverError::CouldNotConnect;
use super::error::DriverError::PacketTooLarge;
use super::error::DriverError::PacketOutOfSync;
use super::error::Result as MyResult;

#[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
use openssl::x509;
#[cfg(all(feature = "ssl", all(unix, not(target_os = "macos"))))]
use openssl::ssl::{self, SslStream, SslContext};
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::secure_transport::{
    ConnectionType,
    HandshakeError,
    ProtocolSide,
    SslContext,
    SslStream,
};
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::certificate::SecCertificate;
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::cipher_suite::CipherSuite;
#[cfg(all(feature = "ssl", target_os = "macos"))]
use security_framework::identity::SecIdentity;
use bufstream::BufStream;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::LittleEndian as LE;
#[cfg(unix)]
use std::os::unix as unix;
#[cfg(windows)]
use named_pipe as np;

mod tcp;

pub trait Read: ReadBytesExt + io::BufRead {
    fn read_lenenc_int(&mut self) -> io::Result<u64> {
        let head_byte = self.read_u8()?;
        let length = match head_byte {
            0xfc => 2,
            0xfd => 3,
            0xfe => 8,
            x => return Ok(x as u64),
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
            Err(io::Error::new(io::ErrorKind::Other,
                               "Unexpected EOF while reading length encoded string"))
        }
    }

    fn read_to_null(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut chars = self.bytes();
        while let Some(c) = chars.next() {
            let c = c?;
            if c == 0u8 {
                break;
            }
            out.push(c);
        }
        Ok(out)
    }

    fn read_bin_value(&mut self, col_type: consts::ColumnType, unsigned: bool) -> io::Result<Value> {
        match col_type {
            ColumnType::MYSQL_TYPE_STRING |
            ColumnType::MYSQL_TYPE_VAR_STRING |
            ColumnType::MYSQL_TYPE_BLOB |
            ColumnType::MYSQL_TYPE_TINY_BLOB |
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB |
            ColumnType::MYSQL_TYPE_LONG_BLOB |
            ColumnType::MYSQL_TYPE_SET |
            ColumnType::MYSQL_TYPE_ENUM |
            ColumnType::MYSQL_TYPE_DECIMAL |
            ColumnType::MYSQL_TYPE_VARCHAR |
            ColumnType::MYSQL_TYPE_BIT |
            ColumnType::MYSQL_TYPE_NEWDECIMAL |
            ColumnType::MYSQL_TYPE_GEOMETRY |
            ColumnType::MYSQL_TYPE_JSON => {
                Ok(Bytes(self.read_lenenc_bytes()?))
            },
            ColumnType::MYSQL_TYPE_TINY => {
                if unsigned {
                    Ok(Int(self.read_u8()? as i64))
                } else {
                    Ok(Int(self.read_i8()? as i64))
                }
            },
            ColumnType::MYSQL_TYPE_SHORT |
            ColumnType::MYSQL_TYPE_YEAR => {
                if unsigned {
                    Ok(Int(self.read_u16::<LE>()? as i64))
                } else {
                    Ok(Int(self.read_i16::<LE>()? as i64))
                }
            },
            ColumnType::MYSQL_TYPE_LONG |
            ColumnType::MYSQL_TYPE_INT24 => {
                if unsigned {
                    Ok(Int(self.read_u32::<LE>()? as i64))
                } else {
                    Ok(Int(self.read_i32::<LE>()? as i64))
                }
            },
            ColumnType::MYSQL_TYPE_LONGLONG => {
                if unsigned {
                    Ok(UInt(self.read_u64::<LE>()?))
                } else {
                    Ok(Int(self.read_i64::<LE>()?))
                }
            },
            ColumnType::MYSQL_TYPE_FLOAT => {
                Ok(Float(self.read_f32::<LE>()? as f64))
            },
            ColumnType::MYSQL_TYPE_DOUBLE => {
                Ok(Float(self.read_f64::<LE>()?))
            },
            ColumnType::MYSQL_TYPE_TIMESTAMP |
            ColumnType::MYSQL_TYPE_DATE |
            ColumnType::MYSQL_TYPE_DATETIME => {
                let len = self.read_u8()?;
                let mut year = 0u16;
                let mut month = 0u8;
                let mut day = 0u8;
                let mut hour = 0u8;
                let mut minute = 0u8;
                let mut second = 0u8;
                let mut micro_second = 0u32;
                if len >= 4u8 {
                    year = self.read_u16::<LE>()?;
                    month = self.read_u8()?;
                    day = self.read_u8()?;
                }
                if len >= 7u8 {
                    hour = self.read_u8()?;
                    minute = self.read_u8()?;
                    second = self.read_u8()?;
                }
                if len == 11u8 {
                    micro_second = self.read_u32::<LE>()?;
                }
                Ok(Date(year, month, day, hour, minute, second, micro_second))
            },
            ColumnType::MYSQL_TYPE_TIME => {
                let len = self.read_u8()?;
                let mut is_negative = false;
                let mut days = 0u32;
                let mut hours = 0u8;
                let mut minutes = 0u8;
                let mut seconds = 0u8;
                let mut micro_seconds = 0u32;
                if len >= 8u8 {
                    is_negative = self.read_u8()? == 1u8;
                    days = self.read_u32::<LE>()?;
                    hours = self.read_u8()?;
                    minutes = self.read_u8()?;
                    seconds = self.read_u8()?;
                }
                if len == 12u8 {
                    micro_seconds = self.read_u32::<LE>()?;
                }
                Ok(Time(is_negative, days, hours, minutes, seconds, micro_seconds))
            },
            _ => Ok(NULL),
        }
    }

    /// Drops mysql packet paylaod. Returns new seq_id.
    fn drop_packet(&mut self, mut seq_id: u8) -> MyResult<u8> {
        use std::io::ErrorKind::Other;
        loop {
            let payload_len = self.read_uint::<LE>(3)? as usize;
            let srv_seq_id = self.read_u8()?;
            if srv_seq_id != seq_id {
                return Err(DriverError(PacketOutOfSync));
            }
            seq_id = seq_id.wrapping_add(1);
            if payload_len == 0 {
                break;
            } else {
                if self.fill_buf()?.len() < payload_len {
                    return Err(io::Error::new(Other, "Unexpected EOF while reading packet").into())
                }
                self.consume(payload_len);
                if payload_len != consts::MAX_PAYLOAD_LEN {
                    break;
                }
            }
        }
        Ok(seq_id)
    }

    /// Reads mysql packet payload returns it with new seq_id value.
    fn read_packet(&mut self, mut seq_id: u8) -> MyResult<(Vec<u8>, u8)> {
        use std::io::ErrorKind::Other;
        let mut output = Vec::new();
        loop {
            let payload_len = self.read_uint::<LE>(3)? as usize;
            let srv_seq_id = self.read_u8()?;
            if srv_seq_id != seq_id {
                return Err(DriverError(PacketOutOfSync));
            }
            seq_id = seq_id.wrapping_add(1);
            if payload_len == 0 {
                break;
            } else {
                output.reserve(payload_len);
                let mut chunk = self.take(payload_len as u64);
                let count = chunk.read_to_end(&mut output)?;
                if count != payload_len {
                    return Err(io::Error::new(Other, "Unexpected EOF while reading packet").into())
                }
                if payload_len != consts::MAX_PAYLOAD_LEN {
                    break;
                }
            }
        }
        Ok((output, seq_id))
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
        StdWrite::write_all(self, &buf[..len])
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

    fn write_packet(&mut self, data: &[u8], mut seq_id: u8, max_allowed_packet: usize) -> MyResult<u8> {
        if data.len() > max_allowed_packet && max_allowed_packet < consts::MAX_PAYLOAD_LEN {
            return Err(DriverError(PacketTooLarge));
        }
        if data.len() == 0 {
            self.write_all(&[0, 0, 0, seq_id])?;
            seq_id = seq_id.wrapping_add(1);
        } else {
            let mut last_was_max = false;
            for chunk in data.chunks(consts::MAX_PAYLOAD_LEN) {
                let chunk_len = chunk.len();
                self.write_le_uint_n(chunk_len as u64, 3)?;
                self.write_u8(seq_id)?;
                self.write_all(chunk)?;
                last_was_max = chunk_len == consts::MAX_PAYLOAD_LEN;
                seq_id = seq_id.wrapping_add(1);
            }
            if last_was_max {
                self.write_all(&[0u8, 0u8, 0u8, seq_id])?;
                seq_id = seq_id.wrapping_add(1);
            }
        }
        self.flush()?;
        Ok(seq_id)
    }
}

impl<T: WriteBytesExt> Write for T {}

#[derive(Debug)]
pub enum Stream {
    #[cfg(unix)]
    SocketStream(BufStream<unix::net::UnixStream>),
    #[cfg(windows)]
    SocketStream(BufStream<np::PipeClient>),
    TcpStream(Option<TcpStream>),
}

trait IoPack: io::Read + io::Write + io::BufRead + 'static { }

impl<T: io::Read + io::Write + 'static> IoPack for BufStream<T> { }

impl AsMut<IoPack> for Stream {
    fn as_mut(&mut self) -> &mut IoPack {
        match *self {
            #[cfg(unix)]
            Stream::SocketStream(ref mut stream) => stream,
            #[cfg(windows)]
            Stream::SocketStream(ref mut stream) => stream,
            Stream::TcpStream(Some(ref mut stream)) => stream.as_mut(),
            _ => panic!("Incomplete stream"),
        }
    }
}

impl Stream {
    #[cfg(unix)]
    pub fn connect_socket(socket: &str,
                          read_timeout: Option<Duration>,
                          write_timeout: Option<Duration>) -> MyResult<Stream>
    {
        match unix::net::UnixStream::connect(socket) {
            Ok(stream) => {
                stream.set_read_timeout(read_timeout)?;
                stream.set_write_timeout(write_timeout)?;
                Ok(Stream::SocketStream(BufStream::new(stream)))
            },
            Err(e) => {
                let addr = format!("{}", socket);
                let desc = format!("{}", e);
                Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
            }
        }
    }

    #[cfg(windows)]
    pub fn connect_socket(socket: &str,
                          read_timeout: Option<Duration>,
                          write_timeout: Option<Duration>) -> MyResult<Stream>
    {
        let full_name = format!(r"\\.\pipe\{}", socket);
        match np::PipeClient::connect(full_name.clone()) {
            Ok(mut stream) => {
                stream.set_read_timeout(read_timeout);
                stream.set_write_timeout(write_timeout);
                Ok(Stream::SocketStream(BufStream::new(stream)))
            },
            Err(e) => {
                let desc = format!("{}", e);
                Err(DriverError(CouldNotConnect(Some((full_name, desc, e.kind())))))
            }
        }
    }

    #[cfg(all(not(unix), not(windows)))]
    fn connect_socket(&mut self) -> MyResult<()> {
        unimplemented!("Sockets is not implemented on current platform");
    }

    pub fn connect_tcp(ip_or_hostname: &str,
                       port: u16,
                       read_timeout: Option<Duration>,
                       write_timeout: Option<Duration>,
                       tcp_keepalive_time: Option<u32>,
                       tcp_connect_timeout: Option<Duration>,
                       bind_address: Option<SocketAddr>) -> MyResult<Stream>
    {
        let mut builder = tcp::MyTcpBuilder::new((ip_or_hostname, port));
        builder
            .connect_timeout(tcp_connect_timeout)
            .read_timeout(read_timeout)
            .write_timeout(write_timeout)
            .keepalive_time_ms(tcp_keepalive_time)
            .bind_address(bind_address);
        builder.connect()
            .map(|stream| Stream::TcpStream(Some(TcpStream::Insecure(BufStream::new(stream)))))
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
            &Stream::TcpStream(Some(TcpStream::Insecure(_))) => true,
            _ => false,
        }
    }
}

#[cfg(all(feature = "ssl", target_os = "macos"))]
impl Stream {
    pub fn make_secure(mut self, verify_peer: bool, ip_or_hostname: &Option<String>, ssl_opts: &SslOpts)
    -> MyResult<Stream>
    {
        use std::path::Path;
        use std::path::PathBuf;

        fn load_client_cert(path: &Path, pass: &str) -> MyResult<Option<SecIdentity>> {
            use security_framework::import_export::Pkcs12ImportOptions;
            let mut import = Pkcs12ImportOptions::new();
            import.passphrase(pass);
            let mut client_file = ::std::fs::File::open(path)?;
            let mut client_data = Vec::new();
            client_file.read_to_end(&mut client_data)?;
            let mut identities = import.import(&*client_data)?;
            Ok(identities.pop().map(|x| x.identity))
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
            let mut ctx: SslContext = SslContext::new(ProtocolSide::Client, ConnectionType::Stream)?;
            match *ssl_opts {
                Some(ref ssl_opts) => {
                    if verify_peer {
                        ctx.set_peer_domain_name(ip_or_hostname.as_ref().unwrap_or(&("localhost".into())))?;
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
                                    let s_stream = match ctx.handshake(stream.into_inner().unwrap()) {
                                        Ok(s_stream) => s_stream,
                                        Err(HandshakeError::Failure(err)) => {
                                            return Err(err.into())
                                        },
                                        Err(HandshakeError::Interrupted(_)) => unreachable!(),
                                    };
                                    Ok(Stream::TcpStream(Some(TcpStream::Secure(BufStream::new(s_stream)))))
                                },
                                _ => unreachable!(),
                            }

                        },
                        _ => unreachable!(),
                    }
                },
                _ => unreachable!(),
            }
        } else {
            Ok(self)
        }
    }
}

#[cfg(all(feature = "ssl", not(target_os = "macos"), unix))]
impl Stream {
    pub fn make_secure(mut self, verify_peer: bool, _: &Option<String>, ssl_opts: &SslOpts) -> MyResult<Stream>
    {
        if self.is_insecure() {
            let mut ctx = SslContext::builder(ssl::SslMethod::tls())?;
            let mode = if verify_peer {
                ssl::SSL_VERIFY_PEER
            } else {
                ssl::SSL_VERIFY_NONE
            };
            ctx.set_verify(mode);
            match *ssl_opts {
                Some((ref ca_cert, None)) => ctx.set_ca_file(&ca_cert)?,
                Some((ref ca_cert, Some((ref client_cert, ref client_key)))) => {
                    ctx.set_ca_file(&ca_cert)?;
                    ctx.set_certificate_file(&client_cert, x509::X509_FILETYPE_PEM)?;
                    ctx.set_private_key_file(&client_key, x509::X509_FILETYPE_PEM)?;
                },
                _ => unreachable!(),
            }
            match self {
                Stream::TcpStream(ref mut opt_stream) if opt_stream.is_some() => {
                    let stream = opt_stream.take().unwrap();
                    match stream {
                        TcpStream::Insecure(stream) => {
                            let ctx = ctx.build();
                            let s_stream = match ssl::Ssl::new(&ctx)?.connect(stream.into_inner().unwrap()) {
                                Ok(s_stream) => s_stream,
                                Err(handshake_err) => match handshake_err {
                                    ssl::HandshakeError::SetupFailure(err) => return Err(err.into()),
                                    ssl::HandshakeError::Failure(mid_stream) => return Err(mid_stream.into_error().into()),
                                    ssl::HandshakeError::Interrupted(_) => unreachable!("Interrupted"),
                                },
                            };
                            Ok(Stream::TcpStream(Some(TcpStream::Secure(BufStream::new(s_stream)))))
                        },
                        _ => unreachable!(),
                    }

                },
                _ => unreachable!(),
            }
        } else {
            Ok(self)
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if let &mut Stream::TcpStream(None) = self {
            return;
        }
        let _ = self.as_mut().write_packet(&[Command::COM_QUIT as u8], 0, consts::MAX_PAYLOAD_LEN);
        let _ = self.as_mut().flush();
    }
}

pub enum TcpStream {
    #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
    Secure(BufStream<SslStream<net::TcpStream>>),
    Insecure(BufStream<net::TcpStream>),
}

impl AsMut<IoPack> for TcpStream {
    fn as_mut(&mut self) -> &mut IoPack {
        match *self {
            #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
            TcpStream::Secure(ref mut stream) => stream,
            TcpStream::Insecure(ref mut stream) => stream,
        }
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            #[cfg(all(feature = "ssl", any(unix, target_os = "macos")))]
            TcpStream::Secure(_) => write!(f, "Secure stream"),
            TcpStream::Insecure(ref s) => write!(f, "Insecure stream {:?}", s),
        }
    }
}
