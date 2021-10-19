#![cfg(feature = "native-tls")]

use std::fmt::Display;

#[derive(Debug)]
pub enum TlsError {
    TlsError(native_tls::Error),
    TlsHandshakeError(native_tls::HandshakeError<std::net::TcpStream>),
}

impl From<TlsError> for super::super::Error {
    fn from(err: TlsError) -> super::super::Error {
        super::super::Error::TlsError(err)
    }
}

impl From<native_tls::Error> for super::super::Error {
    fn from(err: native_tls::Error) -> super::super::Error {
        super::super::Error::TlsError(TlsError::TlsError(err))
    }
}

impl From<native_tls::HandshakeError<std::net::TcpStream>> for super::super::Error {
    fn from(err: native_tls::HandshakeError<std::net::TcpStream>) -> super::super::Error {
        super::super::Error::TlsError(TlsError::TlsHandshakeError(err))
    }
}

impl std::error::Error for TlsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TlsError::TlsError(e) => Some(e),
            TlsError::TlsHandshakeError(e) => Some(e),
        }
    }
}

impl Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::TlsError(e) => e.fmt(f),
            TlsError::TlsHandshakeError(e) => e.fmt(f),
        }
    }
}
