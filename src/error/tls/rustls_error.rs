#![cfg(feature = "rustls")]

use std::fmt::Display;

use rustls::server::VerifierBuilderError;

#[derive(Debug)]
pub enum TlsError {
    VerifierBuilderError(VerifierBuilderError),
    Tls(rustls::Error),
    Pki(webpki::Error),
    InvalidDnsName(webpki::InvalidDnsNameError),
}

impl From<TlsError> for crate::Error {
    fn from(e: TlsError) -> Self {
        crate::Error::TlsError(e)
    }
}

impl From<VerifierBuilderError> for TlsError {
    fn from(e: VerifierBuilderError) -> Self {
        TlsError::VerifierBuilderError(e)
    }
}

impl From<rustls::Error> for TlsError {
    fn from(e: rustls::Error) -> Self {
        TlsError::Tls(e)
    }
}

impl From<webpki::InvalidDnsNameError> for TlsError {
    fn from(e: webpki::InvalidDnsNameError) -> Self {
        TlsError::InvalidDnsName(e)
    }
}

impl From<webpki::Error> for TlsError {
    fn from(e: webpki::Error) -> Self {
        TlsError::Pki(e)
    }
}

impl From<rustls::Error> for crate::Error {
    fn from(e: rustls::Error) -> Self {
        crate::Error::TlsError(e.into())
    }
}

impl From<webpki::Error> for crate::Error {
    fn from(e: webpki::Error) -> Self {
        crate::Error::TlsError(e.into())
    }
}

impl From<webpki::InvalidDnsNameError> for crate::Error {
    fn from(e: webpki::InvalidDnsNameError) -> Self {
        crate::Error::TlsError(e.into())
    }
}

impl std::error::Error for TlsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TlsError::VerifierBuilderError(e) => Some(e),
            TlsError::Tls(e) => Some(e),
            TlsError::Pki(e) => Some(e),
            TlsError::InvalidDnsName(e) => Some(e),
        }
    }
}

impl Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::VerifierBuilderError(e) => e.fmt(f),
            TlsError::Tls(e) => e.fmt(f),
            TlsError::Pki(e) => e.fmt(f),
            TlsError::InvalidDnsName(e) => e.fmt(f),
        }
    }
}
