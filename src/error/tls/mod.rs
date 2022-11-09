#![cfg(any(feature = "native-tls", feature = "rustls"))]

pub mod native_tls_error;
pub mod rustls_error;

#[cfg(feature = "native-tls")]
pub use native_tls_error::TlsError;

#[cfg(feature = "rustls")]
pub use rustls_error::TlsError;
