#![cfg(any(feature = "native-tls", feature = "rustls"))]

mod native_tls_error;
mod rustls_error;

#[cfg(feature = "native-tls")]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        feature = "native-tls",
        feature = "rustls-tls",
        feature = "rustls-tls-ring"
    )))
)]
pub use native_tls_error::TlsError;

#[cfg(feature = "rustls")]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        feature = "native-tls",
        feature = "rustls-tls",
        feature = "rustls-tls-ring"
    )))
)]
pub use rustls_error::TlsError;
