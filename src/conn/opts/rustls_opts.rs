#![cfg(feature = "rustls-tls")]

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer};
use rustls_pemfile::{certs, ec_private_keys, pkcs8_private_keys, rsa_private_keys};

use std::{borrow::Cow, path::Path};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientIdentity {
    cert_chain_path: Cow<'static, Path>,
    priv_key_path: Cow<'static, Path>,
}

impl ClientIdentity {
    /// Creates new identity.
    ///
    /// `cert_chain_path` - path to a certificate chain (in PEM or DER)
    /// `priv_key_path` - path to a private key (in DER or PEM) (it'll take the first one)
    pub fn new<T, U>(cert_chain_path: T, priv_key_path: U) -> Self
    where
        T: Into<Cow<'static, Path>>,
        U: Into<Cow<'static, Path>>,
    {
        Self {
            cert_chain_path: cert_chain_path.into(),
            priv_key_path: priv_key_path.into(),
        }
    }

    /// Sets the certificate chain path (in DER or PEM).
    pub fn with_cert_chain_path<T>(mut self, cert_chain_path: T) -> Self
    where
        T: Into<Cow<'static, Path>>,
    {
        self.cert_chain_path = cert_chain_path.into();
        self
    }

    /// Sets the private key path (in DER or PEM) (it'll take the first one).
    pub fn with_priv_key_path<T>(mut self, priv_key_path: T) -> Self
    where
        T: Into<Cow<'static, Path>>,
    {
        self.priv_key_path = priv_key_path.into();
        self
    }

    /// Returns the certificate chain path.
    pub fn cert_chain_path(&self) -> &Path {
        self.cert_chain_path.as_ref()
    }

    /// Returns the private key path.
    pub fn priv_key_path(&self) -> &Path {
        self.priv_key_path.as_ref()
    }

    pub(crate) fn load(
        &self,
    ) -> crate::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let cert_data = std::fs::read(self.cert_chain_path.as_ref())?;
        let key_data = std::fs::read(self.priv_key_path.as_ref())?;

        let mut cert_chain = Vec::new();
        for cert in certs(&mut &*cert_data) {
            cert_chain.push(cert?.to_owned());
        }
        if cert_chain.is_empty() && !cert_data.is_empty() {
            cert_chain.push(CertificateDer::from(cert_data));
        }

        let mut priv_key = None;

        for key in rsa_private_keys(&mut &*key_data).take(1) {
            priv_key = Some(PrivateKeyDer::Pkcs1(key?.clone_key()));
        }

        if priv_key.is_none() {
            for key in pkcs8_private_keys(&mut &*key_data).take(1) {
                priv_key = Some(PrivateKeyDer::Pkcs8(key?.clone_key()))
            }
        }

        if priv_key.is_none() {
            for key in ec_private_keys(&mut &*key_data).take(1) {
                priv_key = Some(PrivateKeyDer::Sec1(key?.clone_key()))
            }
        }

        if let Some(priv_key) = priv_key {
            return Ok((cert_chain, priv_key));
        }

        match PrivateKeyDer::try_from(key_data.as_slice()) {
            Ok(key) => Ok((cert_chain, key.clone_key())),
            Err(_) => Ok((
                cert_chain,
                PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(key_data)),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rustls::pki_types::PrivateKeyDer;

    use crate::ClientIdentity;

    #[test]
    fn load_pkcs1() {
        let (_certs, key_pem) = ClientIdentity::new(
            Path::new("tests/client.crt"),
            Path::new("tests/client-key.pem"),
        )
        .load()
        .unwrap();
        assert!(matches!(key_pem, PrivateKeyDer::Pkcs1(_)));

        let (_certs, key_der) = ClientIdentity::new(
            Path::new("tests/client.crt"),
            Path::new("tests/client-key.pem"),
        )
        .load()
        .unwrap();
        assert!(matches!(key_der, PrivateKeyDer::Pkcs1(_)));

        assert_eq!(key_der, key_pem);
    }

    #[test]
    fn load_pkcs8() {
        let (_certs, key_der) = ClientIdentity::new(
            Path::new("tests/client.crt"),
            Path::new("tests/client-key.pkcs8.der"),
        )
        .load()
        .unwrap();
        assert!(matches!(key_der, PrivateKeyDer::Pkcs8(_)));

        let (_certs, key_pem) = ClientIdentity::new(
            Path::new("tests/client.crt"),
            Path::new("tests/client-key.pkcs8.pem"),
        )
        .load()
        .unwrap();
        assert!(matches!(key_pem, PrivateKeyDer::Pkcs8(_)));

        assert_eq!(key_der, key_pem);
    }
}
