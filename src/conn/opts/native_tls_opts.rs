#![cfg(feature = "native-tls")]

use native_tls::Identity;

use std::{borrow::Cow, path::Path};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientIdentity {
    pkcs12_path: Cow<'static, Path>,
    password: Option<Cow<'static, str>>,
}

impl ClientIdentity {
    /// Creates new identity with the given path to the pkcs12 archive.
    pub fn new<T>(pkcs12_path: T) -> Self
    where
        T: Into<Cow<'static, Path>>,
    {
        Self {
            pkcs12_path: pkcs12_path.into(),
            password: None,
        }
    }

    /// Sets the archive password.
    pub fn with_password<T>(mut self, pass: T) -> Self
    where
        T: Into<Cow<'static, str>>,
    {
        self.password = Some(pass.into());
        self
    }

    /// Returns the pkcs12 archive path.
    pub fn pkcs12_path(&self) -> &Path {
        self.pkcs12_path.as_ref()
    }

    /// Returns the archive password.
    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(AsRef::as_ref)
    }

    pub(crate) fn load(&self) -> crate::Result<Identity> {
        let der = std::fs::read(self.pkcs12_path.as_ref())?;
        Ok(Identity::from_pkcs12(
            &*der,
            self.password.as_deref().unwrap_or(""),
        )?)
    }
}
