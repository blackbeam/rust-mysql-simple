//! Regression tests for the deprecated `sha256_password` plugin.
//!
//! The plugin has no cache, so every connection performs the full exchange.

use std::{fs, path::PathBuf, time::Duration};

use crate::{
    prelude::Queryable,
    test_misc::{get_opts, test_ssl},
    Conn, Error, OptsBuilder,
};

const PASSWORD: &str = "sha256-password-test";

/// Bounds connection failures while retaining the standard live-test configuration.
fn live_opts() -> OptsBuilder {
    get_opts()
        .tcp_connect_timeout(Some(Duration::from_secs(5)))
        .read_timeout(Some(Duration::from_secs(10)))
        .write_timeout(Some(Duration::from_secs(10)))
}

/// Owns the test accounts and the temporary public-key file, including on panic.
struct Sha256PasswordFixture {
    admin: Conn,
    user: String,
    directory: PathBuf,
}

impl Sha256PasswordFixture {
    fn empty_password_user(&self) -> String {
        format!("{}_e", self.user)
    }

    /// Forces TCP without inherited initialization commands for the test account.
    fn client_opts(&self) -> OptsBuilder {
        live_opts()
            .user(Some(self.user.clone()))
            .pass(Some(PASSWORD))
            .db_name(None::<String>)
            .init(Vec::<String>::new())
            .socket(None::<String>)
            .prefer_socket(false)
    }

    fn key_path(&self) -> PathBuf {
        self.directory.join("server-public-key.pem")
    }
}

impl Drop for Sha256PasswordFixture {
    /// Cleans up the accounts and only the exact temporary paths created by this test.
    fn drop(&mut self) {
        for user in [self.user.clone(), self.empty_password_user()] {
            let _ = self
                .admin
                .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"));
        }
        let _ = fs::remove_file(self.key_path());
        let _ = fs::remove_dir(&self.directory);
    }
}

#[test]
fn sha256_password_authentication() -> Result<(), Box<dyn std::error::Error>> {
    let mut admin = Conn::new(live_opts().ssl_opts(None))?;
    // OAEP padding is what the driver speaks, and the plugin is gone in MySQL 9.
    if admin.0.mariadb_server_version.is_some() || admin.server_version() < (8, 0, 5) {
        return Ok(());
    }
    let active: Option<String> = admin.query_first(
        "SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS \
         WHERE PLUGIN_NAME = 'sha256_password' AND PLUGIN_STATUS = 'ACTIVE'",
    )?;
    if active.is_none() {
        return Ok(());
    }
    let (_, public_key): (String, String) = admin
        .query_first("SHOW STATUS LIKE 'Rsa_public_key'")?
        .ok_or("MySQL did not expose its sha256_password RSA key")?;
    assert!(
        !public_key.is_empty(),
        "MySQL must have an authentication RSA key"
    );

    let id = rand::random::<u64>();
    let directory =
        std::env::temp_dir().join(format!("mysql-sha256-password-{}-{id}", std::process::id()));
    fs::create_dir(&directory)?;
    let mut fixture = Sha256PasswordFixture {
        admin,
        user: format!("__sha256_{id:016x}"),
        directory,
    };
    fixture.admin.query_drop(format!(
        "CREATE USER '{}'@'%' IDENTIFIED WITH sha256_password BY '{PASSWORD}'",
        fixture.user
    ))?;
    fixture.admin.query_drop(format!(
        "CREATE USER '{}'@'%' IDENTIFIED WITH sha256_password BY ''",
        fixture.empty_password_user()
    ))?;

    // Plaintext TCP: the password is encrypted with the key the server hands over.
    let plain = fixture.client_opts().ssl_opts(None);
    let mut conn = Conn::new(plain.clone())?;
    assert!(conn.is_insecure());
    assert!(conn.ping().is_ok());
    drop(conn);

    // Plaintext TCP with a configured key: nothing is requested from the server.
    let path = fixture.key_path();
    fs::write(&path, &public_key)?;
    let mut conn = Conn::new(plain.clone().server_key_path(Some(path.clone())))?;
    assert!(conn.ping().is_ok());
    drop(conn);

    fs::write(
        &path,
        include_bytes!("../../tests/fixtures/server-public-key.pem"),
    )?;
    let error = Conn::new(plain.clone().server_key_path(Some(path))).unwrap_err();
    assert!(matches!(error, Error::MySqlError(_)), "{error:?}");

    let error = Conn::new(plain.clone().pass(Some("wrong"))).unwrap_err();
    assert!(matches!(error, Error::MySqlError(_)), "{error:?}");

    let mut conn = Conn::new(
        plain
            .user(Some(fixture.empty_password_user()))
            .pass(None::<String>),
    )?;
    assert!(conn.ping().is_ok());
    drop(conn);

    // TLS: the password goes as is over the encrypted transport.
    if test_ssl() {
        let mut conn = Conn::new(fixture.client_opts())?;
        assert!(!conn.is_insecure());
        assert!(conn.ping().is_ok());
        drop(conn);

        let error = Conn::new(fixture.client_opts().pass(Some("wrong"))).unwrap_err();
        assert!(matches!(error, Error::MySqlError(_)), "{error:?}");
    }

    Ok(())
}
