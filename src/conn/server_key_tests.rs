//! Regression tests for configured caching_sha2_password server RSA keys.
//!
//! The live test uses fresh MySQL accounts to force full authentication rather
//! than allowing the server's authentication cache to bypass the RSA exchange.

use std::{fs, io, path::PathBuf, time::Duration};

use crate::{prelude::Queryable, test_misc::get_opts, Conn, Error, Opts, OptsBuilder};

/// Bounds connection failures while retaining the standard live-test configuration.
fn live_opts() -> OptsBuilder {
    get_opts()
        .tcp_connect_timeout(Some(Duration::from_secs(5)))
        .read_timeout(Some(Duration::from_secs(10)))
        .write_timeout(Some(Duration::from_secs(10)))
}

/// The builder and URL share the same optional path without enabling TLS.
#[test]
fn server_key_path_options() {
    assert_eq!(Opts::default().get_server_key_path(), None);
    let path = PathBuf::from("keys/server key.pem");
    let opts = Opts::from(OptsBuilder::new().server_key_path(Some(path.clone())));
    assert_eq!(opts.get_server_key_path(), Some(path.as_path()));
    let parsed =
        Opts::from_url("mysql://localhost/db?server_key_path=keys%2Fserver%20key.pem").unwrap();
    assert_eq!(parsed.get_server_key_path(), opts.get_server_key_path());
    assert!(parsed.get_ssl_opts().is_none());
    let cleared = Opts::from(OptsBuilder::from_opts(parsed).server_key_path(None));
    assert_eq!(cleared.get_server_key_path(), None);
}

/// Owns a unique test account and its temporary public-key file, including on panic.
struct ServerKeyFixture {
    admin: Conn,
    user: String,
    directory: PathBuf,
}

impl ServerKeyFixture {
    /// Recreates the account so the next connection cannot take the cached-auth path.
    fn reset_user(&mut self) -> crate::Result<()> {
        self.admin
            .query_drop(format!("DROP USER IF EXISTS '{}'@'%'", self.user))?;
        self.admin.query_drop(format!(
            "CREATE USER '{}'@'%' IDENTIFIED WITH caching_sha2_password BY 'server-key-test'",
            self.user
        ))
    }

    /// Forces TCP without TLS or inherited initialization commands for the test account.
    fn client_opts(&self) -> OptsBuilder {
        live_opts()
            .user(Some(self.user.clone()))
            .pass(Some("server-key-test"))
            .db_name(None::<String>)
            .init(Vec::<String>::new())
            .socket(None::<String>)
            .prefer_socket(false)
            .ssl_opts(None)
    }

    /// Returns the single file owned by this fixture.
    fn key_path(&self) -> PathBuf {
        self.directory.join("server-public-key.pem")
    }
}

impl Drop for ServerKeyFixture {
    /// Cleans up the account and only the exact temporary paths created by this test.
    fn drop(&mut self) {
        let _ = self
            .admin
            .query_drop(format!("DROP USER IF EXISTS '{}'@'%'", self.user));
        let _ = fs::remove_file(self.key_path());
        let _ = fs::remove_dir(&self.directory);
    }
}

/// Valid, wrong and missing keys exercise full auth; no option retains key retrieval.
#[test]
fn server_key_path_full_authentication() -> Result<(), Box<dyn std::error::Error>> {
    let mut admin = Conn::new(live_opts().ssl_opts(None))?;
    if admin.0.mariadb_server_version.is_some() || admin.server_version() < (8, 0, 5) {
        return Ok(());
    }
    let (_, public_key): (String, String) = admin
        .query_first("SHOW STATUS LIKE 'Caching_sha2_password_rsa_public_key'")?
        .ok_or("MySQL did not expose its caching_sha2_password RSA key")?;
    assert!(
        !public_key.is_empty(),
        "MySQL must have an authentication RSA key"
    );
    let id = rand::random::<u64>();
    let directory =
        std::env::temp_dir().join(format!("mysql-server-key-{}-{id}", std::process::id()));
    fs::create_dir(&directory)?;
    let mut fixture = ServerKeyFixture {
        admin,
        user: format!("__server_key_{id:016x}"),
        directory,
    };
    let path = fixture.key_path();
    let opts = fixture.client_opts().server_key_path(Some(path.clone()));

    fixture.reset_user()?;
    fs::write(&path, public_key)?;
    let mut conn = Conn::new(opts.clone())?;
    assert!(conn.is_insecure());
    assert!(conn.ping().is_ok());
    drop(conn);

    fixture.reset_user()?;
    fs::write(
        &path,
        include_bytes!("../../tests/fixtures/server-public-key.pem"),
    )?;
    let error = Conn::new(opts.clone()).unwrap_err();
    assert!(matches!(error, Error::MySqlError(_)), "{error:?}");

    fixture.reset_user()?;
    fs::remove_file(&path)?;
    let error = Conn::new(opts.clone()).unwrap_err();
    assert!(
        matches!(error, Error::IoError(ref e) if e.kind() == io::ErrorKind::NotFound),
        "{error:?}"
    );

    fixture.reset_user()?;
    let mut conn = Conn::new(opts.clone().server_key_path(None))?;
    assert!(conn.is_insecure());
    assert!(conn.ping().is_ok());
    drop(conn);

    // Cached authentication must not read the missing file or perform an RSA exchange.
    let mut conn = Conn::new(opts)?;
    assert!(conn.ping().is_ok());
    Ok(())
}
