#![cfg(feature = "rustls")]

use std::{
    fs::File,
    io::{self, Read},
    sync::Arc,
};

use bufstream::BufStream;
use rustls::{
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        WebPkiServerVerifier,
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
    CertificateError, ClientConfig, Error, RootCertStore, SignatureScheme,
};
use rustls_pemfile::certs;

use crate::{
    error::tls::TlsError,
    io::{Stream, TcpStream},
    Result, SslOpts,
};

impl Stream {
    pub fn make_secure(self, host: url::Host, ssl_opts: SslOpts) -> Result<Stream> {
        if self.is_socket() {
            // won't secure socket connection
            return Ok(self);
        }

        let domain = match host {
            url::Host::Domain(domain) => domain,
            url::Host::Ipv4(ip) => ip.to_string(),
            url::Host::Ipv6(ip) => ip.to_string(),
        };

        let mut root_store = RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|x| x.to_owned()));

        if let Some(root_cert_path) = ssl_opts.root_cert_path() {
            let mut root_cert_data = vec![];
            let mut root_cert_file = File::open(root_cert_path)?;
            root_cert_file.read_to_end(&mut root_cert_data)?;

            let mut root_certs = Vec::new();
            for cert in certs(&mut &*root_cert_data) {
                root_certs.push(cert?);
            }

            if root_certs.is_empty() && !root_cert_data.is_empty() {
                root_certs.push(CertificateDer::from(root_cert_data));
            }

            for cert in &root_certs {
                root_store.add(cert.to_owned())?;
            }
        }

        let mut provider = (**ClientConfig::builder().crypto_provider()).clone();
        if let Some(cipher_suites) = ssl_opts.cipher_suites() {
            provider.cipher_suites.retain(|x| {
                x.suite()
                    .as_str()
                    .map(|name| cipher_suites.iter().any(|x| x == name))
                    .unwrap_or_default()
            })
        }
        let config_builder = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(root_store.clone());

        let mut config = if let Some(identity) = ssl_opts.client_identity() {
            let (cert_chain, priv_key) = identity.load()?;
            config_builder.with_client_auth_cert(cert_chain, priv_key)?
        } else {
            config_builder.with_no_client_auth()
        };

        let server_name = ServerName::try_from(domain.as_str())
            .map_err(|_| webpki::InvalidDnsNameError)?
            .to_owned();
        let mut dangerous = config.dangerous();
        let web_pki_verifier = WebPkiServerVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(TlsError::from)?;
        let dangerous_verifier = DangerousVerifier::new(
            ssl_opts.accept_invalid_certs(),
            ssl_opts.skip_domain_validation(),
            web_pki_verifier,
        );
        dangerous.set_certificate_verifier(Arc::new(dangerous_verifier));

        match self {
            Stream::TcpStream(tcp_stream) => match tcp_stream {
                TcpStream::Insecure(insecure_stream) => {
                    let inner = insecure_stream
                        .into_inner()
                        .map_err(io::Error::from)
                        .unwrap();
                    let conn =
                        rustls::ClientConnection::new(Arc::new(config), server_name).unwrap();
                    let secure_stream = rustls::StreamOwned::new(conn, inner);
                    Ok(Stream::TcpStream(TcpStream::Secure(BufStream::new(
                        Box::new(secure_stream),
                    ))))
                }
                TcpStream::Secure(_) => Ok(Stream::TcpStream(tcp_stream)),
            },
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
struct DangerousVerifier {
    accept_invalid_certs: bool,
    skip_domain_validation: bool,
    verifier: Arc<WebPkiServerVerifier>,
}

impl DangerousVerifier {
    fn new(
        accept_invalid_certs: bool,
        skip_domain_validation: bool,
        verifier: Arc<WebPkiServerVerifier>,
    ) -> Self {
        Self {
            accept_invalid_certs,
            skip_domain_validation,
            verifier,
        }
    }

    fn invalid_signature_assertion(&self) -> Option<HandshakeSignatureValid> {
        self.accept_invalid_certs
            .then(HandshakeSignatureValid::assertion)
    }
}

impl ServerCertVerifier for DangerousVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if self.accept_invalid_certs {
            Ok(ServerCertVerified::assertion())
        } else {
            match self.verifier.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                ocsp_response,
                now,
            ) {
                Ok(assertion) => Ok(assertion),
                Err(Error::InvalidCertificate(
                    CertificateError::NotValidForName
                    | CertificateError::NotValidForNameContext { .. },
                )) if self.skip_domain_validation => Ok(ServerCertVerified::assertion()),
                Err(e) => Err(e),
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        match self.invalid_signature_assertion() {
            Some(assertion) => Ok(assertion),
            None => self.verifier.verify_tls12_signature(message, cert, dss),
        }
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        match self.invalid_signature_assertion() {
            Some(assertion) => Ok(assertion),
            None => self.verifier.verify_tls13_signature(message, cert, dss),
        }
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        if self.accept_invalid_certs {
            vec![
                SignatureScheme::RSA_PKCS1_SHA1,
                SignatureScheme::ECDSA_SHA1_Legacy,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
                SignatureScheme::ED448,
                SignatureScheme::ML_DSA_44,
                SignatureScheme::ML_DSA_65,
                SignatureScheme::ML_DSA_87,
                SignatureScheme::Unknown(0x0809), // rsa_pss_pss_sha256
                SignatureScheme::Unknown(0x080A), // rsa_pss_pss_sha384
                SignatureScheme::Unknown(0x080B), // rsa_pss_pss_sha512
                SignatureScheme::Unknown(0x0202), // dsa_sha1
                SignatureScheme::Unknown(0x0402), // dsa_sha256
                SignatureScheme::Unknown(0x0502), // dsa_sha384
                SignatureScheme::Unknown(0x0602), // dsa_sha512
                SignatureScheme::Unknown(0x0420), // rsa_pkcs1_sha256_legacy
                SignatureScheme::Unknown(0x0520), // rsa_pkcs1_sha384_legacy
                SignatureScheme::Unknown(0x0620), // rsa_pkcs1_sha512_legacy
                SignatureScheme::Unknown(0x0701), // Russian GOST 2012 - 256 bit
                SignatureScheme::Unknown(0x0702), // Russian GOST 2012 - 512 bit
                SignatureScheme::Unknown(0x0810), // Chinese National Standard
            ]
        } else {
            self.verifier.supported_verify_schemes()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write as _, net::TcpListener, thread, time::Duration};

    use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
    use rustls::pki_types::UnixTime;

    use super::*;

    fn verify(
        skip_domain_validation: bool,
        name: &str,
        server_cert_path: &str,
    ) -> Result<ServerCertVerified, Error> {
        let ca = std::fs::read("tests/ca.crt").unwrap();
        let mut root_store = RootCertStore::empty();
        root_store
            .add(certs(&mut &*ca).next().unwrap().unwrap())
            .unwrap();
        let verifier = WebPkiServerVerifier::builder(Arc::new(root_store))
            .build()
            .unwrap();
        let dangerous_verifier = DangerousVerifier::new(false, skip_domain_validation, verifier);

        let server_cert = std::fs::read(server_cert_path).unwrap();
        let end_entity = certs(&mut &*server_cert).next().unwrap().unwrap();
        let server_name = ServerName::try_from(name).unwrap();

        dangerous_verifier.verify_server_cert(&end_entity, &[], &server_name, &[], UnixTime::now())
    }

    // tests/server.crt SANs are 127.0.0.1 and localhost, so any other name is a mismatch
    // while still chaining to the trusted CA.
    #[test]
    fn skip_domain_validation_ignores_hostname_mismatch() {
        assert!(verify(true, "wrong.example", "tests/server.crt").is_ok());
    }

    #[test]
    fn hostname_mismatch_fails_without_skip_domain_validation() {
        assert!(verify(false, "wrong.example", "tests/server.crt").is_err());
    }

    #[test]
    fn skip_domain_validation_does_not_affect_matching_hostname() {
        assert!(verify(true, "localhost", "tests/server.crt").is_ok());
    }

    // tests/other-server.crt is signed by tests/other-ca.crt, not tests/ca.crt, so it
    // must still be rejected even with a matching name and skip_domain_validation set.
    #[test]
    fn skip_domain_validation_does_not_bypass_unknown_issuer() {
        assert!(verify(true, "localhost", "tests/other-server.crt").is_err());
    }

    fn verifier(accept_invalid_certs: bool) -> DangerousVerifier {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        DangerousVerifier::new(accept_invalid_certs, false, verifier)
    }

    #[test]
    fn invalid_x509_v1_is_only_fully_accepted_in_dangerous_mode() {
        let certificate =
            CertificateDer::from(include_bytes!("../../../tests/fixtures/x509-v1.der").as_slice());
        let server_name = ServerName::try_from("localhost").unwrap();
        let dangerous = verifier(true);
        let strict = verifier(false);

        assert!(dangerous
            .verify_server_cert(
                &certificate,
                &[],
                &server_name,
                &[],
                rustls::pki_types::UnixTime::now()
            )
            .is_ok());
        assert!(dangerous.invalid_signature_assertion().is_some());
        assert!(strict
            .verify_server_cert(
                &certificate,
                &[],
                &server_name,
                &[],
                rustls::pki_types::UnixTime::now()
            )
            .is_err());
        assert!(strict.invalid_signature_assertion().is_none());
    }

    fn start_openssl_server(addr: &'static str) {
        // OpenSSL handles X.509 v1 without rejecting its structure outright
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();

        // Replace these paths with your specific local v1 certificate files
        acceptor
            .set_private_key_file("tests/fixtures/rsa_private.key", SslFiletype::PEM)
            .expect("Failed to load server private key");
        acceptor
            .set_certificate_chain_file("tests/fixtures/cert.pem")
            .expect("Failed to load server v1 certificate");

        let acceptor = Arc::new(acceptor.build());
        let listener = TcpListener::bind(addr).unwrap();

        thread::spawn(move || {
            // Accept exactly one connection for our integration test
            if let Some(Ok(stream)) = listener.incoming().next() {
                let acceptor = acceptor.clone();
                if let Ok(mut ssl_stream) = acceptor.accept(stream) {
                    // Read client payload
                    let mut buf = [0u8; 12];
                    if ssl_stream.read_exact(&mut buf).is_ok() {
                        // Send an echo response back to the rustls client
                        let _ = ssl_stream.write_all(b"HELLO-RUSTLS");
                        let _ = ssl_stream.flush();
                    }
                }
            }
        });
    }

    #[test]
    fn test_invalid_cert_handshake() {
        // Define an ephemeral port/address for testing
        let test_addr = "127.0.0.1:28443";

        // Install default crypto provider for rustls (required in rustls 0.23+)
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // 1. Spin up the permissive OpenSSL server in the background
        start_openssl_server(test_addr);

        // Give the background thread a brief moment to bind the TCP port
        thread::sleep(Duration::from_millis(100));

        // 2. Configure the Rustls Client to use our permissive verifier
        let client_config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier(true)))
            .with_no_client_auth();
        let client_config = Arc::new(client_config);

        // Connect via standard TCP loopback
        let mut tcp_stream = std::net::TcpStream::connect(test_addr)
            .expect("Failed to connect to OpenSSL TCP server");

        let server_name = "localhost".try_into().unwrap();
        let mut client_tls = rustls::ClientConnection::new(client_config, server_name).unwrap();

        // 3. Complete the TLS Handshake and communicate via Stream wrapper
        // Using rustls::Stream helper simplifies the read/write network coordination loop.
        let mut tls_stream = rustls::Stream::new(&mut client_tls, &mut tcp_stream);

        // Send data to OpenSSL server
        tls_stream
            .write_all(b"HELLO-OPENSSL")
            .expect("Failed writing to server");
        tls_stream.flush().unwrap();

        // Read the response from OpenSSL server
        let mut response_buf = [0u8; 12];
        tls_stream
            .read_exact(&mut response_buf)
            .expect("Failed reading from server");

        // 4. Assert correctness
        assert_eq!(&response_buf, b"HELLO-RUSTLS");
    }
}
