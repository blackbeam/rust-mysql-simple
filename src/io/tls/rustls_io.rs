#![cfg(feature = "rustls-tls")]

use std::{
    convert::TryInto,
    fs::File,
    io::{self, Read},
    sync::Arc,
};

use bufstream::BufStream;
use rustls::{
    client::{ServerCertVerifier, WebPkiVerifier},
    Certificate, ClientConfig, OwnedTrustAnchor, RootCertStore,
};
use rustls_pemfile::certs;

use crate::{
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
        root_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
            OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        }));

        if let Some(root_cert_path) = ssl_opts.root_cert_path() {
            let mut root_cert_data = vec![];
            let mut root_cert_file = File::open(root_cert_path)?;
            root_cert_file.read_to_end(&mut root_cert_data)?;

            let mut root_certs = Vec::new();
            for cert in certs(&mut &*root_cert_data)? {
                root_certs.push(Certificate(cert));
            }

            if root_certs.is_empty() && !root_cert_data.is_empty() {
                root_certs.push(Certificate(root_cert_data));
            }

            for cert in &root_certs {
                root_store.add(cert)?;
            }
        }

        let config_builder = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store.clone());

        let mut config = if let Some(identity) = ssl_opts.client_identity() {
            let (cert_chain, priv_key) = identity.load()?;
            config_builder.with_single_cert(cert_chain, priv_key)?
        } else {
            config_builder.with_no_client_auth()
        };

        let server_name = domain
            .as_str()
            .try_into()
            .map_err(|_| webpki::InvalidDnsNameError)?;
        let mut dangerous = config.dangerous();
        let web_pki_verifier = WebPkiVerifier::new(root_store, None);
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
                        secure_stream,
                    ))))
                }
                TcpStream::Secure(_) => Ok(Stream::TcpStream(tcp_stream)),
            },
            _ => unreachable!(),
        }
    }
}

struct DangerousVerifier {
    accept_invalid_certs: bool,
    skip_domain_validation: bool,
    verifier: WebPkiVerifier,
}

impl DangerousVerifier {
    fn new(
        accept_invalid_certs: bool,
        skip_domain_validation: bool,
        verifier: WebPkiVerifier,
    ) -> Self {
        Self {
            accept_invalid_certs,
            skip_domain_validation,
            verifier,
        }
    }
}

impl ServerCertVerifier for DangerousVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &rustls::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: std::time::SystemTime,
    ) -> std::result::Result<rustls::client::ServerCertVerified, rustls::Error> {
        if self.accept_invalid_certs {
            Ok(rustls::client::ServerCertVerified::assertion())
        } else {
            match self.verifier.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                scts,
                ocsp_response,
                now,
            ) {
                Ok(assertion) => Ok(assertion),
                Err(ref e)
                    if e.to_string().contains("CertNotValidForName")
                        && self.skip_domain_validation =>
                {
                    Ok(rustls::client::ServerCertVerified::assertion())
                }
                Err(e) => Err(e),
            }
        }
    }
}
