#![cfg(feature = "native-tls")]

use std::{
    fs::File,
    io::{self, Read},
};

use bufstream::BufStream;
use native_tls::{Certificate, TlsConnector};

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

        let mut builder = TlsConnector::builder();
        if let Some(root_cert_path) = ssl_opts.root_cert_path() {
            let mut root_cert_data = vec![];
            let mut root_cert_file = File::open(root_cert_path)?;
            root_cert_file.read_to_end(&mut root_cert_data)?;

            let root_certs = Certificate::from_der(&*root_cert_data)
                .map(|x| vec![x])
                .or_else(|_| {
                    pem::parse_many(&*root_cert_data)
                        .unwrap_or_default()
                        .iter()
                        .map(pem::encode)
                        .map(|s| Certificate::from_pem(s.as_bytes()))
                        .collect()
                })?;

            for root_cert in root_certs {
                builder.add_root_certificate(root_cert);
            }
        }
        if let Some(client_identity) = ssl_opts.client_identity() {
            let identity = client_identity.load()?;
            builder.identity(identity);
        }
        builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
        builder.danger_accept_invalid_certs(ssl_opts.accept_invalid_certs());
        let tls_connector = builder.build()?;
        match self {
            Stream::TcpStream(tcp_stream) => match tcp_stream {
                TcpStream::Insecure(insecure_stream) => {
                    let inner = insecure_stream.into_inner().map_err(io::Error::from)?;
                    let secure_stream = tls_connector.connect(&domain, inner)?;
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
