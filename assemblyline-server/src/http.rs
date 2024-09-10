use anyhow::Result;
use futures::Stream;
use poem::listener::{Listener, OpensslTlsConfig, TcpListener};

use crate::config::TLSConfig;

pub type TlsAcceptor = poem::listener::OpensslTlsAcceptor<poem::listener::TcpAcceptor, std::pin::Pin<Box<dyn Stream<Item = OpensslTlsConfig> + Send>>>;

pub async fn create_tls_binding(bind_address: std::net::SocketAddr, tls: Option<TLSConfig>) -> Result<TlsAcceptor> {
    let listener = TcpListener::bind(bind_address);

    let tls_config = match tls {
        Some(tls) => {
            OpensslTlsConfig::new()
                .cert_from_data(tls.certificate_pem)
                .key_from_data(tls.key_pem)
        },
        None => crate::config::generate_certificate()?
    };
    let listener = listener.openssl_tls(tls_config);
    let acceptor: TlsAcceptor = listener.into_acceptor().await?;
    Ok(acceptor)
}