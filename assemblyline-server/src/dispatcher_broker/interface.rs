use std::sync::Arc;

use poem::http::StatusCode;
use poem::listener::{TcpListener, OpensslTlsConfig};
use poem::web::{Data, Json, Redirect, Path};
use poem::{EndpointExt, post, get, Server, handler, Response};
use serde::Deserialize;

use crate::logging::LoggerMiddleware;

use super::{BrokerSession, Error};


pub async fn start_interface(session: Arc<BrokerSession>) -> Result<(), Error> {

    let app = poem::Route::new()
        .at("/submission", post(post_submission))
        .at("/submission/:sid", get(get_submission))
        .at("/relocate", post(post_relocate))
        .at("/health", get(get_health))
        .data(session.clone())
        .with(LoggerMiddleware);

    let config = session.config.core.dispatcher.broker;

    let listener = TcpListener::bind(config.bind_address);
    let tls_config = match config.tls {
        Some(tls) => {
            OpensslTlsConfig::new()
                .cert_from_data(tls.certificate_pem)
                .key_from_data(tls.key_pem)
        },
        None => {
            use openssl::{rsa::Rsa, x509::X509, pkey::PKey, asn1::{Asn1Integer, Asn1Time}, bn::BigNum};

            // Generate our keypair
            let key = Rsa::generate(1 << 11)?;
            let pkey = PKey::from_rsa(key)?;

            // Use that keypair to sign a certificate
            let mut builder = X509::builder()?;

            // Set serial number to 1
            let one = BigNum::from_u32(1)?;
            let serial_number = Asn1Integer::from_bn(&one)?;
            builder.set_serial_number(&serial_number)?;

            // set subject/issuer name
            let mut name = openssl::x509::X509NameBuilder::new()?;
            name.append_entry_by_text("C", "CA")?;
            name.append_entry_by_text("ST", "ON")?;
            name.append_entry_by_text("O", "Inside the house")?;
            name.append_entry_by_text("CN", "localhost")?;
            let name = name.build();
            builder.set_issuer_name(&name)?;
            builder.set_subject_name(&name)?;

            // Set not before/after
            let not_before = Asn1Time::from_unix((chrono::Utc::now() - chrono::Duration::days(1)).timestamp())?;
            builder.set_not_before(&not_before)?;
            let not_after = Asn1Time::from_unix((chrono::Utc::now() + chrono::Duration::days(366)).timestamp())?;
            builder.set_not_after(&not_after)?;

            // set public key
            builder.set_pubkey(&pkey)?;

            // sign and build
            builder.sign(&pkey, openssl::hash::MessageDigest::sha256())?;
            let cert = builder.build();

            OpensslTlsConfig::new()
                .cert_from_data(cert.to_pem()?)
                .key_from_data(pkey.rsa()?.private_key_to_pem()?)
        }
    };
    let listener = listener.openssl_tls(tls_config);

    let exit = tokio::spawn(async {
        todo!()
    });

    Server::new(listener)
        .run_with_graceful_shutdown(app, exit, None)
        .await
}

#[derive(Deserialize)]
pub struct StartSubmissionRequest {
    submission: assemblyline_models::datastore::Submission
}

#[handler]
async fn post_submission(
    Data(session): Data<&Arc<BrokerSession>>, 
    Json(request): Json<StartSubmissionRequest>,
) -> Response {
    // check if this submission belongs here or should be forwarded
    let target_instance = session.assign_sid(request.submission.sid);
    if session.instance != target_instance {
        let url = format!("https://{}/{}/submission", session.peer_address(target_instance), session.config.core.dispatcher.broker.path);
        return Redirect::temporary(url).into().into();
    }

    // check if the submission is already present
    if session.database.have_submission(&request.submission.sid).await? {
        return StatusCode::OK.into()
    }

    // assign to a dispatcher
    let dispatcher = session.select_dispatcher();
    
    // save to disk
    session.database.assign_submission(&request).await?;

    // notify the dispatcher
    session.http_client.post();

    // Return success
    StatusCode::OK.into()
}

#[handler]
fn get_submission(
    Data(session): Data<&Arc<BrokerSession>>, 
    Path(sid): Path<String>
) -> Response {
    // check if this submission belongs here or should be forwarded
    let target_instance = session.assign_sid(sid);
    if session.instance != target_instance {
        return Redirect::temporary(session.peer_address(target_instance)).into().into();
    }
    
    // get submission data
    todo!();
}

#[handler]
fn post_relocate() -> Response {
    // check if this submission belongs here or should be rejected
    todo!();

    // check if the submission is already present
    todo!();

    // assign to a dispatcher
    todo!();

    // save to disk
    todo!();
}

#[handler]
fn get_health() -> Response {
    todo!();
}
