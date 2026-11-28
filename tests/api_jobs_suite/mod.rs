mod backups;
mod export_templates;
mod exports;
mod imports;
mod remote_targets;
mod restores;
mod schema_profiling;
mod tasks;

/// Read the exact HTTP representation before decoding JSON or text.
async fn read_verified_output(response: actix_web::dev::ServiceResponse) -> actix_web::web::Bytes {
    use crate::tests::asserts::header_value;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use sha2::{Digest, Sha256};

    let size = header_value(&response, "Content-Length")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let digest = header_value(&response, "Content-Digest").unwrap();
    assert!(header_value(&response, "Content-Encoding").is_none());
    let bytes = actix_web::test::read_body(response).await;
    assert_eq!(bytes.len(), size);
    assert_eq!(
        digest,
        format!("sha-256=:{}:", STANDARD.encode(Sha256::digest(&bytes)))
    );
    bytes
}
