use actix_web::{HttpResponse, HttpResponseBuilder};
use base64::{Engine, engine::general_purpose::STANDARD};

use crate::services::artifacts::OutputDownload;

impl OutputDownload {
    pub(crate) fn respond(self, response: &mut HttpResponseBuilder) -> HttpResponse {
        let digest = STANDARD.encode(self.sha256());
        response
            .insert_header(("Content-Digest", format!("sha-256=:{digest}:")))
            .insert_header(("Cache-Control", "no-store"))
            .no_chunking(self.byte_size())
            .streaming(self.into_stream())
    }
}
