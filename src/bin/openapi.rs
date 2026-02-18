use hubuum::api::openapi::ApiDoc;
use utoipa::OpenApi;

fn main() {
    let openapi = ApiDoc::openapi();
    println!(
        "{}",
        serde_json::to_string_pretty(&openapi).expect("failed to serialize OpenAPI document")
    );
}
