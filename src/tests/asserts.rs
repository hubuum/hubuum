use actix_web::{http, test};

pub fn assert_contains<T: PartialEq>(vec: &[T], item: &T) {
    assert!(vec.iter().any(|v| v == item));
}

pub fn assert_contains_all<T: PartialEq>(vec: &[T], items: &[T]) {
    assert!(items.iter().all(|item| vec.contains(item)));
}

pub async fn assert_response_status(
    resp: actix_web::dev::ServiceResponse,
    expected_status: http::StatusCode,
) -> actix_web::dev::ServiceResponse {
    assert_eq!(
        resp.status(),
        expected_status,
        "Unexpected response status: {:?}",
        test::read_body(resp).await
    );
    resp
}
