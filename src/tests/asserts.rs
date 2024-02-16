use actix_web::{http, test};

#[macro_export]
macro_rules! assert_contains {
    ($vec:expr, $item:expr) => {
        if !$vec.iter().any(|v| v == $item) {
            panic!(
                "Assertion failed: item not found in vec. Called from: {}:{}",
                file!(),
                line!()
            );
        }
    };
}

#[macro_export]
macro_rules! assert_not_contains {
    ($vec:expr, $item:expr) => {
        if $vec.iter().any(|v| v == $item) {
            panic!(
                "Assertion failed: item found in vec. Called from: {}:{}",
                file!(),
                line!()
            );
        }
    };
}

#[macro_export]
macro_rules! assert_contains_all {
    ($vec:expr, $items:expr) => {
        for item in $items {
            if !$vec.iter().any(|v| v == item) {
                panic!(
                    "Assertion failed: item not found in vec. Called from: {}:{}",
                    file!(),
                    line!()
                );
            }
        }
    };
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
