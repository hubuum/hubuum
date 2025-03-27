use actix_web::{http, test};

/// ## Asserts that a given item is found within the specified vector.
///
/// This macro will panic at runtime if the specified item is not
/// found in the given vector. It is intended to be used in tests
/// and other non-production code where a failure to find an item
/// should result in a halt of execution.
///
/// ### Examples
///
/// ```rust,ignore
/// let vec = vec![1, 2, 3, 4];
/// assert_contains!(vec, 3); // Succeeds
/// assert_contains!(vec, 5); // Panics
/// ```
///
/// ### Panics
///
/// Panics if the item is not found in the vector, with a message
/// indicating the failure and the source location of the call.
///
/// ### Parameters
///
/// - `$vec`: The vector to search within. This argument should be
///   a `Vec<T>` where `T` is any type that implements `PartialEq`.
///
/// - `$item`: The item to search for within the vector. This
///   argument should have the same type as the elements of the
///   vector (`T`).
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

/// ## Asserts that a given item is not found within the specified vector.
///
/// This macro will panic at runtime if the specified item is
/// found in the given vector. It is intended to be used in tests
/// and other non-production code where finding an item should
/// result in a halt of execution.
///
/// ### Examples
///
/// ```rust,ignore
/// let vec = vec![1, 2, 3, 4];
/// assert_not_contains!(vec, 5); // Succeeds
/// assert_not_contains!(vec, 3); // Panics
/// ```
///
/// ### Panics
///
/// Panics if the item is found in the vector, with a message
/// indicating the failure and the source location of the call.
///
/// ### Parameters
///
/// - `$vec`: The vector to search within. This argument should be
///   a `Vec<T>` where `T` is any type that implements `PartialEq`.
///
/// - `$item`: The item to ensure is not within the vector. This
///   argument should have the same type as the elements of the
///   vector (`T`).
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

/// ## Asserts that all given items are found within the specified vector.
///
/// This macro will panic at runtime if any of the specified items are not
/// found in the given vector. It is intended to be used in tests
/// and other non-production code where a failure to find any item
/// from a set should result in a halt of execution.
///
/// The macro iterates over the list of items you wish to check for and
/// verifies each is present in the vector. If any item is not found,
/// the macro will panic, indicating which item was not found and
/// providing the source location of the call.
///
/// ### Examples
///
/// ```rust,ignore
/// let vec = vec![1, 2, 3, 4, 5];
/// assert_contains_all!(vec, [2, 3, 5]); // Succeeds
/// assert_contains_all!(vec, [0, 1, 2]); // Panics because 0 is not in `vec`
/// ```
///
/// ### Panics
///
/// Panics if any of the items are not found in the vector, with a message
/// indicating the failure and the source location of the call.
///
/// ### Parameters
///
/// - `$vec`: The vector to search within. This argument should be
///   a `Vec<T>` where `T` is any type that implements `PartialEq`.
///
/// - `$items`: The collection of items to search for within the vector.
///   This argument should be an array or a slice containing elements of the
///   same type as the elements of the vector (`T`).
#[macro_export]
macro_rules! assert_contains_all {
    ($vec:expr, $items:expr) => {
        for item in $items {
            if !$vec.iter().any(|v| v == item) {
                panic!(
                    "Assertion failed: item {:?} not found in vec {:?}. Called from: {}:{}",
                    item,
                    $vec,
                    file!(),
                    line!()
                );
            }
        }
    };
}
/// ## Assert identical IDs in two collections.
///
/// Asserts that two collections of instances with an ID field contain exactly the same unique IDs,
/// or that a collection of instances contains exactly the specified IDs.
///
/// This macro supports two modes of operation based on the input:
///
/// - Comparing the IDs of two collections of instances for exact match, regardless of order.
/// - Comparing the IDs in a single collection of instances against a specified set of IDs.
///
/// In either case, it checks that all specified IDs are present and that there are no extra or missing IDs.
/// It is intended for use in tests where ensuring the exact match of IDs is necessary.
///
/// ### Examples
///
/// ```rust,ignore
/// #[derive(Debug)]
/// struct HubuumClass { id: i32 }
///
/// let collection1 = vec![
///     HubuumClass { id: 1 },
///     HubuumClass { id: 2 },
///     HubuumClass { id: 3 },
/// ];
///
/// let collection2 = vec![
///     HubuumClass { id: 3 },
///     HubuumClass { id: 1 },
///     HubuumClass { id: 2 },
/// ];
///
/// assert_contains_same_ids!(collection1, collection2); // Succeeds
///
/// let specific_ids = &[1, 2, 3];
/// assert_contains_same_ids!(collection1, specific_ids); // Succeeds
/// ```
///
/// ### Panics
///
/// Panics if the sets of IDs do not match exactly, with a message indicating the missing and extra IDs,
/// and the source location of the call.
///
/// ### Parameters
///
/// - `$collection1` and `$collection2`: The collections of instances to compare. Each argument should be
///   a vector of instances.
///
/// - Alternatively, `$collection` is a vector of instances and `$ids` is a slice of `i32` representing
///   the expected unique IDs to be found in `$collection`.
#[macro_export]
macro_rules! assert_contains_same_ids {
    // Case where both arguments are references to vectors of instances
    ($collection1:expr, $collection2:expr) => {{
        let ids1: std::collections::HashSet<i32> = $collection1.iter().map(|item| item.id).collect();
        let ids2: std::collections::HashSet<i32> = $collection2.iter().map(|item| item.id).collect();

        if ids1 != ids2 {
            let missing_ids: Vec<_> = ids2.difference(&ids1).collect();
            let extra_ids: Vec<_> = ids1.difference(&ids2).collect();

            panic!(
                "Assertion failed: IDs do not match.\n Missing ids: {:?}\n Extra ids in collection: {:?}\nCalled from: {}:{}",
                missing_ids,
                extra_ids,
                file!(),
                line!()
            );
        }
    }};

    // Case where the first argument is a vector of instances and the second is a slice of i32
    ($collection:expr, $ids:expr) => {{
        let collected_ids: std::collections::HashSet<i32> = $collection.iter().map(|item| item.id).collect();
        let expected_ids: std::collections::HashSet<i32> = $ids.iter().cloned().collect();

        if collected_ids != expected_ids {
            let missing_ids: Vec<_> = expected_ids.difference(&collected_ids).collect();
            let extra_ids: Vec<_> = collected_ids.difference(&expected_ids).collect();

            panic!(
                "Assertion failed: IDs do not match.\n Missing ids: {:?}\n Extra ids in collection: {:?}\nCalled from: {}:{}",
                missing_ids,
                extra_ids,
                file!(),
                line!()
            );
        }
    }};
}

// Assuming the `HubuumClass` struct is defined as before

/// ## Asserts that the response status matches the expected HTTP status code.
///
/// This function is an asynchronous test utility designed to compare the HTTP status code
/// of an `actix_web::dev::ServiceResponse` with an expected `http::StatusCode`. It is primarily
/// intended for use in automated tests where verifying the HTTP response status is necessary.
///
/// If the actual response status does not match the expected status, the function will panic,
/// displaying the unexpected response status and the response body.
///
/// ### Examples
///
/// ```rust,ignore
/// #[actix_web::test]
/// async fn test_api_classes_get() {
///     let created_classes = create_test_classes("get").await;
///     let (pool, admin_token, _) = setup_pool_and_tokens().await;
///
///     let resp = get_request(&pool, &admin_token, CLASSES_ENDPOINT).await;
///     let resp = assert_response_status(resp, http::StatusCode::OK).await;
///     let classes: Vec<crate::models::class::HubuumClass> = test::read_body_json(resp).await;@
///     assert_contains_all!(&classes, &created_classes);
/// }
/// ```
///
/// ### Parameters
///
/// - `resp`: The response to check. This is an instance of `actix_web::dev::ServiceResponse`
///   that you want to verify the HTTP status code of.
///
/// - `expected_status`: The expected HTTP status code (`http::StatusCode`) for the response.
///
/// ### Returns
///
/// Returns the original `actix_web::dev::ServiceResponse` object, allowing for further
/// assertions or manipulations in the test chain.
///
/// ### Panics
///
/// Panics if the response's status code does not match the expected status code, providing
/// a detailed error message that includes the unexpected response body.
pub async fn assert_response_status(
    resp: actix_web::dev::ServiceResponse,
    expected_status: http::StatusCode,
) -> actix_web::dev::ServiceResponse {
    assert_eq!(
        resp.status(),
        expected_status,
        "Unexpected response status: {:?} ({:?}).",
        resp.request().uri().clone(),
        test::read_body(resp).await,
    );
    resp
}
