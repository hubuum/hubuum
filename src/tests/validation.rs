#![cfg(test)]

use serde_json::Value;
use yare::parameterized;

use crate::models::{Namespace, NewHubuumClass, NewHubuumObject, UpdateHubuumObject};
use crate::tests::constants::{get_schema, SchemaType};
use crate::tests::{create_namespace, get_pool_and_config};
use crate::traits::{CanDelete, CanSave, Validate, ValidateAgainstSchema};
use crate::errors::ApiError;

/// Sets up a Geo class (with its namespace) for testing.
/// The identifier string is used to create unique names.
async fn setup_geo_class(identifier: &str) -> (Namespace, NewHubuumClass, Value) {
    let (pool, _) = get_pool_and_config().await;
    println!("Creating namespace for test: {}", identifier);
    let ns = create_namespace(&pool, &format!("Validation_test_{}", identifier))
        .await
        .expect("Failed to create namespace");
    // Get the Geo schema.
    let schema = get_schema(SchemaType::Geo);
    let new_class = NewHubuumClass {
        name: format!("{}_class", identifier),
        validate_schema: Some(true),
        json_schema: Some(schema.clone()),
        namespace_id: ns.id,
        description: "Geo class".to_string(),
    };
    (ns, new_class, schema.clone())
}

/// Asserts that a validation result (Ok or Err) matches the expected value.
/// The `context` parameter is used to provide more context if the assertion fails.
fn assert_validation_result(result: Result<(), ApiError>, expected: bool, context: &str) {
    if expected {
        assert!(
            result.is_ok(),
            "{} failed, but it was expected to pass: {:?}",
            context,
            result.err()
        );
    } else {
        assert!(
            result.is_err(),
            "{} passed, but it was expected to fail: {:?}",
            context,
            result
        );
    }
}

#[parameterized(    
    ok_40_74 = { r#"{"latitude": 40.7128, "longitude": -74.0060}"#, true },
    failed_91_74 = { r#"{"latitude": 91, "longitude": 200}"#, false },
    failed_neg91_74 = { r#"{"latitude": -91, "longitude": 200}"#, false },
    failed_40_181 = { r#"{"latitude": 40.7128, "longitude": 181}"#, false },
    failed_40_neg181 = { r#"{"latitude": 40.7128, "longitude": -181}"#, false },
    failed_100_200 = { r#"{"latitude": 100, "longitude": 200}"#, false },
    failed_lat_missing = { r#"{"longitude": 0}"#, false },
    failed_long_missing = { r#"{"latitude": 0}"#, false },
    ok_extra_fields = { r#"{"latitude": 40.7128, "longitude": -74.0060, "extra_field": "value"}"#, true },
)]
#[test_macro(actix_web::test)]
async fn test_validate_object(json_data: &str, expected: bool) {
    let data = serde_json::from_str::<Value>(json_data).unwrap();
    let obj_name = format!("{}_test_validate_object", json_data);

    let (pool, _) = get_pool_and_config().await;
    let (ns, new_class, _schema) = setup_geo_class(&format!("new_{}", json_data)).await;

    // Save the class and build the new object.
    let class = new_class.save(&pool).await.expect("Failed to create class");
    let object = NewHubuumObject {
        name: obj_name,
        namespace_id: ns.id,
        hubuum_class_id: class.id,
        data: data.clone(),
        description: "Test object".to_string(),
    };

    // First, test the direct schema validation.
    let schema_validate = object
        .validate_against_schema(class.json_schema.as_ref().unwrap())
        .await;
    assert_validation_result(schema_validate, expected, "Schema validation");

    // Then, test the full object validation that fetches the class from the DB.
    let object_validate = object.validate(&pool).await;
    assert_validation_result(object_validate, expected, "Object validation");

    ns.delete(&pool).await.expect("Failed to delete namespace");
}

#[parameterized(    
    ok_40_74 = { r#"{"latitude": -40.7128, "longitude": 74.0060}"#, true },
    failed_91_74 = { r#"{"latitude": 91, "longitude": 75}"#, false },
    failed_neg91_74 = { r#"{"latitude": -91, "longitude": 74}"#, false },
    failed_lat_missing = { r#"{"longitude": 0}"#, false },
    failed_long_missing = { r#"{"latitude": 0}"#, false },
)]
#[test_macro(actix_web::test)]
async fn test_validate_update_object(json_data: &str, expected: bool) {
    // The base data for the original object.
    let base_data = r#"{"latitude": 40.7128, "longitude": -74.0060}"#;
    let obj_name = format!("{}_test_validate_update_object", json_data);
    
    let base_data = serde_json::from_str::<Value>(base_data).unwrap();
    let updated_data = serde_json::from_str::<Value>(json_data).unwrap();

    let (pool, _) = get_pool_and_config().await;
    let (ns, new_class, _schema) = setup_geo_class(&format!("update_{}", json_data)).await;

    // Save the class and then create an object with the base data.
    let class = new_class.save(&pool).await.expect("Failed to create class");
    let object = NewHubuumObject {
        name: obj_name,
        namespace_id: ns.id,
        hubuum_class_id: class.id,
        data: base_data,
        description: "Test object".to_string(),
    }.save(&pool).await.expect("Failed to create object");

    // Build the update with the new JSON data.
    let update_object = UpdateHubuumObject {
        name: None,
        namespace_id: None,
        hubuum_class_id: None,
        description: None,        
        data: Some(updated_data.clone()),
    };

    let validate = (&update_object, object.id).validate(&pool).await;
    assert_validation_result(validate, expected, "Update object validation");

    ns.delete(&pool).await.expect("Failed to delete namespace");
}
