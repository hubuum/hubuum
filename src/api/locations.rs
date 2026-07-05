use crate::api::response::ResponseLocation;
use crate::errors::ApiError;

fn api_v1(path: String) -> Result<ResponseLocation, ApiError> {
    ResponseLocation::new(format!("/api/v1/{path}"))
}

pub fn class(class_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("classes/{class_id}"))
}

pub fn class_relation(class_id: i32, relation_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("classes/{class_id}/relations/{relation_id}"))
}

pub fn class_object(class_id: i32, object_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("classes/{class_id}/{object_id}"))
}

pub fn group(group_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("iam/groups/{group_id}"))
}

pub fn collection(collection_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("collections/{collection_id}"))
}

pub fn object_relation(
    from_class_id: i32,
    from_object_id: i32,
    to_class_id: i32,
    to_object_id: i32,
) -> Result<ResponseLocation, ApiError> {
    api_v1(format!(
        "classes/{from_class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}"
    ))
}

pub fn remote_target(target_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("remote-targets/{target_id}"))
}

pub fn service_account(service_account_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("iam/service-accounts/{service_account_id}"))
}

pub fn template(template_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("templates/{template_id}"))
}

pub fn task(task_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("tasks/{task_id}"))
}

pub fn user(user_id: i32) -> Result<ResponseLocation, ApiError> {
    api_v1(format!("iam/users/{user_id}"))
}
