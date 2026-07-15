#![cfg(feature = "integration-test-support")]

pub use hubuum::*;

#[path = "support/mod.rs"]
pub mod tests;

#[path = "api/v1/classes.rs"]
mod api_classes;
#[path = "api/v1/collections.rs"]
mod api_collections;
#[path = "api/v1/computed_fields.rs"]
mod api_computed_fields;
#[path = "api/v1/objects.rs"]
mod api_objects;
#[path = "api/v1/querying.rs"]
mod api_querying;
#[path = "api/v1/relations.rs"]
mod api_relations;
#[path = "api/v1/search.rs"]
mod api_search;
