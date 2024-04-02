use serde_json;

use serde_json::Value;
use std::collections::HashMap;

// Define an enum with variants for each schema
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SchemaType {
    Geo,
    Blog,
    Address,
}

// Use lazy_static to hold your parsed schemas
lazy_static! {
    static ref SCHEMAS: HashMap<SchemaType, Value> = {
        let mut m = HashMap::new();
        m.insert(
            SchemaType::Geo,
            serde_json::from_str(GEO_SCHEMA_STR).expect("Failed to parse GEO_SCHEMA"),
        );
        m.insert(
            SchemaType::Blog,
            serde_json::from_str(BLOG_SCHEMA_STR).expect("Failed to parse OTHER_SCHEMA1"),
        );
        m.insert(
            SchemaType::Address,
            serde_json::from_str(ADDRESS_SCHEMA_STR).expect("Failed to parse OTHER_SCHEMA2"),
        );
        m
    };
}

// Function to get a schema by type
pub fn get_schema(schema_type: SchemaType) -> &'static Value {
    SCHEMAS
        .get(&schema_type)
        .expect("get_schema asked to find non-existent schema")
}

const BLOG_SCHEMA_STR: &str = r#"{
    "$id": "https://example.com/blog-post.schema.json",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "A representation of a blog post",
    "type": "object",
    "required": ["title", "content", "author"],
    "properties": {
        "title": {
        "type": "string"
        },
        "content": {
        "type": "string"
        },
        "publishedDate": {
        "type": "string",
        "format": "date-time"
        },
        "author": {
        "$ref": "https://example.com/user-profile.schema.json"
        },
        "tags": {
        "type": "array",
        "items": {
            "type": "string"
        }
        }
    }
}"#;

const ADDRESS_SCHEMA_STR: &str = r#"{
    "$id": "https://example.com/address.schema.json",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "An address similar to http://microformats.org/wiki/h-card",
    "type": "object",
    "properties": {
        "postOfficeBox": {
        "type": "string"
        },
        "extendedAddress": {
        "type": "string"
        },
        "streetAddress": {
        "type": "string"
        },
        "locality": {
        "type": "string"
        },
        "region": {
        "type": "string"
        },
        "postalCode": {
        "type": "string"
        },
        "countryName": {
        "type": "string"
        }
    },
    "required": [ "locality", "region", "countryName" ],
    "dependentRequired": {
        "postOfficeBox": [ "streetAddress" ],
        "extendedAddress": [ "streetAddress" ]
    }
}"#;

const GEO_SCHEMA_STR: &str = r#"{
    "$id": "https://example.com/geographical-location.schema.json",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Geographical Location",
    "description": "A geographical location",
    "required": [ "latitude", "longitude" ],
    "type": "object",
    "properties": {
        "latitude": {
        "type": "number",
        "minimum": -90,
        "maximum": 90
        },
        "longitude": {
        "type": "number",
        "minimum": -180,
        "maximum": 180
        }
    },
    "required": [ "latitude", "longitude" ]
}"#;
