//! Decode a bounded profile without allowing an object member to overwrite an
//! earlier observation. Keep scalar decoding in serde_json for exact integers.
use mediatype::{
    MediaType,
    names::{APPLICATION, JSON},
};
use oauth2::http::{HeaderMap, header::CONTENT_TYPE};
use serde::de::{Error, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value, value::RawValue};

use super::ProtocolError;

/// Require one JSON media type, including when the provider sends parameters.
pub fn has_json_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let content_type = values
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| MediaType::parse(value.trim()).ok());
    values.next().is_none()
        && content_type.is_some_and(|mime| mime.essence() == MediaType::new(APPLICATION, JSON))
}

pub fn parse(bytes: &[u8]) -> Result<Value, ProtocolError> {
    let raw: &RawValue =
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidResponse)?;
    decode(raw, 0)
}

fn decode(raw: &RawValue, depth: usize) -> Result<Value, ProtocolError> {
    if depth >= 128 {
        return Err(ProtocolError::InvalidResponse);
    }
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'{') => serde_json::Deserializer::from_str(text)
            .deserialize_map(Object(depth + 1))
            .map_err(|_| ProtocolError::InvalidResponse),
        Some(b'[') => {
            let items =
                Vec::<&RawValue>::deserialize(&mut serde_json::Deserializer::from_str(text))
                    .map_err(|_| ProtocolError::InvalidResponse)?;
            items
                .into_iter()
                .map(|value| decode(value, depth + 1))
                .collect::<Result<_, _>>()
                .map(Value::Array)
        }
        _ => serde_json::from_str(text).map_err(|_| ProtocolError::InvalidResponse),
    }
}

struct Object(usize);

impl<'de> Visitor<'de> for Object {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an object with unique member names")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Value, A::Error> {
        let mut object = Map::new();
        while let Some(key) = input.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(A::Error::custom("duplicate profile member"));
            }
            let raw = input.next_value::<&RawValue>()?;
            let value =
                decode(raw, self.0).map_err(|_| A::Error::custom("invalid profile member"))?;
            object.insert(key, value);
        }
        Ok(Value::Object(object))
    }
}

/// Link targets can contain commas and parameters can be quoted. Inspect
/// relation names without resolving or following any of the supplied targets.
pub fn paginated(headers: &oauth2::http::HeaderMap) -> Result<bool, ProtocolError> {
    for header in headers.get_all(oauth2::http::header::LINK) {
        let mut links = header
            .to_str()
            .map_err(|_| ProtocolError::InvalidResponse)?
            .trim();
        while !links.is_empty() {
            let (_, parameters) = links
                .strip_prefix('<')
                .and_then(|s| s.split_once('>'))
                .ok_or(ProtocolError::InvalidResponse)?;
            links = parameters.trim_start();
            while let Some(parameter) = links.strip_prefix(';') {
                let parameter = parameter.trim_start();
                let end = parameter
                    .find(['=', ';', ',', ' ', '\t'])
                    .unwrap_or(parameter.len());
                let name = &parameter[..end];
                if name.is_empty() {
                    return Err(ProtocolError::InvalidResponse);
                }
                links = parameter[end..].trim_start();
                let Some(value) = links.strip_prefix('=').map(str::trim_start) else {
                    if name.eq_ignore_ascii_case("rel") {
                        return Err(ProtocolError::InvalidResponse);
                    }
                    continue;
                };
                let mut quoted = String::new();
                let word = if let Some(rest) = value.strip_prefix('"') {
                    let mut chars = rest.chars();
                    loop {
                        match chars.next().ok_or(ProtocolError::InvalidResponse)? {
                            '"' => break,
                            '\\' => {
                                quoted.push(chars.next().ok_or(ProtocolError::InvalidResponse)?)
                            }
                            c => quoted.push(c),
                        }
                    }
                    links = chars.as_str().trim_start();
                    quoted.as_str()
                } else {
                    let end = value.find([';', ',', ' ', '\t']).unwrap_or(value.len());
                    links = value[end..].trim_start();
                    &value[..end]
                };
                if name.eq_ignore_ascii_case("rel")
                    && word.split_ascii_whitespace().any(|rel| {
                        ["next", "prev", "previous", "first", "last"]
                            .iter()
                            .any(|page| rel.eq_ignore_ascii_case(page))
                    })
                {
                    return Ok(true);
                }
            }
            if !links.is_empty() {
                links = links
                    .strip_prefix(',')
                    .ok_or(ProtocolError::InvalidResponse)?
                    .trim_start();
            }
        }
    }
    Ok(false)
}
