//! Digests for durable task requests and materialized object data.

use std::fmt::Write as _;
use std::io::{self, Write};

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Hash compact JSON with lexicographically sorted object keys, preserving
/// array order and serde_json's string escapes and number representation.
/// The digest is lowercase hexadecimal SHA-256.
pub fn canonical_json_sha256(data: &Value) -> serde_json::Result<String> {
    let mut hasher = DigestWriter(Sha256::new());
    {
        // Escaping can emit many tiny fragments. Batch them in a fixed-size
        // buffer without retaining the complete canonical document.
        let mut writer = io::BufWriter::with_capacity(8 * 1024, &mut hasher);
        canonical_json(data, &mut writer)?;
        writer.flush().map_err(serde_json::Error::io)?;
    }
    let mut digest = String::with_capacity(64);
    for byte in hasher.0.finalize() {
        write!(digest, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(digest)
}

struct DigestWriter(Sha256);

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Write exactly the original representation: sorted object keys, compact JSON,
// and serde_json's escaping and number tokens. Do not normalize numbers here.
fn canonical_json(value: &Value, output: &mut impl Write) -> Result<(), serde_json::Error> {
    match value {
        Value::Object(values) => {
            output.write_all(b"{").map_err(serde_json::Error::io)?;
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.write_all(b",").map_err(serde_json::Error::io)?;
                }
                serde_json::to_writer(&mut *output, key)?;
                output.write_all(b":").map_err(serde_json::Error::io)?;
                canonical_json(&values[key], output)?;
            }
            output.write_all(b"}").map_err(serde_json::Error::io)?;
        }
        Value::Array(values) => {
            output.write_all(b"[").map_err(serde_json::Error::io)?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.write_all(b",").map_err(serde_json::Error::io)?;
                }
                canonical_json(value, output)?;
            }
            output.write_all(b"]").map_err(serde_json::Error::io)?;
        }
        _ => serde_json::to_writer(output, value)?,
    }
    Ok(())
}
