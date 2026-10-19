use serde_json::{Map, Value as JsonValue};
use sha2::{Digest, Sha256};

pub const MAX_LINE_BYTES: usize = 65_536;
pub const MAX_TYPE_CHARS: usize = 128;
pub const MAX_TARGET_CHARS: usize = 2_048;
pub const MAX_DEPTH: usize = 32;
pub const MAX_SAFE_INT: i64 = (1_i64 << 53) - 1;

fn is_valid_type_segment(seg: &str) -> bool {
    let mut chars = seg.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

pub fn is_valid_type(kind: &str) -> bool {
    if kind.is_empty() || kind.len() > MAX_TYPE_CHARS {
        return false;
    }
    kind.split('.').all(is_valid_type_segment)
}

pub fn is_valid_target(target: &str) -> bool {
    if target.is_empty() || target.len() > MAX_TARGET_CHARS {
        return false;
    }
    let Some((scheme, rest)) = target.split_once(':') else {
        return false;
    };
    if scheme.is_empty() || rest.is_empty() {
        return false;
    }
    let mut scheme_chars = scheme.chars();
    match scheme_chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    if !scheme_chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')) {
        return false;
    }
    rest.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                '-' | '.' | '_' | '~' | ':' | '/' | '?' | '#' | '[' | ']' | '@' | '!' | '$' | '&'
                    | '\'' | '(' | ')' | '*' | '+' | ',' | ';' | '=' | '%'
            )
    })
}

fn validate_canonical_value(val: &JsonValue, depth: usize) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("JSON nesting exceeds bound".to_string());
    }
    match val {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) => Ok(()),
        JsonValue::Number(n) => {
            if n.is_f64() && !n.is_i64() && !n.is_u64() {
                return Err("Float is outside canonical profile".to_string());
            }
            if let Some(i) = n.as_i64() {
                if i.unsigned_abs() > (MAX_SAFE_INT as u64) {
                    return Err("Large integers must use decimal strings".to_string());
                }
            } else if let Some(u) = n.as_u64() {
                if u > (MAX_SAFE_INT as u64) {
                    return Err("Large integers must use decimal strings".to_string());
                }
            }
            Ok(())
        }
        JsonValue::Array(arr) => {
            for item in arr {
                validate_canonical_value(item, depth + 1)?;
            }
            Ok(())
        }
        JsonValue::Object(map) => {
            for v in map.values() {
                validate_canonical_value(v, depth + 1)?;
            }
            Ok(())
        }
    }
}

/// Produce `wellmanifest-canonical-json-v1` UTF-8 bytes (sorted keys, compact separators, no floats).
pub fn canonical_json_bytes(val: &JsonValue) -> Result<Vec<u8>, String> {
    validate_canonical_value(val, 1)?;
    // serde_json::Map without preserve_order is a BTreeMap (lexicographically sorted keys)
    serde_json::to_vec(val).map_err(|e| e.to_string())
}

pub fn canonical_digest(val: &JsonValue) -> Result<String, String> {
    let bytes = canonical_json_bytes(val)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn compute_event_hash(event: &JsonValue) -> Result<String, String> {
    let Some(obj) = event.as_object() else {
        return Err("Event must be a JSON object".to_string());
    };
    let mut filtered = Map::new();
    for (k, v) in obj {
        if k != "eventHash" {
            filtered.insert(k.clone(), v.clone());
        }
    }
    canonical_digest(&JsonValue::Object(filtered))
}

pub fn format_gateway_line(kind: &str, target: &str, vars: &JsonValue) -> Result<String, String> {
    if !is_valid_type(kind) {
        return Err("Invalid type".to_string());
    }
    if !is_valid_target(target) {
        return Err("Invalid target".to_string());
    }
    let Some(obj) = vars.as_object() else {
        return Err("Variables must be one JSON object".to_string());
    };
    validate_canonical_value(vars, 1)?;
    let line = if obj.is_empty() {
        format!("{kind}: {target}")
    } else {
        let body = String::from_utf8(canonical_json_bytes(vars)?).map_err(|e| e.to_string())?;
        format!("{kind}: {target} {body}")
    };
    if line.len() > MAX_LINE_BYTES {
        return Err("Line exceeds byte bound".to_string());
    }
    Ok(line)
}

pub fn parse_gateway_line(
    text: &str,
    strict: bool,
) -> Result<(String, String, JsonValue, String), String> {
    let trimmed = text
        .strip_suffix("\r\n")
        .or_else(|| text.strip_suffix('\n'))
        .unwrap_or(text);
    if trimmed.len() > MAX_LINE_BYTES {
        return Err("Line exceeds byte bound".to_string());
    }
    if trimmed.contains('\n') || trimmed.contains('\r') {
        return Err("A line cannot contain a line break".to_string());
    }
    if trimmed.starts_with('\u{feff}') {
        return Err("Byte order mark is not allowed".to_string());
    }
    let Some((head, rest)) = trimmed.split_once(": ") else {
        return Err("Expected `type: target`".to_string());
    };
    if !is_valid_type(head) {
        return Err("Expected `type: target`".to_string());
    }
    let (target, vars) = match rest.split_once(' ') {
        Some((t, payload)) => {
            if !payload.starts_with('{') {
                return Err("Variables must be one JSON object".to_string());
            }
            let parsed: JsonValue = serde_json::from_str(payload)
                .map_err(|_| "Variables are not valid JSON".to_string())?;
            if !parsed.is_object() {
                return Err("Variables must be one JSON object".to_string());
            }
            (t, parsed)
        }
        None => (rest, JsonValue::Object(Map::new())),
    };
    if !is_valid_target(target) {
        return Err("Target must be an absolute URI or URN".to_string());
    }
    let canonical_line = format_gateway_line(head, target, &vars)?;
    if strict && canonical_line != trimmed {
        return Err("Line is not canonical".to_string());
    }
    let mut hasher = Sha256::new();
    hasher.update(canonical_line.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());
    Ok((head.to_string(), target.to_string(), vars, sha256))
}
