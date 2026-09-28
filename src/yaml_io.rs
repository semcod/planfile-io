use crate::atomic::atomic_write_str;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub const MIRROR_SUFFIX: &str = ".fast.json";

pub fn mirror_path(yaml_path: &Path) -> PathBuf {
    let mut name = yaml_path.file_name().unwrap_or_default().to_os_string();
    name.push(MIRROR_SUFFIX);
    yaml_path.with_file_name(name)
}

pub fn stat_mtime_ns(path: &Path) -> Option<u128> {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
}

/// Convert YAML AST to JSON AST (preserving strings, numbers, booleans, maps, arrays)
pub fn yaml_to_json_val(val: YamlValue) -> JsonValue {
    match val {
        YamlValue::Null => JsonValue::Null,
        YamlValue::Bool(b) => JsonValue::Bool(b),
        YamlValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                JsonValue::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                JsonValue::Number(u.into())
            } else if let Some(f) = n.as_f64() {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            } else {
                JsonValue::Null
            }
        }
        YamlValue::String(s) => JsonValue::String(s),
        YamlValue::Sequence(seq) => {
            JsonValue::Array(seq.into_iter().map(yaml_to_json_val).collect())
        }
        YamlValue::Mapping(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                let key_str = match k {
                    YamlValue::String(s) => s,
                    other => match serde_json::to_string(&yaml_to_json_val(other)) {
                        Ok(s) => s.trim_matches('"').to_string(),
                        Err(_) => String::new(),
                    },
                };
                obj.insert(key_str, yaml_to_json_val(v));
            }
            JsonValue::Object(obj)
        }
        YamlValue::Tagged(tagged) => yaml_to_json_val(tagged.value),
    }
}

pub fn read_yaml_fast(yaml_path: &Path) -> Result<JsonValue, String> {
    let mirror = mirror_path(yaml_path);
    let current_mtime = stat_mtime_ns(yaml_path).unwrap_or(0);

    // 1. Sprawdź mirror .fast.json
    if mirror.exists() {
        if let Ok(content) = fs::read_to_string(&mirror) {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(&content) {
                if let Some(cached_mtime) = parsed.get("mtime_ns").and_then(|v| v.as_u64()) {
                    if (cached_mtime as u128) == current_mtime {
                        if let Some(data) = parsed.get("data") {
                            return Ok(data.clone());
                        }
                    }
                }
            }
        }
    }

    // 2. Cache miss: Parsuj YAML
    let yaml_raw = fs::read_to_string(yaml_path)
        .map_err(|e| format!("Failed to read YAML {}: {}", yaml_path.display(), e))?;

    let yaml_val: YamlValue = serde_yaml::from_str(&yaml_raw)
        .map_err(|e| format!("YAML syntax error in {}: {}", yaml_path.display(), e))?;

    let json_val = yaml_to_json_val(yaml_val);

    // 3. Zapisz mirror .fast.json (best-effort)
    let mirror_envelope = serde_json::json!({
        "version": 1,
        "mtime_ns": current_mtime as u64,
        "data": &json_val
    });
    if let Ok(serialized) = serde_json::to_string(&mirror_envelope) {
        let _ = atomic_write_str(&mirror, &serialized);
    }

    Ok(json_val)
}
