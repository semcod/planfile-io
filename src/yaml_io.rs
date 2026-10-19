use crate::atomic::atomic_write_str;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};
use std::time::UNIX_EPOCH;

pub const MIRROR_SUFFIX: &str = ".fast.json";
pub const MIRROR_VERSION: u64 = 1;
const MAX_MEM_CACHE_ENTRIES: usize = 2048;

fn mem_yaml_cache() -> &'static RwLock<HashMap<String, (u128, JsonValue)>> {
    static CACHE: OnceLock<RwLock<HashMap<String, (u128, JsonValue)>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

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

pub fn stat_size_and_mtime_ns(path: &Path) -> Option<(u64, u64)> {
    let meta = fs::metadata(path).ok()?;
    let mtime = meta
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_nanos() as u64;
    Some((meta.len(), mtime))
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

fn extract_mirror_mtime(parsed: &JsonValue) -> Option<u128> {
    if parsed.get("version").and_then(|v| v.as_u64()) != Some(MIRROR_VERSION) {
        return None;
    }
    parsed
        .get("yaml_mtime_ns")
        .or_else(|| parsed.get("mtime_ns"))
        .and_then(|v| v.as_u64())
        .map(|v| v as u128)
}

pub fn write_mirror_envelope(mirror: &Path, mtime_ns: u64, data: &JsonValue) -> Result<(), String> {
    let mirror_envelope = serde_json::json!({
        "version": MIRROR_VERSION,
        "yaml_mtime_ns": mtime_ns,
        "mtime_ns": mtime_ns,
        "data": data
    });
    let serialized = serde_json::to_string(&mirror_envelope).map_err(|e| e.to_string())?;
    atomic_write_str(mirror, &serialized).map_err(|e| e.to_string())
}

pub fn read_yaml_fast(yaml_path: &Path) -> Result<JsonValue, String> {
    let Some(current_mtime) = stat_mtime_ns(yaml_path) else {
        return Err(format!("Failed to stat YAML {}", yaml_path.display()));
    };
    let key = yaml_path.to_string_lossy().into_owned();

    // 0. Check in-process memory cache keyed by exact (path, mtime_ns)
    if let Ok(guard) = mem_yaml_cache().read() {
        if let Some((cached_mtime, cached_val)) = guard.get(&key) {
            if *cached_mtime == current_mtime {
                return Ok(cached_val.clone());
            }
        }
    }

    let mirror = mirror_path(yaml_path);

    // 1. Check .fast.json mirror (supports both `yaml_mtime_ns` and `mtime_ns`)
    if mirror.exists() {
        if let Ok(content) = fs::read_to_string(&mirror) {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(&content) {
                if extract_mirror_mtime(&parsed) == Some(current_mtime) {
                    if let Some(data) = parsed.get("data") {
                        let out = data.clone();
                        if content.len() <= 1_048_576 {
                            if let Ok(mut wguard) = mem_yaml_cache().write() {
                                if wguard.len() >= MAX_MEM_CACHE_ENTRIES {
                                    wguard.clear();
                                }
                                wguard.insert(key, (current_mtime, out.clone()));
                            }
                        }
                        return Ok(out);
                    }
                }
            }
        }
    }

    // 2. Cache miss: Parse YAML
    let yaml_raw = fs::read_to_string(yaml_path)
        .map_err(|e| format!("Failed to read YAML {}: {}", yaml_path.display(), e))?;

    let yaml_val: YamlValue = serde_yaml::from_str(&yaml_raw)
        .map_err(|e| format!("YAML syntax error in {}: {}", yaml_path.display(), e))?;

    let json_val = match yaml_to_json_val(yaml_val) {
        JsonValue::Null => JsonValue::Object(serde_json::Map::new()),
        other => other,
    };

    // 3. TOCTOU guard: re-stat immediately after reading before caching mirror
    if stat_mtime_ns(yaml_path) == Some(current_mtime) {
        if key.contains("/.planfile/") || key.starts_with("/tmp/") {
            let _ = write_mirror_envelope(&mirror, current_mtime as u64, &json_val);
        }
        if yaml_raw.len() <= 1_048_576 {
            if let Ok(mut wguard) = mem_yaml_cache().write() {
                if wguard.len() >= MAX_MEM_CACHE_ENTRIES {
                    wguard.clear();
                }
                wguard.insert(key, (current_mtime, json_val.clone()));
            }
        }
    }

    Ok(json_val)
}

/// Audit on-disk `.fast.json` mirror against authoritative YAML and self-heal on drift.
pub fn audit_mirror_fast(yaml_path: &Path) -> (bool, bool, Option<String>) {
    if !yaml_path.exists() {
        return (true, false, None);
    }
    let yaml_raw = match fs::read_to_string(yaml_path) {
        Ok(r) => r,
        Err(e) => return (false, false, Some(format!("YAML unreadable: {e}"))),
    };
    let yaml_val: YamlValue = match serde_yaml::from_str(&yaml_raw) {
        Ok(v) => v,
        Err(e) => return (false, false, Some(format!("YAML unparsable: {e}"))),
    };
    let fresh = match yaml_to_json_val(yaml_val) {
        JsonValue::Null => JsonValue::Object(serde_json::Map::new()),
        other => other,
    };
    let mirror = mirror_path(yaml_path);
    if !mirror.exists() {
        return (true, false, None);
    }
    let mut reason: Option<String> = None;
    match fs::read_to_string(&mirror)
        .ok()
        .and_then(|s| serde_json::from_str::<JsonValue>(&s).ok())
    {
        Some(parsed) => {
            if parsed.get("data") != Some(&fresh) {
                reason = Some("cached mirror disagrees with a fresh YAML parse".to_string());
            }
        }
        None => {
            reason = Some("mirror unreadable or invalid JSON".to_string());
        }
    }
    if let Some(r) = reason {
        if let Some(mtime) = stat_mtime_ns(yaml_path) {
            let healed = write_mirror_envelope(&mirror, mtime as u64, &fresh).is_ok();
            if let Ok(mut wguard) = mem_yaml_cache().write() {
                wguard.insert(yaml_path.to_string_lossy().into_owned(), (mtime, fresh));
            }
            return (false, healed, Some(r));
        }
        return (false, false, Some(r));
    }
    (true, false, None)
}
