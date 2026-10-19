use crate::yaml_io::{read_yaml_fast, stat_size_and_mtime_ns};
use serde_json::{Map, Value as JsonValue};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

pub struct ShardRouter {
    pub shard_size: usize,
    pub custom_shards: usize,
}

impl ShardRouter {
    pub fn new(shard_size: usize, custom_shards: usize) -> Self {
        Self {
            shard_size: shard_size.max(1),
            custom_shards: custom_shards.clamp(1, 256),
        }
    }

    pub fn shard_name(&self, ticket_id: &str) -> String {
        let num_str: String = ticket_id
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect();

        if !num_str.is_empty() {
            let num: usize = num_str.chars().rev().collect::<String>().parse().unwrap_or(0);
            let start = (num / self.shard_size) * self.shard_size;
            let end = start + self.shard_size - 1;
            let width = 6.max(format!("{}", end).len());
            return format!("tickets-{:0width$}-{:0width$}.yaml", start, end, width = width);
        }

        let mut hasher = Sha256::new();
        hasher.update(ticket_id.as_bytes());
        let digest = hasher.finalize();
        let bucket =
            u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) as usize
                % self.custom_shards;
        format!("tickets-custom-{:02x}.yaml", bucket)
    }

    pub fn shard_path(&self, sprints_dir: &Path, sprint: &str, ticket_id: &str) -> PathBuf {
        sprints_dir
            .join(format!("{}.shards", sprint))
            .join(self.shard_name(ticket_id))
    }
}

fn extract_tickets_map(val: &JsonValue) -> Option<&Map<String, JsonValue>> {
    let obj = val.as_object()?;
    if let Some(t) = obj.get("tickets").and_then(|v| v.as_object()) {
        return Some(t);
    }
    obj.get("sprint")
        .and_then(|s| s.as_object())
        .and_then(|s| s.get("tickets"))
        .and_then(|t| t.as_object())
}

/// Load and merge `metadata.yaml` + all `tickets-*.yaml` shards for a sprint in native Rust.
pub fn load_sharded_sprint(sprints_dir: &Path, sprint: &str) -> Result<JsonValue, String> {
    let sprint_dir = sprints_dir.join(format!("{sprint}.shards"));
    let metadata_path = sprint_dir.join("metadata.yaml");
    let metadata_data = if metadata_path.exists() {
        read_yaml_fast(&metadata_path).unwrap_or_else(|_| JsonValue::Object(Map::new()))
    } else {
        JsonValue::Object(Map::new())
    };

    let mut sprint_meta = metadata_data
        .as_object()
        .and_then(|o| o.get("sprint"))
        .and_then(|s| s.as_object())
        .cloned()
        .or_else(|| metadata_data.as_object().cloned())
        .unwrap_or_default();

    let mut merged_tickets = Map::new();
    if sprint_dir.exists() {
        let mut shard_paths: Vec<PathBuf> = fs::read_dir(&sprint_dir)
            .map_err(|e| e.to_string())?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| {
                let name = p.file_name().unwrap_or_default().to_string_lossy();
                name.starts_with("tickets-") && name.ends_with(".yaml")
            })
            .collect();
        shard_paths.sort();

        for shard_path in shard_paths {
            if let Ok(shard_val) = read_yaml_fast(&shard_path) {
                if let Some(tickets) = extract_tickets_map(&shard_val) {
                    for (tid, tval) in tickets {
                        merged_tickets.insert(tid.clone(), tval.clone());
                    }
                }
            }
        }
    }

    sprint_meta
        .entry("id".to_string())
        .or_insert_with(|| JsonValue::String(sprint.to_string()));
    sprint_meta
        .entry("status".to_string())
        .or_insert_with(|| JsonValue::String("active".to_string()));
    sprint_meta.insert("tickets".to_string(), JsonValue::Object(merged_tickets));

    let mut envelope = if metadata_data
        .as_object()
        .and_then(|o| o.get("sprint"))
        .is_some_and(|s| s.is_object())
    {
        metadata_data.as_object().cloned().unwrap_or_default()
    } else {
        Map::new()
    };
    envelope.insert("sprint".to_string(), JsonValue::Object(sprint_meta));
    Ok(JsonValue::Object(envelope))
}

/// Batch stat paths in Rust to compute source signatures `(path, size, mtime_ns)`.
pub fn compute_source_signature(paths: &[String]) -> Vec<(String, u64, u64)> {
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        let path = Path::new(p);
        if let Some((size, mtime_ns)) = stat_size_and_mtime_ns(path) {
            out.push((p.clone(), size, mtime_ns));
        }
    }
    out
}
