use std::env;

/// Dynamic storage & data-plane runtime configuration governed by `wellmanifest/nohardcode`
/// (`NOHARDCODE-011`) and `wellmanifest/data-plane` (`low-ram-v1`).
#[derive(Debug, Clone)]
pub struct StorageRuntimeConfig {
    pub max_cacheable_yaml_bytes: u64,
    pub shard_size: usize,
    pub custom_shards: usize,
    pub sqlite_batch_size: usize,
    pub sqlite_cache_kb: i64,
    pub sqlite_busy_timeout_ms: u64,
    pub sqlite_commit_window_ms: u64,
    pub sqlite_max_batch: usize,
    pub lock_timeout_seconds: f64,
    pub max_inline_content_bytes: usize,
}

fn env_u64(key: &str, default: u64, min: u64, max: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|v| v.clamp(min, max))
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize, min: usize, max: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .map(|v| v.clamp(min, max))
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64, min: i64, max: i64) -> i64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .map(|v| v.clamp(min, max))
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64, min: f64, max: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .map(|v| v.clamp(min, max))
        .unwrap_or(default)
}

impl StorageRuntimeConfig {
    pub fn from_env() -> Self {
        Self {
            max_cacheable_yaml_bytes: env_u64(
                "PLANFILE_MAX_CACHEABLE_YAML_BYTES",
                5_000_000,
                65_536,
                268_435_456,
            ),
            shard_size: env_usize("PLANFILE_SHARD_SIZE", 100, 1, 10_000),
            custom_shards: env_usize("PLANFILE_CUSTOM_SHARDS", 16, 1, 256),
            sqlite_batch_size: env_usize("PLANFILE_SQLITE_BATCH_SIZE", 128, 1, 4_096),
            // wellmanifest/data-plane low-ram-v1: -2048 (~2 MiB page cache per SQLite DB)
            sqlite_cache_kb: env_i64("PLANFILE_SQLITE_CACHE_KB", -2048, -65_536, -256),
            sqlite_busy_timeout_ms: env_u64("PLANFILE_SQLITE_BUSY_TIMEOUT_MS", 5_000, 100, 120_000),
            // wellmanifest/data-gateway group-commit window (2 ms default)
            sqlite_commit_window_ms: env_u64("PLANFILE_SQLITE_COMMIT_WINDOW_MS", 2, 0, 1_000),
            sqlite_max_batch: env_usize("PLANFILE_SQLITE_MAX_BATCH", 64, 1, 1_024),
            lock_timeout_seconds: env_f64("PLANFILE_LOCK_TIMEOUT_SECONDS", 10.0, 0.1, 300.0),
            max_inline_content_bytes: env_usize(
                "PLANFILE_MAX_INLINE_CONTENT_BYTES",
                16_384,
                1_024,
                4_194_304,
            ),
        }
    }
}
