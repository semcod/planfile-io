use sha2::{Digest, Sha256};
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
        // Extract numeric suffix if present
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

        // Custom non-numeric ID -> SHA-256 bucket
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
