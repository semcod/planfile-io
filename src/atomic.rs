use std::fs;
use std::io::{self, Write};
use std::path::Path;

pub fn atomic_write_bytes(target: &Path, content: &[u8]) -> io::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::Builder::new()
        .prefix(&format!(
            ".{}.",
            target.file_name().unwrap_or_default().to_string_lossy()
        ))
        .suffix(".tmp")
        .tempfile_in(parent)?;

    temp.write_all(content)?;
    temp.as_file().sync_all()?;
    temp.persist(target).map_err(|e| e.error)?;
    Ok(())
}

pub fn atomic_write_str(target: &Path, text: &str) -> io::Result<()> {
    atomic_write_bytes(target, text.as_bytes())
}
