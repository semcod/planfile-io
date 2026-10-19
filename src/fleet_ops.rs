use crate::yaml_io::{read_yaml_fast, stat_mtime_ns};
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{OnceLock, RwLock};

fn hist_done_cache() -> &'static RwLock<HashMap<String, (u128, Vec<String>)>> {
    static CACHE: OnceLock<RwLock<HashMap<String, (u128, Vec<String>)>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn linked_worktree(project: &Path) -> bool {
    let marker = project.join(".git");
    if !marker.is_file() {
        return false;
    }
    let Ok(raw) = fs::read_to_string(&marker) else {
        return false;
    };
    let Some(rest) = raw.trim().strip_prefix("gitdir:") else {
        return false;
    };
    let dir = Path::new(rest.trim());
    let resolved = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        project.join(dir)
    };
    resolved.join("commondir").is_file()
}

pub fn resolve_git_toplevel(project: &Path) -> Result<Option<PathBuf>, String> {
    for parent in project.ancestors() {
        let marker = parent.join(".git");
        if !marker.exists() {
            continue;
        }
        if marker.is_dir() {
            if !marker.join("HEAD").is_file() {
                return Err(format!(
                    "Cannot resolve inventory project identity: {}",
                    project.display()
                ));
            }
            return parent
                .canonicalize()
                .map(Some)
                .map_err(|e| e.to_string());
        }
        if marker.is_file() {
            let value = fs::read_to_string(&marker).map_err(|_| {
                format!(
                    "Cannot resolve inventory project identity: {}",
                    project.display()
                )
            })?;
            let value = value.trim();
            let Some(rest) = value.strip_prefix("gitdir:") else {
                return Err(format!(
                    "Cannot resolve inventory project identity: {}",
                    project.display()
                ));
            };
            let gitdir_rel = Path::new(rest.trim());
            let gitdir = if gitdir_rel.is_absolute() {
                gitdir_rel.to_path_buf()
            } else {
                parent.join(gitdir_rel)
            };
            if !gitdir.join("HEAD").is_file() {
                return Err(format!(
                    "Cannot resolve inventory project identity: {}",
                    project.display()
                ));
            }
            return parent
                .canonicalize()
                .map(Some)
                .map_err(|e| e.to_string());
        }
        return Err(format!(
            "Cannot resolve inventory project identity: {}",
            project.display()
        ));
    }
    Ok(None)
}

pub fn declared_project(project: &Path) -> Result<bool, String> {
    let primary = resolve_git_toplevel(project)?;
    let Some(primary) = primary else {
        return Ok(true);
    };
    if linked_worktree(&primary) {
        return Ok(false);
    }
    let resolved_project = project.canonicalize().map_err(|e| e.to_string())?;
    if primary == resolved_project {
        return Ok(true);
    }
    let manifest_file = primary.join(".governance/manifest.json");
    let Ok(raw) = fs::read_to_string(&manifest_file) else {
        return Ok(false);
    };
    let Ok(manifest) = serde_json::from_str::<JsonValue>(&raw) else {
        return Ok(false);
    };
    let repo = &manifest["repository"];
    if repo["mode"].as_str() != Some("monorepo") {
        return Ok(false);
    }
    let Some(components) = repo["componentRoots"].as_array() else {
        return Ok(false);
    };
    for comp in components {
        if let Some(comp_str) = comp.as_str() {
            if let Ok(comp_resolved) = primary.join(comp_str).canonicalize() {
                if comp_resolved.starts_with(&primary) && comp_resolved == resolved_project {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

pub fn discover_project_candidates(base: &Path) -> Result<Vec<String>, String> {
    let base_canon = base.canonicalize().map_err(|e| e.to_string())?;
    if linked_worktree(&base_canon) {
        return Ok(Vec::new());
    }
    let mut raw_candidates: Vec<PathBuf> = vec![base_canon.clone()];
    if let Ok(children) = fs::read_dir(&base_canon) {
        for child_entry in children.flatten() {
            let child_path = child_entry.path();
            let Some(name) = child_path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.starts_with('.') || !child_path.is_dir() {
                continue;
            }
            let Ok(child_canon) = child_path.canonicalize() else {
                continue;
            };
            if !child_canon.starts_with(&base_canon) || linked_worktree(&child_canon) {
                continue;
            }
            raw_candidates.push(child_canon.clone());
            if let Ok(subs) = fs::read_dir(&child_path) {
                for sub_entry in subs.flatten() {
                    let sub_path = sub_entry.path();
                    let Some(sub_name) = sub_path.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    if sub_name.starts_with('.') || !sub_path.is_dir() {
                        continue;
                    }
                    raw_candidates.push(sub_path);
                }
            }
        }
    }

    let mut valid_set: BTreeSet<String> = BTreeSet::new();
    for cand in raw_candidates {
        if !cand.join(".planfile/sprints/current.yaml").is_file() {
            continue;
        }
        let Ok(resolved) = cand.canonicalize() else {
            continue;
        };
        if !resolved.starts_with(&base_canon)
            || linked_worktree(&resolved)
            || resolved
                .components()
                .any(|c| c.as_os_str() == ".worktrees")
        {
            continue;
        }
        if declared_project(&resolved)? {
            valid_set.insert(resolved.to_string_lossy().into_owned());
        }
    }
    Ok(valid_set.into_iter().collect())
}

pub fn ticket_is_done_json(ticket: &JsonValue) -> bool {
    let Some(obj) = ticket.as_object() else {
        return false;
    };
    if let Some(status) = obj.get("status").and_then(|v| v.as_str()) {
        if matches!(
            status,
            "closed" | "done" | "completed" | "cancelled" | "canceled"
        ) {
            return true;
        }
    }
    if let Some(exec) = obj.get("execution").and_then(|v| v.as_object()) {
        if let Some(state) = exec.get("state").and_then(|v| v.as_str()) {
            if matches!(state, "done" | "completed" | "cancelled" | "canceled") {
                return true;
            }
        }
    }
    false
}

pub fn collect_sprint_done_fast(path: &Path) -> Vec<String> {
    let mut done_ids = Vec::new();
    let Ok(data) = read_yaml_fast(path) else {
        return done_ids;
    };
    let tickets_opt = data
        .get("sprint")
        .and_then(|s| s.get("tickets"))
        .and_then(|t| t.as_object())
        .or_else(|| data.get("tickets").and_then(|t| t.as_object()));
    if let Some(tickets) = tickets_opt {
        for (tid, t) in tickets {
            if ticket_is_done_json(t) {
                done_ids.push(tid.clone());
            }
        }
    }
    done_ids
}

pub fn collect_dir_historical_done_fast(sprint_dir: &Path) -> Vec<String> {
    let dir_mtime = stat_mtime_ns(sprint_dir).unwrap_or(0);
    let key = sprint_dir.to_string_lossy().into_owned();
    if dir_mtime > 0 {
        if let Ok(guard) = hist_done_cache().read() {
            if let Some((cached_mtime, cached_ids)) = guard.get(&key) {
                if *cached_mtime == dir_mtime {
                    return cached_ids.clone();
                }
            }
        }
    }
    let mut union_done: BTreeSet<String> = BTreeSet::new();
    let Ok(entries) = fs::read_dir(sprint_dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let fname = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            fname.ends_with(".yaml") && fname != "current.yaml"
        })
        .collect();
    paths.sort();
    for p in paths {
        for tid in collect_sprint_done_fast(&p) {
            union_done.insert(tid);
        }
    }
    let out: Vec<String> = union_done.into_iter().collect();
    if dir_mtime > 0 {
        if let Ok(mut wguard) = hist_done_cache().write() {
            if wguard.len() >= 2048 {
                wguard.clear();
            }
            wguard.insert(key, (dir_mtime, out.clone()));
        }
    }
    out
}

fn load_single_project_inventory(project_str: &str) -> JsonValue {
    let project = Path::new(project_str);
    let current = project.join(".planfile/sprints/current.yaml");
    match read_yaml_fast(&current) {
        Err(err) => json!({
            "project_root": project_str,
            "error": err,
        }),
        Ok(data) => {
            let sprint_dir = project.join(".planfile/sprints");
            let historical_done = collect_dir_historical_done_fast(&sprint_dir);
            json!({
                "project_root": project_str,
                "data": data,
                "historical_done": historical_done,
            })
        }
    }
}

pub fn batch_inventory_sprints(project_roots: &[String]) -> Vec<JsonValue> {
    if project_roots.is_empty() {
        return Vec::new();
    }
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(16)
        .min(project_roots.len());
    if num_threads <= 1 || project_roots.len() <= 2 {
        return project_roots
            .iter()
            .map(|p| load_single_project_inventory(p))
            .collect();
    }
    let chunk_size = (project_roots.len() + num_threads - 1) / num_threads;
    let mut results = Vec::with_capacity(project_roots.len());
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for chunk in project_roots.chunks(chunk_size) {
            handles.push(s.spawn(move || {
                chunk
                    .iter()
                    .map(|p| load_single_project_inventory(p))
                    .collect::<Vec<JsonValue>>()
            }));
        }
        for h in handles {
            if let Ok(part) = h.join() {
                results.extend(part);
            }
        }
    });
    results
}

fn resolve_git_dir_fast(project_root: &Path) -> Option<PathBuf> {
    let marker = project_root.join(".git");
    if marker.is_dir() {
        return Some(marker);
    }
    if marker.is_file() {
        let raw = fs::read_to_string(&marker).ok()?;
        let rest = raw.trim().strip_prefix("gitdir:")?.trim();
        let p = Path::new(rest);
        let resolved = if p.is_absolute() {
            p.to_path_buf()
        } else {
            project_root.join(p)
        };
        if resolved.is_dir() {
            return Some(resolved);
        }
    }
    None
}

/// Fast binary `.git/index` v2/v3 parser to check if `rel_path` is tracked without spawning `git ls-files`.
pub fn is_git_tracked_fast(project_root: &Path, rel_path: &str) -> Option<bool> {
    let git_dir = resolve_git_dir_fast(project_root)?;
    let index_path = git_dir.join("index");
    let bytes = fs::read(&index_path).ok()?;
    if bytes.len() < 12 || &bytes[0..4] != b"DIRC" {
        return None;
    }
    let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != 2 && version != 3 {
        return None;
    }
    let num_entries = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let target = rel_path.trim_start_matches("./").as_bytes();
    let mut offset = 12usize;
    for _ in 0..num_entries {
        if offset + 62 > bytes.len() {
            return None;
        }
        let flags = u16::from_be_bytes([bytes[offset + 60], bytes[offset + 61]]);
        let extended = (version == 3) && ((flags & 0x4000) != 0);
        let header_len = if extended { 64usize } else { 62usize };
        let name_start = offset + header_len;
        if name_start >= bytes.len() {
            return None;
        }
        let mut name_end = name_start;
        while name_end < bytes.len() && bytes[name_end] != 0 {
            name_end += 1;
        }
        if name_end >= bytes.len() {
            return None;
        }
        let entry_name = &bytes[name_start..name_end];
        if entry_name == target {
            return Some(true);
        }
        let entry_len = (header_len + (name_end - name_start) + 8) & !7;
        offset += entry_len;
    }
    Some(false)
}

fn extract_ticket_tokens_from_text(text: &str, out: &mut HashSet<String>) {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        let b = bytes[i];
        if b.is_ascii_alphabetic() {
            let start = i;
            i += 1;
            while i < len && bytes[i].is_ascii_alphanumeric() {
                i += 1;
            }
            if i < len && (bytes[i] == b'-' || bytes[i] == b'/') && (i - start) >= 2 {
                let prefix = &text[start..i];
                let sep_idx = i;
                i += 1;
                let num_start = i;
                while i < len && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i > num_start {
                    let prev_ok = start == 0
                        || (!bytes[start - 1].is_ascii_alphanumeric() && bytes[start - 1] != b'_');
                    let next_ok =
                        i == len || (!bytes[i].is_ascii_alphanumeric() && bytes[i] != b'_');
                    let is_ticket_prefix = prefix.eq_ignore_ascii_case("ticket")
                        || (bytes[sep_idx] == b'-'
                            && prefix.bytes().next().unwrap().is_ascii_alphabetic());
                    if prev_ok && next_ok && is_ticket_prefix {
                        let digits = &text[num_start..i];
                        let norm = format!("{}-{}", prefix, digits);
                        out.insert(norm.clone());
                        out.insert(norm.to_ascii_lowercase());
                        out.insert(norm.to_ascii_uppercase());
                        if prefix.eq_ignore_ascii_case("ticket") {
                            if let Ok(n) = digits.parse::<u64>() {
                                out.insert(format!("ticket-{:03}", n));
                                out.insert(format!("ticket-{}", n));
                            }
                        }
                    }
                }
            }
        } else {
            i += 1;
        }
    }
}

fn has_status_done_line(text: &str) -> bool {
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(pos) = lower.find("status") {
            let after = &lower[pos + 6..];
            let trimmed = after.trim_start_matches('*').trim_start();
            if let Some(rest) = trimmed.strip_prefix(':') {
                let val = rest.trim_start();
                if val.starts_with("done") {
                    let after_done = &val[4..];
                    if after_done.is_empty()
                        || !after_done
                            .bytes()
                            .next()
                            .unwrap()
                            .is_ascii_alphanumeric()
                    {
                        return true;
                    }
                }
            }
        }
    }
    false
}

pub fn collect_committed_ticket_ids_fast(project_root: &Path) -> Vec<String> {
    let mut repo_dirs: Vec<PathBuf> = Vec::new();
    if project_root.join(".git").exists() {
        repo_dirs.push(project_root.to_path_buf());
    }
    if let Ok(entries) = fs::read_dir(project_root) {
        let mut children: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
        children.sort();
        for child in children.into_iter().take(16) {
            let fname = child.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if child.is_dir() && !fname.starts_with('.') && child.join(".git").exists() {
                repo_dirs.push(child);
            }
        }
    }

    let mut committed: HashSet<String> = HashSet::new();
    if !repo_dirs.is_empty() {
        std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(repo_dirs.len());
            for repo in &repo_dirs {
                handles.push(s.spawn(move || {
                    let mut local_ids: HashSet<String> = HashSet::new();
                    if let Some(git_dir) = resolve_git_dir_fast(repo) {
                        if let Ok(content) = fs::read_to_string(git_dir.join("logs/HEAD")) {
                            for line in content.lines() {
                                if let Some((_, msg)) = line.split_once('\t') {
                                    extract_ticket_tokens_from_text(msg, &mut local_ids);
                                }
                            }
                        }
                    }
                    if let Ok(out) = Command::new("git")
                        .args(["-C", &repo.to_string_lossy(), "log", "-n", "400", "--format=%s"])
                        .output()
                    {
                        if out.status.success() {
                            let stdout = String::from_utf8_lossy(&out.stdout);
                            extract_ticket_tokens_from_text(&stdout, &mut local_ids);
                        }
                    }
                    local_ids
                }));
            }
            for h in handles {
                if let Ok(local_ids) = h.join() {
                    committed.extend(local_ids);
                }
            }
        });
    }

    let proj_dir = project_root.join("project");
    if proj_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&proj_dir) {
            for entry in entries.flatten() {
                let td = entry.path();
                let td_name = td.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !td_name.starts_with("ticket-") || !td.is_dir() {
                    continue;
                }
                let readme = td.join("README.md");
                let Ok(r_txt) = fs::read_to_string(&readme) else {
                    continue;
                };
                if has_status_done_line(&r_txt) {
                    let low = td_name.to_ascii_lowercase();
                    committed.insert(low.clone());
                    if let Some(digits) = low.strip_prefix("ticket-") {
                        let num_part: String =
                            digits.chars().take_while(|c| c.is_ascii_digit()).collect();
                        if let Ok(n) = num_part.parse::<u64>() {
                            committed.insert(format!("ticket-{:03}", n));
                            committed.insert(format!("ticket-{}", n));
                        }
                    }
                    extract_ticket_tokens_from_text(&r_txt, &mut committed);
                }
            }
        }
    }

    committed.into_iter().collect()
}

