"""High-performance I/O and Data-Plane engine for planfile with Rust acceleration and Python fallback."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

try:
    from .planfile_io_rs import (
        audit_mirror_fast as _rs_audit_mirror_fast,
        batch_inventory_sprints as _rs_batch_inventory_sprints,
        canonical_digest as _rs_canonical_digest,
        collect_committed_ticket_ids_fast as _rs_collect_committed_ticket_ids_fast,
        collect_dir_historical_done_fast as _rs_collect_dir_historical_done_fast,
        collect_sprint_done_fast as _rs_collect_sprint_done_fast,
        compute_event_hash as _rs_compute_event_hash,
        compute_source_signature as _rs_compute_source_signature,
        discover_project_candidates_fast as _rs_discover_project_candidates_fast,
        format_gateway_line as _rs_format_gateway_line,
        get_runtime_config as _rs_get_runtime_config,
        is_git_tracked_fast as _rs_is_git_tracked_fast,
        load_sharded_sprint as _rs_load_sharded_sprint,
        parse_gateway_line as _rs_parse_gateway_line,
        read_yaml_fast as _rs_read_yaml_fast,
        resolve_shard_path as _rs_resolve_shard_path,
        write_yaml_fast as _rs_write_yaml_fast,
    )
    HAS_RUST_IO = True
except ImportError:
    try:
        from planfile_io_rs import (
            audit_mirror_fast as _rs_audit_mirror_fast,
            batch_inventory_sprints as _rs_batch_inventory_sprints,
            canonical_digest as _rs_canonical_digest,
            collect_committed_ticket_ids_fast as _rs_collect_committed_ticket_ids_fast,
            collect_dir_historical_done_fast as _rs_collect_dir_historical_done_fast,
            collect_sprint_done_fast as _rs_collect_sprint_done_fast,
            compute_event_hash as _rs_compute_event_hash,
            compute_source_signature as _rs_compute_source_signature,
            discover_project_candidates_fast as _rs_discover_project_candidates_fast,
            format_gateway_line as _rs_format_gateway_line,
            get_runtime_config as _rs_get_runtime_config,
            is_git_tracked_fast as _rs_is_git_tracked_fast,
            load_sharded_sprint as _rs_load_sharded_sprint,
            parse_gateway_line as _rs_parse_gateway_line,
            read_yaml_fast as _rs_read_yaml_fast,
            resolve_shard_path as _rs_resolve_shard_path,
            write_yaml_fast as _rs_write_yaml_fast,
        )
        HAS_RUST_IO = True
    except ImportError:
        HAS_RUST_IO = False


def get_runtime_config() -> dict[str, Any]:
    """Return dynamic storage configuration governed by wellmanifest/nohardcode & data-plane low-ram-v1."""
    if HAS_RUST_IO:
        return _rs_get_runtime_config()
    return {
        "max_cacheable_yaml_bytes": int(os.environ.get("PLANFILE_MAX_CACHEABLE_YAML_BYTES", "5000000")),
        "shard_size": int(os.environ.get("PLANFILE_SHARD_SIZE", "100")),
        "custom_shards": int(os.environ.get("PLANFILE_CUSTOM_SHARDS", "16")),
        "sqlite_batch_size": int(os.environ.get("PLANFILE_SQLITE_BATCH_SIZE", "128")),
        "sqlite_cache_kb": int(os.environ.get("PLANFILE_SQLITE_CACHE_KB", "-2048")),
        "sqlite_busy_timeout_ms": int(os.environ.get("PLANFILE_SQLITE_BUSY_TIMEOUT_MS", "5000")),
        "sqlite_commit_window_ms": int(os.environ.get("PLANFILE_SQLITE_COMMIT_WINDOW_MS", "2")),
        "sqlite_max_batch": int(os.environ.get("PLANFILE_SQLITE_MAX_BATCH", "64")),
        "lock_timeout_seconds": float(os.environ.get("PLANFILE_LOCK_TIMEOUT_SECONDS", "10.0")),
        "max_inline_content_bytes": int(os.environ.get("PLANFILE_MAX_INLINE_CONTENT_BYTES", "16384")),
    }


def read_yaml_fast(path: str | Path) -> Any:
    """Read YAML file using native Rust engine (with .fast.json caching and yaml_mtime_ns support)."""
    p = str(path)
    if HAS_RUST_IO:
        return _rs_read_yaml_fast(p)
    import yaml
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_yaml_fast(path: str | Path, data: Any) -> None:
    """Write YAML file and .fast.json mirror using native Rust engine."""
    p = str(path)
    if HAS_RUST_IO:
        _rs_write_yaml_fast(p, data)
        return
    import yaml
    with open(p, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, default_flow_style=False)


def audit_mirror_fast(path: str | Path) -> dict[str, Any]:
    """Verify .fast.json mirror against authoritative YAML and self-heal on drift."""
    p = str(path)
    if HAS_RUST_IO:
        return _rs_audit_mirror_fast(p)
    return {"path": p, "ok": True, "healed": False, "reason": None}


def resolve_shard_path(
    sprints_dir: str | Path,
    sprint: str,
    ticket_id: str,
    shard_size: int = 100,
    custom_shards: int = 16,
) -> Path:
    """Resolve physical shard path for a given ticket ID."""
    if HAS_RUST_IO:
        res = _rs_resolve_shard_path(str(sprints_dir), sprint, ticket_id, shard_size, custom_shards)
        return Path(res)
    return Path(sprints_dir) / f"{sprint}.shards" / f"{ticket_id}.yaml"


def load_sharded_sprint(sprints_dir: str | Path, sprint: str) -> dict[str, Any]:
    """Load and merge metadata.yaml + all tickets-*.yaml shards for a sprint in native Rust."""
    if HAS_RUST_IO:
        return _rs_load_sharded_sprint(str(sprints_dir), sprint)
    sprint_dir = Path(sprints_dir) / f"{sprint}.shards"
    meta_path = sprint_dir / "metadata.yaml"
    meta = read_yaml_fast(meta_path) if meta_path.exists() else {}
    sprint_obj = dict(meta.get("sprint", meta) if isinstance(meta, dict) else {})
    tickets: dict[str, Any] = {}
    if sprint_dir.exists():
        for p in sorted(sprint_dir.glob("tickets-*.yaml")):
            data = read_yaml_fast(p) or {}
            if isinstance(data, dict) and isinstance(data.get("tickets"), dict):
                tickets.update(data["tickets"])
    sprint_obj.setdefault("id", sprint)
    sprint_obj.setdefault("status", "active")
    sprint_obj["tickets"] = tickets
    return {"sprint": sprint_obj}


def compute_source_signature(paths: list[str | Path]) -> list[tuple[str, int, int]]:
    """Batch stat files in Rust and return (path, size, mtime_ns) tuples."""
    str_paths = [str(p) for p in paths]
    if HAS_RUST_IO:
        return _rs_compute_source_signature(str_paths)
    out: list[tuple[str, int, int]] = []
    for p in str_paths:
        try:
            st = os.stat(p)
            out.append((p, st.st_size, st.st_mtime_ns))
        except OSError:
            pass
    return out


def parse_gateway_line(text: str, *, strict: bool = False) -> tuple[str, str, dict[str, Any], str]:
    """Parse data-plane.line/v1 (`kind: target {vars}`) and return (kind, target, vars, sha256)."""
    if HAS_RUST_IO:
        return _rs_parse_gateway_line(text, strict)
    head, sep, rest = text.strip().partition(": ")
    if not sep:
        raise ValueError("Expected `type: target`")
    target, space, payload = rest.partition(" ")
    variables = json.loads(payload) if space else {}
    canonical = format_gateway_line(head, target, variables)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return head, target, variables, digest


def format_gateway_line(kind: str, target: str, variables: dict[str, Any] | None = None) -> str:
    """Format canonical data-plane.line/v1 string."""
    if HAS_RUST_IO:
        return _rs_format_gateway_line(kind, target, variables)
    variables = variables or {}
    if not variables:
        return f"{kind}: {target}"
    body = json.dumps(variables, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{kind}: {target} {body}"


def canonical_digest(value: Any) -> str:
    """Compute SHA-256 of wellmanifest-canonical-json-v1 representation."""
    if HAS_RUST_IO:
        return _rs_canonical_digest(value)
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def compute_event_hash(event: dict[str, Any]) -> str:
    """Compute canonical eventHash excluding eventHash field."""
    if HAS_RUST_IO:
        return _rs_compute_event_hash(event)
    filtered = {k: v for k, v in event.items() if k != "eventHash"}
    return canonical_digest(filtered)


def discover_project_candidates_fast(base: str | Path) -> list[str]:
    """Discover governed and legacy Planfile project roots under base in native Rust."""
    if HAS_RUST_IO:
        return _rs_discover_project_candidates_fast(str(base))
    return []


def collect_sprint_done_fast(path: str | Path) -> list[str]:
    """Extract completed ticket IDs from a sprint YAML / .fast.json mirror in native Rust."""
    if HAS_RUST_IO:
        return _rs_collect_sprint_done_fast(str(path))
    data = read_yaml_fast(path) or {}
    tickets = data.get("sprint", {}).get("tickets") or data.get("tickets") or {}
    if not isinstance(tickets, dict):
        return []
    out: list[str] = []
    for tid, t in tickets.items():
        if isinstance(t, dict) and (
            t.get("status") in ("closed", "done", "completed", "cancelled", "canceled")
            or (isinstance(t.get("execution"), dict) and t["execution"].get("state") in ("done", "completed", "cancelled", "canceled"))
        ):
            out.append(str(tid))
    return out


def collect_dir_historical_done_fast(sprint_dir: str | Path) -> list[str]:
    """Collect all completed ticket IDs across archived sprint files in sprint_dir in native Rust."""
    if HAS_RUST_IO:
        return _rs_collect_dir_historical_done_fast(str(sprint_dir))
    sdir = Path(sprint_dir)
    if not sdir.is_dir():
        return []
    union_done: set[str] = set()
    for p in sorted(sdir.glob("*.yaml")):
        if p.name != "current.yaml":
            union_done.update(collect_sprint_done_fast(p))
    return sorted(union_done)


def batch_inventory_sprints(project_roots: list[str | Path]) -> list[dict[str, Any]]:
    """Load current.yaml and historical_done in parallel across project_roots in native Rust."""
    str_roots = [str(p) for p in project_roots]
    if HAS_RUST_IO:
        return _rs_batch_inventory_sprints(str_roots)
    rows: list[dict[str, Any]] = []
    for r in str_roots:
        curr = Path(r) / ".planfile/sprints/current.yaml"
        try:
            data = read_yaml_fast(curr) or {}
            hdone = collect_dir_historical_done_fast(curr.parent)
            rows.append({"project_root": r, "data": data, "historical_done": hdone})
        except Exception as exc:
            rows.append({"project_root": r, "error": str(exc)})
    return rows


def is_git_tracked_fast(project_root: str | Path, rel_path: str | Path) -> bool | None:
    """Check binary .git/index v2/v3 directly in Rust to see if rel_path is tracked."""
    if HAS_RUST_IO:
        return _rs_is_git_tracked_fast(str(project_root), str(rel_path))
    return None


def collect_committed_ticket_ids_fast(project_root: str | Path) -> list[str]:
    """Scan git commits and project/ticket-*/README.md across project_root in parallel Rust threads."""
    if HAS_RUST_IO:
        return _rs_collect_committed_ticket_ids_fast(str(project_root))
    return []


def _load_dockuri_uri() -> Any:
    import sys

    try:
        import dockuri_uri
        return dockuri_uri
    except ImportError:
        cand = Path.home() / "github" / "dockuri" / "dockuri" / "python"
        if cand.is_dir() and str(cand) not in sys.path:
            sys.path.insert(0, str(cand))
        import dockuri_uri
        return dockuri_uri


def uri(address: str, payload: Any = None, **kwargs: Any) -> Any:
    """Universal `uri('<full-uri>', [payload_or_jsonl])` caller backed by `dockuri_uri`."""
    return _load_dockuri_uri().uri(address, payload, **kwargs)


def uri_jsonl(address: str, lines: Any, **kwargs: Any) -> list[dict[str, Any]]:
    """Stream JSONL lines or dicts into a `dockuri` URI process."""
    return _load_dockuri_uri().uri_jsonl(address, lines, **kwargs)


__all__ = [
    "HAS_RUST_IO",
    "audit_mirror_fast",
    "batch_inventory_sprints",
    "canonical_digest",
    "collect_committed_ticket_ids_fast",
    "collect_dir_historical_done_fast",
    "collect_sprint_done_fast",
    "compute_event_hash",
    "compute_source_signature",
    "discover_project_candidates_fast",
    "format_gateway_line",
    "get_runtime_config",
    "is_git_tracked_fast",
    "load_sharded_sprint",
    "parse_gateway_line",
    "read_yaml_fast",
    "resolve_shard_path",
    "uri",
    "uri_jsonl",
    "write_yaml_fast",
]

