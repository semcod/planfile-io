"""High-performance I/O engine for planfile with Rust acceleration and Python fallback."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from .planfile_io_rs import (
        read_yaml_fast as _rs_read_yaml_fast,
        write_yaml_fast as _rs_write_yaml_fast,
        resolve_shard_path as _rs_resolve_shard_path,
    )
    HAS_RUST_IO = True
except ImportError:
    try:
        from planfile_io_rs import (
            read_yaml_fast as _rs_read_yaml_fast,
            write_yaml_fast as _rs_write_yaml_fast,
            resolve_shard_path as _rs_resolve_shard_path,
        )
        HAS_RUST_IO = True
    except ImportError:
        HAS_RUST_IO = False


def read_yaml_fast(path: str | Path) -> Any:
    """Read YAML file using native Rust engine (with .fast.json caching)."""
    p = str(path)
    if HAS_RUST_IO:
        return _rs_read_yaml_fast(p)
    import yaml
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_yaml_fast(path: str | Path, data: Any) -> None:
    """Write YAML file and mirror using native Rust engine."""
    p = str(path)
    if HAS_RUST_IO:
        _rs_write_yaml_fast(p, data)
        return
    import yaml
    with open(p, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, default_flow_style=False)


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
    # Simple fallback
    return Path(sprints_dir) / f"{sprint}.shards" / f"{ticket_id}.yaml"


__all__ = ["read_yaml_fast", "write_yaml_fast", "resolve_shard_path", "HAS_RUST_IO"]
