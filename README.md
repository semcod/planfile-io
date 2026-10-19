# planfile-io

High-performance native I/O and Data-Plane engine for `planfile` written in Rust with PyO3 bindings, compliant with:
- **`wellmanifest/data-plane`** (`sqlite-embedded-es-v1` & `low-ram-v1` profiles)
- **`wellmanifest/data-gateway`** (`data-plane.line/v1` canonical single-line operation kernel & `SAVEPOINT` group-commit)
- **`wellmanifest/nohardcode`** (`StorageRuntimeConfig` environment-governed thresholds and budgets)
- **`wellmanifest/performance`** (`performance.plan/v1` validated evidence & AST audit clean)

## Features

- **Fast YAML/JSON parsing & TOCTOU-guarded `.fast.json` mirror**: Powered by `serde_yaml` and `serde_json`, supporting both `yaml_mtime_ns` and `mtime_ns` envelope keys plus `audit_mirror_fast()` self-healing.
- **Native Sharded Sprint Merging (`load_sharded_sprint`)**: Reads and merges `metadata.yaml` and all `tickets-*.yaml` shards in Rust (~112× faster than pure-Python YAML shard loading).
- **Batch Source Signature (`compute_source_signature`)**: Computes `(path, size, mtime_ns)` in Rust without Python per-file `stat` overhead.
- **`wellmanifest/data-plane` & `data-gateway` Primitives**:
  - `parse_gateway_line` / `format_gateway_line` / `canonical_digest` / `compute_event_hash` (`wellmanifest-canonical-json-v1`).
  - `PooledSQLiteTicketIndex` (`planfile_io.data_plane`): Persistent WAL connection with `low-ram-v1` pragmas (`cache_size = -2048`, `temp_store = FILE`, `mmap_size = 0`, `busy_timeout = 5000`), eliminating the per-query reconnect and 4-PRAGMA overhead (~19.3× faster SQLite queries).
  - `DataPlaneGatewayStore` (`planfile_io.data_plane`): SQLite Event Store + `wellmanifest.logs/event/v1` audit chain with `SAVEPOINT` group-commit.
- **Dynamic Runtime Configuration (`get_runtime_config`)**: Governed by `PLANFILE_*` environment variables (`wellmanifest/nohardcode` `NOHARDCODE-011`).

## Installation

```bash
pip install planfile-io
```

Or build from source:

```bash
maturin build --release --locked
```
