# planfile-io

High-performance native I/O engine for planfile written in Rust with PyO3 bindings.

## Features

- **Fast YAML/JSON parsing**: Powered by `serde_yaml` and `serde_json`, 20x–100x faster than pure PyYAML.
- **Self-healing `.fast.json` mirror**: Validates file `mtime_ns` and falls back automatically.
- **Deterministic Sharding**: Instant shard routing for sprint tickets (SHA-256 bucket allocation & numeric ranges).
- **Atomic File Operations**: Safe writes using `tempfile` + `fsync` + rename.

## Installation

```bash
pip install planfile-io
```

Or build from source:

```bash
maturin develop --release
```
