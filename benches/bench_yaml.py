import json
from pathlib import Path
import sys
import tempfile
import time
import yaml

from planfile_io import HAS_RUST_IO, read_yaml_fast, write_yaml_fast

try:
    from yaml import CSafeLoader
    HAS_CSAFE = True
except ImportError:
    HAS_CSAFE = False


def generate_large_sprint(num_tickets: int = 1000) -> dict:
    tickets = []
    for i in range(1, num_tickets + 1):
        tickets.append({
            "id": f"PLF-{i:05d}",
            "name": f"Implement feature or bugfix item number {i} with detailed requirements",
            "status": "in_progress" if i % 3 == 0 else ("done" if i % 2 == 0 else "open"),
            "priority": "critical" if i % 10 == 0 else ("high" if i % 4 == 0 else "normal"),
            "sprint": "current",
            "description": f"Detailed description for ticket {i}. It involves multiple components and integration steps.",
            "acceptance_criteria": [
                f"Requirement 1 for {i} is strictly satisfied",
                f"Requirement 2 for {i} passes all integration tests",
                "Performance target under 50ms is met",
            ],
            "labels": ["backend", "performance", f"batch-{i // 50}"],
            "blocked_by": [f"PLF-{i - 1:05d}"] if i > 1 and i % 5 == 0 else [],
            "children": [],
            "files": [f"src/module_{i % 20}/handler_{i}.py", f"tests/test_{i}.py"],
        })
    return {
        "version": "1.1",
        "project": "semcod-benchmark",
        "sprint": "current",
        "tickets": tickets,
        "metadata": {
            "total_tickets": num_tickets,
            "generated_at": "2026-09-28T10:00:00Z",
        },
    }


def run_benchmark() -> dict:
    num_tickets = 1000
    data = generate_large_sprint(num_tickets)

    with tempfile.TemporaryDirectory() as tmp_dir:
        yaml_file = Path(tmp_dir) / "large_current.yaml"

        t0 = time.perf_counter()
        write_yaml_fast(yaml_file, data)
        write_time_rust = (time.perf_counter() - t0) * 1000
        size_kb = yaml_file.stat().st_size / 1024
        line_count = len(yaml_file.read_text(encoding="utf-8").splitlines())

        content = yaml_file.read_text(encoding="utf-8")
        t0 = time.perf_counter()
        _ = yaml.load(content, Loader=yaml.SafeLoader)
        py_time = (time.perf_counter() - t0) * 1000

        csafe_time = None
        if HAS_CSAFE:
            t0 = time.perf_counter()
            _ = yaml.load(content, Loader=CSafeLoader)
            csafe_time = (time.perf_counter() - t0) * 1000

        mirror = yaml_file.with_name(yaml_file.name + ".fast.json")
        mirror.unlink(missing_ok=True)
        t0 = time.perf_counter()
        _ = read_yaml_fast(yaml_file)
        rust_cold_time = (time.perf_counter() - t0) * 1000

        assert mirror.exists()
        warm_times = []
        for _ in range(10):
            t0 = time.perf_counter()
            rust_warm_result = read_yaml_fast(yaml_file)
            warm_times.append((time.perf_counter() - t0) * 1000)
        rust_warm_time = sum(warm_times) / len(warm_times)

        report = {
            "schema": "planfile-io.benchmark/v1",
            "has_rust_io": HAS_RUST_IO,
            "tickets": len(rust_warm_result["tickets"]),
            "size_kb": round(size_kb, 1),
            "line_count": line_count,
            "write_rust_ms": round(write_time_rust, 2),
            "pyyaml_safe_ms": round(py_time, 2),
            "pyyaml_csafe_ms": round(csafe_time, 2) if csafe_time else None,
            "rust_cold_ms": round(rust_cold_time, 2),
            "rust_warm_mirror_ms": round(rust_warm_time, 2),
            "speedup_warm_vs_pyyaml": round(py_time / rust_warm_time, 1),
        }
        sys.stdout.write(json.dumps(report, indent=2) + "\n")
        return report


if __name__ == "__main__":
    run_benchmark()
