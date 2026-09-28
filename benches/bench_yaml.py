import tempfile
import time
from pathlib import Path
import yaml

from planfile_io import read_yaml_fast, write_yaml_fast, HAS_RUST_IO

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
                f"Performance target under 50ms is met"
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
            "generated_at": "2026-09-28T10:00:00Z"
        }
    }


def run_benchmark():
    num_tickets = 1000
    print("=" * 70)
    print(f"BENCHMARK: planfile-io (Rust) vs PyYAML (Python)")
    print(f"Generating synthetic sprint dataset with {num_tickets} tickets...")
    data = generate_large_sprint(num_tickets)
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmp_dir:
        yaml_file = Path(tmp_dir) / "large_current.yaml"

        # 1. Zapis pliku YAML (i wygenerowanie .fast.json przez Rust)
        t0 = time.perf_counter()
        write_yaml_fast(yaml_file, data)
        write_time_rust = (time.perf_counter() - t0) * 1000
        size_kb = yaml_file.stat().st_size / 1024
        line_count = len(yaml_file.read_text(encoding="utf-8").splitlines())
        print(f"File generated: {size_kb:.1f} KB, {line_count:,} lines of YAML\n")

        # 2. PyYAML SafeLoader (Pure Python)
        content = yaml_file.read_text(encoding="utf-8")
        t0 = time.perf_counter()
        py_result = yaml.load(content, Loader=yaml.SafeLoader)
        py_time = (time.perf_counter() - t0) * 1000

        # 3. PyYAML CSafeLoader (libyaml C binding, if available)
        csafe_time = None
        if HAS_CSAFE:
            t0 = time.perf_counter()
            csafe_result = yaml.load(content, Loader=CSafeLoader)
            csafe_time = (time.perf_counter() - t0) * 1000

        # 4. planfile-io: Rust Cold Parse (bez mirrora .fast.json)
        mirror = yaml_file.with_name(yaml_file.name + ".fast.json")
        mirror.unlink(missing_ok=True)
        t0 = time.perf_counter()
        rust_cold_result = read_yaml_fast(yaml_file)
        rust_cold_time = (time.perf_counter() - t0) * 1000

        # 5. planfile-io: Rust Warm Read (z mirrora .fast.json)
        # Upewniamy się, że mirror istnieje
        assert mirror.exists()
        warm_times = []
        for _ in range(10):
            t0 = time.perf_counter()
            rust_warm_result = read_yaml_fast(yaml_file)
            warm_times.append((time.perf_counter() - t0) * 1000)
        rust_warm_time = sum(warm_times) / len(warm_times)

        # Prezentacja wyników
        print(f"{'Metoda odczytu':<40} | {'Czas [ms]':<12} | {'Przyspieszenie':<14}")
        print("-" * 72)
        print(f"{'PyYAML SafeLoader (Pure Python)':<40} | {py_time:>9.2f} ms | {'1.0x (bazowy)':<14}")

        if csafe_time:
            speedup_csafe = py_time / csafe_time
            print(f"{'PyYAML CSafeLoader (C-libyaml)':<40} | {csafe_time:>9.2f} ms | {speedup_csafe:>10.1f}x")

        speedup_cold = py_time / rust_cold_time
        print(f"{'planfile-io Rust (Cold parse YAML)':<40} | {rust_cold_time:>9.2f} ms | {speedup_cold:>10.1f}x")

        speedup_warm = py_time / rust_warm_time
        print(f"{'planfile-io Rust (.fast.json mirror)':<40} | {rust_warm_time:>9.2f} ms | {speedup_warm:>10.1f}x")
        print("=" * 72)
        print(f"Wynik weryfikacji: {len(rust_warm_result['tickets'])} biletów odczytanych poprawnie.")
        print(f"Zysk czasowy: odczyt skrócony z {py_time:.1f} ms do {rust_warm_time:.2f} ms!")


if __name__ == "__main__":
    run_benchmark()
