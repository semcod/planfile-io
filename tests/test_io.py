import tempfile
from pathlib import Path
from planfile_io import read_yaml_fast, write_yaml_fast, resolve_shard_path, HAS_RUST_IO


def test_has_rust_io():
    assert HAS_RUST_IO is True, "Rust extension module planfile_io_rs must be loaded"


def test_write_and_read_fast():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "sprint.yaml"
        sample_data = {
            "name": "Sprint 42",
            "tickets": [
                {"id": "PLF-001", "title": "First ticket", "points": 5},
                {"id": "PLF-002", "title": "Second ticket", "points": 8},
            ],
            "stats": {"total": 2, "completed": 0}
        }

        # Zapis przez natywny silnik Rust
        write_yaml_fast(tmp_path, sample_data)
        assert tmp_path.exists()

        # Sprawdzenie czy mirror .fast.json powstał
        mirror = tmp_path.with_name(tmp_path.name + ".fast.json")
        assert mirror.exists()

        # Odczyt przez natywny silnik Rust
        loaded = read_yaml_fast(tmp_path)
        assert loaded["name"] == "Sprint 42"
        assert len(loaded["tickets"]) == 2
        assert loaded["tickets"][0]["id"] == "PLF-001"


def test_shard_routing():
    res = resolve_shard_path("/tmp/sprints", "current", "PLF-00150", shard_size=100)
    assert "tickets-000100-000199.yaml" in str(res)

    res_custom = resolve_shard_path("/tmp/sprints", "current", "custom-bug-fix", custom_shards=16)
    assert "tickets-custom-" in str(res_custom)
