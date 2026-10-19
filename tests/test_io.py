import json
import os
import tempfile
from pathlib import Path
import pytest

from planfile_io import (
    HAS_RUST_IO,
    audit_mirror_fast,
    canonical_digest,
    compute_event_hash,
    compute_source_signature,
    format_gateway_line,
    get_runtime_config,
    load_sharded_sprint,
    parse_gateway_line,
    read_yaml_fast,
    resolve_shard_path,
    write_yaml_fast,
)
from planfile_io.data_plane import DataPlaneGatewayStore, PooledSQLiteTicketIndex


def test_has_rust_io():
    assert HAS_RUST_IO is True, "Rust extension module planfile_io_rs must be loaded"


def test_write_and_read_fast_and_mirror_compat():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "sprint.yaml"
        sample_data = {
            "name": "Sprint 42",
            "tickets": [
                {"id": "PLF-001", "title": "First ticket", "points": 5},
                {"id": "PLF-002", "title": "Second ticket", "points": 8},
            ],
            "stats": {"total": 2, "completed": 0},
        }

        write_yaml_fast(tmp_path, sample_data)
        assert tmp_path.exists()

        mirror = tmp_path.with_name(tmp_path.name + ".fast.json")
        assert mirror.exists()
        mirror_payload = json.loads(mirror.read_text(encoding="utf-8"))
        assert mirror_payload["version"] == 1
        assert "yaml_mtime_ns" in mirror_payload
        assert "mtime_ns" in mirror_payload

        loaded = read_yaml_fast(tmp_path)
        assert loaded["name"] == "Sprint 42"
        assert len(loaded["tickets"]) == 2
        assert loaded["tickets"][0]["id"] == "PLF-001"

        audit = audit_mirror_fast(tmp_path)
        assert audit["ok"] is True
        assert audit["healed"] is False

        # Corrupt mirror data and verify self-healing via audit_mirror_fast
        mirror_payload["data"]["name"] = "Corrupted Sprint"
        mirror.write_text(json.dumps(mirror_payload), encoding="utf-8")
        healed = audit_mirror_fast(tmp_path)
        assert healed["ok"] is False
        assert healed["healed"] is True
        assert read_yaml_fast(tmp_path)["name"] == "Sprint 42"


def test_shard_routing_and_sharded_sprint_loader():
    res = resolve_shard_path("/tmp/sprints", "current", "PLF-00150", shard_size=100)
    assert "tickets-000100-000199.yaml" in str(res)

    res_custom = resolve_shard_path("/tmp/sprints", "current", "custom-bug-fix", custom_shards=16)
    assert "tickets-custom-" in str(res_custom)

    with tempfile.TemporaryDirectory() as tmp_dir:
        sprints_dir = Path(tmp_dir)
        shard_dir = sprints_dir / "current.shards"
        shard_dir.mkdir(parents=True)
        write_yaml_fast(
            shard_dir / "metadata.yaml",
            {"sprint": {"id": "current", "name": "Current Sprint", "status": "active"}},
        )
        write_yaml_fast(
            shard_dir / "tickets-000000-000099.yaml",
            {"tickets": {"PLF-001": {"id": "PLF-001", "status": "open"}}},
        )
        write_yaml_fast(
            shard_dir / "tickets-000100-000199.yaml",
            {"tickets": {"PLF-150": {"id": "PLF-150", "status": "done"}}},
        )
        merged = load_sharded_sprint(sprints_dir, "current")
        assert merged["sprint"]["id"] == "current"
        assert set(merged["sprint"]["tickets"].keys()) == {"PLF-001", "PLF-150"}

        sigs = compute_source_signature(
            [shard_dir / "metadata.yaml", shard_dir / "tickets-000000-000099.yaml"]
        )
        assert len(sigs) == 2
        assert sigs[0][1] > 0 and sigs[0][2] > 0


def test_nohardcode_runtime_config(monkeypatch):
    monkeypatch.setenv("PLANFILE_SQLITE_CACHE_KB", "-4096")
    monkeypatch.setenv("PLANFILE_SQLITE_BATCH_SIZE", "256")
    cfg = get_runtime_config()
    assert cfg["sqlite_cache_kb"] == -4096
    assert cfg["sqlite_batch_size"] == 256


def test_data_gateway_line_and_canonical_hash():
    line = 'ticket.update: urn:planfile:ticket:PLF-001 {"priority":"high","status":"done"}'
    kind, target, variables, digest = parse_gateway_line(line, strict=True)
    assert kind == "ticket.update"
    assert target == "urn:planfile:ticket:PLF-001"
    assert variables == {"priority": "high", "status": "done"}
    assert len(digest) == 64

    formatted = format_gateway_line(kind, target, {"status": "done", "priority": "high"})
    assert formatted == line

    # Canonical profile rejects floats per wellmanifest/data-plane LOG-021
    with pytest.raises(ValueError, match="Float is outside canonical profile"):
        canonical_digest({"bad_float": 1.25})


def test_pooled_sqlite_ticket_index_and_gateway_store():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "tickets.sqlite3"
        idx = PooledSQLiteTicketIndex(db_path)
        sig = [("current.yaml", 120, 1700000000)]
        records = [
            {
                "id": "PLF-001",
                "sprint": "current",
                "status": "open",
                "priority": "high",
                "source": "koru",
                "queue": "ready",
                "created_at": "2026-10-19T10:00:00Z",
                "updated_at": "2026-10-19T10:00:00Z",
                "position": 0,
                "ticket_json": json.dumps({"id": "PLF-001", "status": "open"}),
                "summary_json": json.dumps({"id": "PLF-001"}),
                "blocked_by": [],
            },
            {
                "id": "PLF-002",
                "sprint": "current",
                "status": "done",
                "priority": "normal",
                "source": "koru",
                "queue": "done",
                "created_at": "2026-10-19T10:01:00Z",
                "updated_at": "2026-10-19T10:02:00Z",
                "position": 1,
                "ticket_json": json.dumps({"id": "PLF-002", "status": "done"}),
                "summary_json": json.dumps({"id": "PLF-002"}),
                "blocked_by": ["PLF-001"],
            },
        ]
        assert idx.rebuild(records, sig) == 2
        assert idx.is_current(sig) is True
        assert idx.get_ticket("PLF-001") == {"id": "PLF-001", "status": "open"}
        raw_bytes, total, count = idx.render_payloads(
            sprint="current", filters={"status": "open"}, offset=0, limit=10
        )
        assert total == 1
        assert count == 1
        assert json.loads(raw_bytes.decode("utf-8")) == [{"id": "PLF-001", "status": "open"}]
        idx.close()

        gw_db = Path(tmp_dir) / "gateway.sqlite3"
        gw = DataPlaneGatewayStore(gw_db, commit_window=0.0)
        res1 = gw.execute_line('ticket.claim: urn:planfile:ticket:PLF-001 {"actor":"koru"}')
        res2 = gw.execute_line('ticket.complete: urn:planfile:ticket:PLF-001 {"actor":"koru"}')
        assert res1["audit_sequence"] == 1
        assert res2["audit_sequence"] == 2
        assert res1["audit_hash"] != res2["audit_hash"]
        gw.close()
