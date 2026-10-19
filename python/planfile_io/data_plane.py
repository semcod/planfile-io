"""wellmanifest/data-plane (sqlite-embedded-es-v1 + low-ram-v1) and wellmanifest/data-gateway
reusable storage engine for Planfile, backed by planfile-io native Rust primitives.
"""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager
from itertools import islice
import json
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any

from . import (
    canonical_digest,
    compute_event_hash,
    format_gateway_line,
    get_runtime_config,
    parse_gateway_line,
)

SQLITE_INDEX_SCHEMA = "planfile.sqlite-ticket-index/v1"
DATA_PLANE_EVENT_SCHEMA = "data-plane.domain-event/v1"
WELLMANIFEST_LOGS_SCHEMA = "wellmanifest.logs/event/v1"
_SCHEMA_VERSION = 1
_SOURCE_INDEXED_AT_NS_KEY = "source_indexed_at_ns"
_ZERO_HASH = "0" * 64


def _apply_pragma(db: sqlite3.Connection, pragma: str) -> None:
    db.execute(f"PRAGMA {pragma}")


class _CommitBatch:
    """Group-commit batch sharing one SQLite transaction and fsync (wellmanifest/data-gateway)."""

    def __init__(self) -> None:
        self.members = 0
        self.durable = False
        self.error: BaseException | None = None
        self.done = threading.Event()


class PooledSQLiteTicketIndex:
    """Persistent-connection SQLite ticket projection adhering to wellmanifest/data-plane low-ram-v1.

    Eliminates per-query connection creation and repeated PRAGMA/sqlite_master roundtrips
    while bounding page cache memory (`PRAGMA cache_size = -2048`, `temp_store = FILE`, `mmap_size = 0`)
    via dynamic configuration (`wellmanifest/nohardcode`).
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._conn_inode: tuple[int, int] | None = None

    @staticmethod
    def serialize_signature(signature: tuple | list) -> str:
        return json.dumps(signature, ensure_ascii=False, separators=(",", ":"))

    def _file_inode(self) -> tuple[int, int] | None:
        try:
            st = self.path.stat()
            return (st.st_dev, st.st_ino)
        except OSError:
            return None

    def _ensure_connection(self) -> sqlite3.Connection:
        current_inode = self._file_inode()
        if self._conn is not None and current_inode is not None and self._conn_inode == current_inode:
            return self._conn
        if self._conn is not None:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
            self._conn = None

        cfg = get_runtime_config()
        timeout_sec = max(0.1, float(cfg["sqlite_busy_timeout_ms"]) / 1000.0)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self.path),
            timeout=timeout_sec,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        for pragma in (
            "foreign_keys = ON",
            "journal_mode = WAL",
            "synchronous = NORMAL",
            f"cache_size = {int(cfg['sqlite_cache_kb'])}",
            "mmap_size = 0",
            "temp_store = FILE",
            f"busy_timeout = {int(cfg['sqlite_busy_timeout_ms'])}",
        ):
            _apply_pragma(conn, pragma)
        self._initialize(conn)
        self._conn = conn
        self._conn_inode = self._file_inode()
        return conn

    @staticmethod
    def _initialize(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tickets (
                id TEXT PRIMARY KEY,
                sprint TEXT NOT NULL,
                status TEXT NOT NULL,
                priority TEXT NOT NULL,
                source TEXT,
                queue TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT,
                position INTEGER NOT NULL,
                ticket_json TEXT NOT NULL,
                summary_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS tickets_sprint_position
                ON tickets(sprint, position);
            CREATE INDEX IF NOT EXISTS tickets_sprint_status_priority
                ON tickets(sprint, status, priority, position);
            CREATE INDEX IF NOT EXISTS tickets_source
                ON tickets(source, position);
            CREATE TABLE IF NOT EXISTS dependencies (
                ticket_id TEXT NOT NULL,
                blocked_by TEXT NOT NULL,
                PRIMARY KEY(ticket_id, blocked_by),
                FOREIGN KEY(ticket_id) REFERENCES tickets(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS dependencies_blocked_by
                ON dependencies(blocked_by);
            """
        )
        connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema', ?)",
            (SQLITE_INDEX_SCHEMA,),
        )
        connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
            (str(_SCHEMA_VERSION),),
        )

    @staticmethod
    def _insert_rebuild_batch(
        connection: sqlite3.Connection,
        batch: list[dict[str, Any]],
    ) -> None:
        connection.executemany(
            """
            INSERT INTO tickets(
                id, sprint, status, priority, source, queue,
                created_at, updated_at, position, ticket_json, summary_json
            ) VALUES(
                :id, :sprint, :status, :priority, :source, :queue,
                :created_at, :updated_at, :position, :ticket_json, :summary_json
            )
            """,
            batch,
        )
        connection.executemany(
            "INSERT INTO dependencies(ticket_id, blocked_by) VALUES(?, ?)",
            (
                (row["id"], dep)
                for row in batch
                for dep in row.get("blocked_by", [])
            ),
        )

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except sqlite3.Error:
                    pass
                self._conn = None
                self._conn_inode = None

    def is_current(self, signature: tuple | list) -> bool:
        if not self.path.exists():
            return False
        expected = self.serialize_signature(signature)
        try:
            with self._lock:
                conn = self._ensure_connection()
                row = conn.execute(
                    "SELECT value FROM meta WHERE key='source_signature'"
                ).fetchone()
                return row is not None and row["value"] == expected
        except sqlite3.DatabaseError:
            return False

    def rebuild(
        self,
        records: Iterable[dict[str, Any]],
        signature: tuple | list,
        *,
        batch_size: int | None = None,
    ) -> int:
        if batch_size is None:
            batch_size = int(get_runtime_config()["sqlite_batch_size"])
        if batch_size < 1:
            raise ValueError("ticket_index_rebuild_batch_size_invalid")
        rows = iter(records)
        count = 0
        with self._lock:
            conn = self._ensure_connection()
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM dependencies")
                conn.execute("DELETE FROM tickets")
                while True:
                    batch = list(islice(rows, batch_size))
                    if not batch:
                        break
                    self._insert_rebuild_batch(conn, batch)
                    count += len(batch)
                    batch.clear()
                conn.execute(
                    """
                    INSERT INTO meta(key, value) VALUES('source_signature', ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    (self.serialize_signature(signature),),
                )
                conn.execute(
                    """
                    INSERT INTO meta(key, value) VALUES(?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    (_SOURCE_INDEXED_AT_NS_KEY, str(time.time_ns())),
                )
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        return count

    def get_ticket(self, ticket_id: str) -> dict[str, Any] | None:
        with self._lock:
            conn = self._ensure_connection()
            row = conn.execute(
                "SELECT ticket_json FROM tickets WHERE id=?",
                (ticket_id,),
            ).fetchone()
        return json.loads(row["ticket_json"]) if row is not None else None

    def render_payloads(
        self,
        *,
        sprint: str,
        filters: dict[str, Any],
        offset: int = 0,
        limit: int | None = None,
    ) -> tuple[bytes, int, int]:
        conditions: list[str] = []
        parameters: list[Any] = []
        if sprint != "all":
            conditions.append("sprint=?")
            parameters.append(sprint)
        for key in ("status", "priority", "source"):
            value = filters.get(key)
            if value is None:
                continue
            conditions.append(f"{key}=?")
            parameters.append(str(getattr(value, "value", value)))
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._lock:
            conn = self._ensure_connection()
            total = int(
                conn.execute(
                    f"SELECT COUNT(*) AS count FROM tickets{where}",
                    parameters,
                ).fetchone()["count"]
            )
            sql = f"SELECT ticket_json FROM tickets{where} ORDER BY position"
            page_parameters = list(parameters)
            if limit is not None:
                sql += " LIMIT ? OFFSET ?"
                page_parameters.extend((limit, offset))
            elif offset:
                sql += " LIMIT -1 OFFSET ?"
                page_parameters.append(offset)
            cursor = conn.execute(sql, page_parameters)
            chunks = [row["ticket_json"] for row in cursor]
        count = len(chunks)
        return ("[" + ",".join(chunks) + "]").encode("utf-8"), total, count

    def reset(self) -> None:
        self.close()
        for p in (
            self.path,
            self.path.with_name(self.path.name + "-wal"),
            self.path.with_name(self.path.name + "-shm"),
        ):
            p.unlink(missing_ok=True)


class DataPlaneGatewayStore:
    """SQLite Event Store + Audit Chain with SAVEPOINT group-commit (wellmanifest/data-plane & data-gateway)."""

    DDL = """
    CREATE TABLE IF NOT EXISTS events (
        event_urn TEXT PRIMARY KEY,
        stream_urn TEXT NOT NULL,
        version INTEGER NOT NULL,
        hash TEXT NOT NULL,
        canonical BLOB NOT NULL,
        UNIQUE(stream_urn, version)
    );
    CREATE TABLE IF NOT EXISTS receipts (
        operation TEXT NOT NULL,
        key TEXT NOT NULL,
        input_hash TEXT NOT NULL,
        result BLOB NOT NULL,
        PRIMARY KEY(operation, key)
    );
    CREATE TABLE IF NOT EXISTS audit (
        sequence INTEGER PRIMARY KEY,
        hash TEXT NOT NULL,
        canonical BLOB NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS immutable_event_update BEFORE UPDATE ON events
        BEGIN SELECT RAISE(ABORT, 'immutable event'); END;
    CREATE TRIGGER IF NOT EXISTS immutable_event_delete BEFORE DELETE ON events
        BEGIN SELECT RAISE(ABORT, 'immutable event'); END;
    CREATE TRIGGER IF NOT EXISTS immutable_audit_update BEFORE UPDATE ON audit
        BEGIN SELECT RAISE(ABORT, 'immutable audit'); END;
    CREATE TRIGGER IF NOT EXISTS immutable_audit_delete BEFORE DELETE ON audit
        BEGIN SELECT RAISE(ABORT, 'immutable audit'); END;
    """

    def __init__(
        self,
        path: str | Path,
        *,
        commit_window: float | None = None,
        max_batch: int | None = None,
    ) -> None:
        cfg = get_runtime_config()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.commit_window = (
            float(cfg["sqlite_commit_window_ms"]) / 1000.0
            if commit_window is None
            else commit_window
        )
        self.max_batch = int(cfg["sqlite_max_batch"]) if max_batch is None else int(max_batch)
        self.db = sqlite3.connect(
            str(self.path),
            isolation_level=None,
            check_same_thread=False,
        )
        for pragma in (
            "foreign_keys = ON",
            "journal_mode = WAL",
            "synchronous = FULL",
            f"cache_size = {int(cfg['sqlite_cache_kb'])}",
            "mmap_size = 0",
            "temp_store = FILE",
            f"busy_timeout = {int(cfg['sqlite_busy_timeout_ms'])}",
        ):
            _apply_pragma(self.db, pragma)
        self.db.executescript(self.DDL)
        self.lock = threading.Lock()
        self.state = threading.Lock()
        self.inflight = 0
        self.batch: _CommitBatch | None = None

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def transaction(self, *, durable: bool = True):
        with self.state:
            self.inflight += 1
        batch: _CommitBatch | None = None
        try:
            with self.lock:
                if self.batch is not None and durable and not self.batch.durable:
                    self._commit_locked(self.batch)
                if self.batch is None:
                    _apply_pragma(self.db, f"synchronous = {'FULL' if durable else 'NORMAL'}")
                    self.db.execute("BEGIN IMMEDIATE")
                    self.batch = _CommitBatch()
                    self.batch.durable = durable
                batch = self.batch
                batch.members += 1
                self.db.execute("SAVEPOINT command")
                try:
                    yield self
                except BaseException:
                    self.db.execute("ROLLBACK TO command")
                    self.db.execute("RELEASE command")
                    raise
                self.db.execute("RELEASE command")
                if self.commit_window is None or self.commit_window <= 0:
                    self._commit_locked(batch)
        finally:
            with self.state:
                self.inflight -= 1
                alone = self.inflight == 0
            if batch is not None and (
                alone
                or batch.members >= self.max_batch
                or (self.commit_window and not batch.done.wait(self.commit_window))
            ):
                with self.lock:
                    self._commit_locked(batch)
        if batch is not None and batch.error is not None:
            raise batch.error

    def _commit_locked(self, batch: _CommitBatch) -> None:
        if self.batch is not batch:
            return
        try:
            self.db.execute("COMMIT")
        except sqlite3.Error as error:
            batch.error = error
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
        finally:
            self.batch = None
            batch.done.set()

    def execute_line(self, line_text: str) -> dict[str, Any]:
        """Parse and record a canonical data-plane.line/v1 operation with wellmanifest/logs audit."""
        kind, target, variables, input_hash = parse_gateway_line(line_text, strict=False)
        canonical_line = format_gateway_line(kind, target, variables)
        with self.transaction(durable=True):
            row = self.db.execute(
                "SELECT sequence, hash FROM audit ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence, prev_hash = (row[0] + 1, row[1]) if row else (1, _ZERO_HASH)
            audit_event = {
                "schema": WELLMANIFEST_LOGS_SCHEMA,
                "eventId": f"event:planfile-io-{sequence}",
                "stream": "audit:planfile-data-gateway",
                "sequence": sequence,
                "eventType": "gateway.request_completed",
                "producer": "planfile-io",
                "source": "gateway.line",
                "mode": "APPLY",
                "outcome": "SUCCEEDED",
                "subjectRef": target,
                "inputHash": input_hash,
                "previousHash": prev_hash,
            }
            event_hash = compute_event_hash(audit_event)
            audit_event["eventHash"] = event_hash
            canonical_bytes = json.dumps(
                audit_event, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            self.db.execute(
                "INSERT INTO audit VALUES (?, ?, ?)",
                (sequence, event_hash, canonical_bytes),
            )
        return {
            "kind": kind,
            "target": target,
            "variables": variables,
            "canonical_line": canonical_line,
            "input_hash": input_hash,
            "audit_sequence": sequence,
            "audit_hash": event_hash,
        }


__all__ = [
    "DataPlaneGatewayStore",
    "PooledSQLiteTicketIndex",
    "SQLITE_INDEX_SCHEMA",
    "canonical_digest",
    "compute_event_hash",
    "format_gateway_line",
    "parse_gateway_line",
]
