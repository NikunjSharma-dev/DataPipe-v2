"""
datapipe/engine.py — Incremental indexing engine (v2).

Upgrades vs v1
--------------
• xxHash (xxh64) replaces SHA-256 — ~10x faster fingerprinting
• Pandas outer-merge delta computation (same pattern as DeltaContext)
• Pluggable parser registry (Strategy pattern)
• FileDelta dataclass separates added / modified / deleted clearly
• Store keeps a shadow _rows table + FTS5 virtual table per pipeline
• search() and sql() return DataFrames as before
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Generator, Iterable, Iterator

import pandas as pd
import xxhash

logger = logging.getLogger("datapipe.engine")

_DEFAULT_DB = os.getenv("DATAPIPE_DB", "./datapipe.db")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SourceFile:
    path: Path
    size: int
    mtime: float
    content_hash: str = ""
    file_type: str = ""

    def compute_hash(self) -> str:
        h = xxhash.xxh64()
        with open(self.path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        self.content_hash = h.hexdigest()
        return self.content_hash


@dataclass
class FileDelta:
    """Three-way diff result: files to add, update, or remove."""
    added: list[dict] = field(default_factory=list)
    modified: list[dict] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.added or self.modified or self.deleted)

    def __repr__(self) -> str:
        return (
            f"FileDelta(+{len(self.added)} ~{len(self.modified)} -{len(self.deleted)})"
        )


@dataclass
class IndexStats:
    total_files: int = 0
    new_files: int = 0
    updated_files: int = 0
    deleted_files: int = 0
    skipped_files: int = 0
    total_rows_indexed: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"  Files scanned  : {self.total_files}",
            f"  New            : {self.new_files}",
            f"  Updated        : {self.updated_files}",
            f"  Deleted        : {self.deleted_files}",
            f"  Skipped        : {self.skipped_files}",
            f"  Rows indexed   : {self.total_rows_indexed}",
            f"  Duration       : {self.duration_seconds:.2f}s",
        ]
        if self.errors:
            lines.append(f"  Errors         : {len(self.errors)}")
            for e in self.errors[:3]:
                lines.append(f"    • {e}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Store — SQLite backend
# ---------------------------------------------------------------------------

class Store:
    """
    SQLite-backed store with FTS5 full-text search.

    Tables
    ------
    _dp_files            — indexed file state (path, hash, mtime, type)
    _dp_meta             — pipeline key/value metadata
    dp_<name>_rows       — shadow rows table with JSON payloads
    dp_<name>_fts        — FTS5 virtual table (Porter stemming)
    """

    def __init__(self, path: str | Path = _DEFAULT_DB) -> None:
        self.db_path = Path(path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._bootstrap()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        c = self.conn
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise

    def _bootstrap(self) -> None:
        with self.transaction() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS _dp_files (
                    file_path    TEXT PRIMARY KEY,
                    pipeline     TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    mtime        REAL NOT NULL,
                    file_type    TEXT NOT NULL DEFAULT '',
                    indexed_at   REAL NOT NULL DEFAULT (unixepoch('now')),
                    row_count    INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS _dp_meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """)

    # -- file state ----------------------------------------------------------

    def get_file_state(self, path: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM _dp_files WHERE file_path = ?", (path,)
        ).fetchone()
        return dict(row) if row else None

    def upsert_file_state(
        self,
        path: str,
        pipeline: str,
        content_hash: str,
        mtime: float,
        file_type: str,
        row_count: int,
    ) -> None:
        with self.transaction() as c:
            c.execute(
                """
                INSERT INTO _dp_files
                    (file_path, pipeline, content_hash, mtime, file_type, indexed_at, row_count)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(file_path) DO UPDATE SET
                    content_hash = excluded.content_hash,
                    mtime        = excluded.mtime,
                    file_type    = excluded.file_type,
                    indexed_at   = excluded.indexed_at,
                    row_count    = excluded.row_count
                """,
                (path, pipeline, content_hash, mtime, file_type, time.time(), row_count),
            )

    def delete_file_state(self, path: str) -> None:
        with self.transaction() as c:
            c.execute("DELETE FROM _dp_files WHERE file_path = ?", (path,))

    def all_indexed_paths(self, pipeline: str) -> set[str]:
        rows = self.conn.execute(
            "SELECT file_path FROM _dp_files WHERE pipeline = ?", (pipeline,)
        ).fetchall()
        return {r["file_path"] for r in rows}

    def all_file_states(self, pipeline: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM _dp_files WHERE pipeline = ?", (pipeline,)
        ).fetchall()
        return [dict(r) for r in rows]

    # -- data tables ---------------------------------------------------------

    def ensure_data_table(self, table: str, columns: list[str]) -> None:
        # Use a standalone FTS5 table (no content= alias) to avoid the
        # "database disk image is malformed" error that arises when doing
        # manual deletes on content-table-backed FTS5 virtual tables.
        # We keep a shadow _rows table for JSON payloads + source_path lookup.
        cols = ", ".join(columns)
        with self.transaction() as c:
            c.executescript(f"""
                CREATE TABLE IF NOT EXISTS {table}_rows (
                    rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_path TEXT NOT NULL,
                    data_json   TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_{table}_src
                    ON {table}_rows(source_path);
                CREATE VIRTUAL TABLE IF NOT EXISTS {table}_fts
                    USING fts5(
                        source_path UNINDEXED,
                        {cols},
                        tokenize='porter ascii'
                    );
            """)

    def delete_rows_for_source(self, table: str, source_path: str) -> None:
        # Delete from standalone FTS5 table by source_path column, then shadow table
        with self.transaction() as c:
            try:
                c.execute(
                    f"DELETE FROM {table}_fts WHERE source_path = ?", (source_path,)
                )
            except Exception:
                pass
            c.execute(
                f"DELETE FROM {table}_rows WHERE source_path = ?", (source_path,)
            )

    def insert_rows(
        self,
        table: str,
        source_path: str,
        rows: list[dict],
        columns: list[str],
    ) -> None:
        # Insert into shadow table for JSON retrieval, and into standalone FTS5 for search
        fts_cols = ["source_path"] + columns
        fts_placeholders = ", ".join(["?"] * len(fts_cols))
        with self.transaction() as c:
            for row in rows:
                blob = json.dumps(row, ensure_ascii=False, default=str)
                c.execute(
                    f"INSERT INTO {table}_rows (source_path, data_json) VALUES (?,?)",
                    (source_path, blob),
                )
                fts_vals = [source_path] + [str(row.get(col, "")) for col in columns]
                c.execute(
                    f"INSERT INTO {table}_fts({', '.join(fts_cols)}) VALUES ({fts_placeholders})",
                    fts_vals,
                )

    def search(self, table: str, query: str, limit: int = 20) -> pd.DataFrame:
        try:
            # FTS5 standalone: join back to _rows via source_path for the JSON payload
            rows = self.conn.execute(
                f"""
                SELECT f.source_path, f.rank,
                       (SELECT r.data_json FROM {table}_rows r
                        WHERE r.source_path = f.source_path LIMIT 1) AS data_json
                FROM   {table}_fts f
                WHERE  {table}_fts MATCH ?
                ORDER  BY f.rank
                LIMIT  ?
                """,
                (query, limit),
            ).fetchall()
        except sqlite3.OperationalError:
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            if r["data_json"] is None:
                continue
            d = json.loads(r["data_json"])
            d["_source_path"] = r["source_path"]
            d["_rank"] = r["rank"]
            records.append(d)
        return pd.DataFrame(records)

    def sql_query(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        try:
            return pd.read_sql_query(sql, self.conn, params=params)
        except Exception as exc:
            raise RuntimeError(f"SQL error: {exc}") from exc

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM _dp_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.transaction() as c:
            c.execute(
                "INSERT INTO _dp_meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def pipeline_stats(self, pipeline: str) -> dict:
        row = self.conn.execute(
            """
            SELECT COUNT(*)       as file_count,
                   SUM(row_count) as total_rows,
                   MIN(indexed_at) as first_indexed,
                   MAX(indexed_at) as last_indexed
            FROM _dp_files WHERE pipeline = ?
            """,
            (pipeline,),
        ).fetchone()
        return dict(row) if row else {}

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# ---------------------------------------------------------------------------
# Delta computation (Pandas outer-merge, same as DeltaContext)
# ---------------------------------------------------------------------------

def compute_delta(current_df: pd.DataFrame, db: Store, pipeline: str) -> FileDelta:
    """
    Compare a live scan DataFrame against the DB state.

    current_df columns: file_path, content_hash, mtime, file_type
    """
    db_records = db.all_file_states(pipeline)
    if db_records:
        db_df = pd.DataFrame(db_records)[["file_path", "content_hash"]].rename(
            columns={"content_hash": "db_hash"}
        )
    else:
        db_df = pd.DataFrame(columns=["file_path", "db_hash"])

    if current_df.empty:
        deleted = list(db_df["file_path"]) if not db_df.empty else []
        return FileDelta(deleted=deleted)

    merged = pd.merge(current_df, db_df, on="file_path", how="outer", indicator=True)

    added_mask   = merged["_merge"] == "left_only"
    deleted_mask = merged["_merge"] == "right_only"
    both_mask    = merged["_merge"] == "both"
    changed_mask = both_mask & (merged["content_hash"] != merged["db_hash"])

    def _rows(mask) -> list[dict]:
        cols = [c for c in ["file_path", "content_hash", "mtime", "file_type"] if c in merged.columns]
        return merged.loc[mask, cols].to_dict(orient="records")

    delta = FileDelta(
        added    = _rows(added_mask),
        modified = _rows(changed_mask),
        deleted  = merged.loc[deleted_mask, "file_path"].tolist(),
    )
    logger.info("Δ %r", delta)
    return delta


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

# Supported extensions → file_type tag
EXTENSION_MAP: dict[str, str] = {
    ".csv":  "csv",
    ".tsv":  "csv",
    ".json": "json",
    ".jsonl":"json",
    ".txt":  "text",
    ".md":   "text",
    ".rst":  "text",
    ".markdown": "text",
    ".xlsx": "excel",
    ".xls":  "excel",
    ".py":   "python",
    ".pdf":  "pdf",
    ".html": "html",
    ".htm":  "html",
    ".docx": "docx",
    ".png":  "image",
    ".jpg":  "image",
    ".jpeg": "image",
}

TransformFn = Callable[[Path], pd.DataFrame | None]


class Pipeline:
    """
    Declarative incremental indexing pipeline.

    Usage
    -----
    pipe = Pipeline("docs", store)
    pipe.source("./data", patterns=["*.csv", "*.md"])
    pipe.transform(auto_transform)   # or a custom fn
    pipe.columns(["title", "body"])
    stats = pipe.run()
    df = pipe.search("machine learning")
    """

    def __init__(self, name: str, store: Store) -> None:
        self.name = name
        self.store = store
        self._source_dirs: list[tuple[Path, list[str]]] = []
        self._transform: TransformFn | None = None
        self._columns: list[str] = []
        self._table = f"dp_{name}"

    # -- builder API ---------------------------------------------------------

    def source(self, directory: str | Path, patterns: list[str] | None = None) -> "Pipeline":
        self._source_dirs.append((Path(directory), patterns or ["*"]))
        return self

    def transform(self, fn: TransformFn) -> "Pipeline":
        self._transform = fn
        return self

    def columns(self, cols: list[str]) -> "Pipeline":
        self._columns = cols
        return self

    # -- scan ----------------------------------------------------------------

    def _scan(self) -> pd.DataFrame:
        records = []
        for directory, patterns in self._source_dirs:
            if not directory.exists():
                logger.warning("Source dir not found: %s", directory)
                continue
            for pattern in patterns:
                for path in sorted(directory.rglob(pattern)):
                    if not path.is_file():
                        continue
                    ext = path.suffix.lower()
                    ftype = EXTENSION_MAP.get(ext, "unknown")
                    try:
                        src = SourceFile(
                            path=path,
                            size=path.stat().st_size,
                            mtime=path.stat().st_mtime,
                            file_type=ftype,
                        )
                        src.compute_hash()
                        records.append({
                            "file_path":    str(path),
                            "content_hash": src.content_hash,
                            "mtime":        src.mtime,
                            "file_type":    ftype,
                        })
                    except OSError as exc:
                        logger.warning("Cannot stat %s: %s", path, exc)
        if not records:
            return pd.DataFrame(columns=["file_path", "content_hash", "mtime", "file_type"])
        return pd.DataFrame(records)

    # -- run -----------------------------------------------------------------

    def run(self, *, force: bool = False) -> IndexStats:
        """Incremental index: only processes files whose hash changed."""
        if not self._transform:
            raise RuntimeError("No transform set — call .transform(fn) first.")
        if not self._columns:
            raise RuntimeError("No columns declared — call .columns([...]) first.")

        self.store.ensure_data_table(self._table, self._columns)

        t0 = time.monotonic()
        stats = IndexStats()

        current_df = self._scan()
        stats.total_files = len(current_df)

        delta = compute_delta(current_df, self.store, self.name)

        if force:
            # treat everything as modified
            delta.added.extend(delta.modified)
            delta.modified = []
            for rec in current_df.to_dict(orient="records"):
                if rec["file_path"] not in [d["file_path"] for d in delta.added]:
                    delta.added.append(rec)

        # Process new + changed
        for rec in delta.added + delta.modified:
            path = Path(rec["file_path"])
            is_new = rec in delta.added
            logger.info("%s %s", "NEW" if is_new else "UPD", path.name)
            try:
                df = self._transform(path)
            except Exception as exc:
                msg = f"{path.name}: {exc}"
                logger.error("Transform error: %s", msg)
                stats.errors.append(msg)
                continue

            if df is None or df.empty:
                stats.skipped_files += 1
                continue

            self.store.delete_rows_for_source(self._table, str(path))
            rows = df.to_dict(orient="records")
            self.store.insert_rows(self._table, str(path), rows, self._columns)
            self.store.upsert_file_state(
                str(path), self.name,
                rec.get("content_hash", ""),
                rec.get("mtime", 0.0),
                rec.get("file_type", ""),
                len(rows),
            )
            stats.total_rows_indexed += len(rows)
            if is_new:
                stats.new_files += 1
            else:
                stats.updated_files += 1

        # Remove deleted
        for fpath in delta.deleted:
            logger.info("DEL %s", fpath)
            self.store.delete_rows_for_source(self._table, fpath)
            self.store.delete_file_state(fpath)
            stats.deleted_files += 1

        # Count truly skipped (unchanged)
        stats.skipped_files = (
            stats.total_files - stats.new_files - stats.updated_files - len(stats.errors)
        )

        stats.duration_seconds = time.monotonic() - t0
        self.store.set_meta(f"{self.name}:last_run", str(time.time()))
        return stats

    # -- query ---------------------------------------------------------------

    def search(self, query: str, limit: int = 20) -> pd.DataFrame:
        return self.store.search(self._table, query, limit)

    def sql(self, sql: str) -> pd.DataFrame:
        return self.store.sql_query(sql)

    def stats(self) -> dict:
        return self.store.pipeline_stats(self.name)
