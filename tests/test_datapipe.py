"""
Tests for DataPipe v2 — engine, parsers, delta, session memory.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import pytest

from datapipe.engine import Pipeline, Store, compute_delta
from datapipe.parsers.csv_parser import CSVParser
from datapipe.parsers.json_parser import JSONParser
from datapipe.parsers.text_parser import TextParser
from datapipe.parsers.python_parser import PythonParser
from datapipe.parsers.excel_parser import ExcelParser
from datapipe.parsers import auto_transform
from datapipe.memory.session import SessionMemory


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def csv_file(tmp_path) -> Path:
    f = tmp_path / "data.csv"
    f.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\n")
    return f


# ── Store ─────────────────────────────────────────────────────────────────────

class TestStore:
    def test_bootstrap_creates_meta_table(self, store):
        cur = store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = {r[0] for r in cur.fetchall()}
        assert "_dp_files" in names and "_dp_meta" in names

    def test_file_state_roundtrip(self, store):
        store.upsert_file_state("/a.csv", "pipe", "abc", 1.0, "csv", 10)
        rec = store.get_file_state("/a.csv")
        assert rec["content_hash"] == "abc"
        assert rec["row_count"] == 10

    def test_file_state_upsert(self, store):
        store.upsert_file_state("/a.csv", "pipe", "old", 1.0, "csv", 5)
        store.upsert_file_state("/a.csv", "pipe", "new", 2.0, "csv", 8)
        assert store.get_file_state("/a.csv")["content_hash"] == "new"

    def test_delete_file_state(self, store):
        store.upsert_file_state("/a.csv", "p", "x", 1.0, "csv", 1)
        store.delete_file_state("/a.csv")
        assert store.get_file_state("/a.csv") is None

    def test_meta_roundtrip(self, store):
        store.set_meta("k", "v1")
        assert store.get_meta("k") == "v1"
        store.set_meta("k", "v2")
        assert store.get_meta("k") == "v2"

    def test_fts5_search(self, store):
        store.ensure_data_table("dp_t", ["title", "body"])
        store.insert_rows("dp_t", "/f.txt",
            [{"title": "Python Guide", "body": "Learn Python programming fast"}],
            ["title", "body"])
        df = store.search("dp_t", "Python")
        assert len(df) >= 1

    def test_delete_rows_removes_from_fts(self, store):
        store.ensure_data_table("dp_d", ["text"])
        store.insert_rows("dp_d", "/a.txt", [{"text": "hello world"}], ["text"])
        store.insert_rows("dp_d", "/b.txt", [{"text": "foo bar baz"}], ["text"])
        store.delete_rows_for_source("dp_d", "/a.txt")
        assert store.search("dp_d", "hello").empty
        assert len(store.search("dp_d", "foo")) >= 1

    def test_sql_query(self, store):
        store.ensure_data_table("dp_s", ["name"])
        store.insert_rows("dp_s", "/x.csv", [{"name": "alice"}, {"name": "bob"}], ["name"])
        df = store.sql_query("SELECT source_path, data_json FROM dp_s_rows")
        assert len(df) == 2


# ── Delta computation ─────────────────────────────────────────────────────────

class TestComputeDelta:
    def _current(self, records: list[dict]) -> pd.DataFrame:
        return pd.DataFrame(records, columns=["file_path", "content_hash", "mtime", "file_type"])

    def test_all_new(self, store):
        cur = self._current([{"file_path": "/a.csv", "content_hash": "h1", "mtime": 1.0, "file_type": "csv"}])
        delta = compute_delta(cur, store, "test")
        assert len(delta.added) == 1
        assert delta.modified == []
        assert delta.deleted == []

    def test_unchanged_skipped(self, store):
        store.upsert_file_state("/a.csv", "test", "h1", 1.0, "csv", 1)
        cur = self._current([{"file_path": "/a.csv", "content_hash": "h1", "mtime": 1.0, "file_type": "csv"}])
        delta = compute_delta(cur, store, "test")
        assert delta.is_empty()

    def test_changed_detected(self, store):
        store.upsert_file_state("/a.csv", "test", "old_hash", 1.0, "csv", 1)
        cur = self._current([{"file_path": "/a.csv", "content_hash": "new_hash", "mtime": 2.0, "file_type": "csv"}])
        delta = compute_delta(cur, store, "test")
        assert len(delta.modified) == 1
        assert delta.added == []

    def test_deleted_detected(self, store):
        store.upsert_file_state("/gone.csv", "test", "h1", 1.0, "csv", 1)
        cur = self._current([])
        delta = compute_delta(cur, store, "test")
        assert "/gone.csv" in delta.deleted


# ── Parsers ───────────────────────────────────────────────────────────────────

class TestCSVParser:
    def test_basic(self, tmp_path):
        f = tmp_path / "x.csv"
        f.write_text("name,age\nAlice,30\nBob,25\n")
        df = CSVParser()(f)
        assert df is not None and len(df) == 2

    def test_sanitizes_columns(self, tmp_path):
        f = tmp_path / "x.csv"
        f.write_text("Product Name,Unit Price\nWidget,9.99\n")
        df = CSVParser()(f)
        assert "product_name" in df.columns and "unit_price" in df.columns

    def test_tsv(self, tmp_path):
        f = tmp_path / "x.tsv"
        f.write_text("a\tb\n1\t2\n3\t4\n")
        df = CSVParser()(f)
        assert df is not None and len(df) == 2

    def test_empty_returns_none(self, tmp_path):
        f = tmp_path / "x.csv"
        f.write_text("name,age\n")
        df = CSVParser()(f)
        assert df is None or df.empty


class TestJSONParser:
    def test_array(self, tmp_path):
        f = tmp_path / "x.json"
        f.write_text(json.dumps([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]))
        df = JSONParser()(f)
        assert df is not None and len(df) == 2

    def test_single_object(self, tmp_path):
        f = tmp_path / "x.json"
        f.write_text(json.dumps({"key": "val"}))
        df = JSONParser()(f)
        assert df is not None and len(df) == 1

    def test_jsonl(self, tmp_path):
        f = tmp_path / "x.jsonl"
        f.write_text('{"a":1}\n{"a":2}\n{"a":3}\n')
        df = JSONParser()(f)
        assert df is not None and len(df) == 3


class TestTextParser:
    def test_produces_chunks(self, tmp_path):
        f = tmp_path / "x.txt"
        f.write_text("hello world " * 500)
        df = TextParser(chunk_size=200)(f)
        assert df is not None and len(df) >= 2
        assert "text" in df.columns and "chunk_index" in df.columns

    def test_empty_returns_none(self, tmp_path):
        f = tmp_path / "x.txt"
        f.write_text("   ")
        assert TextParser()(f) is None


class TestPythonParser:
    def test_parses_functions(self, tmp_path):
        f = tmp_path / "sample.py"
        f.write_text(
            '"""Module docstring."""\n\ndef add(a: int, b: int) -> int:\n    """Add two numbers."""\n    return a + b\n'
        )
        df = PythonParser()(f)
        assert df is not None and len(df) >= 1
        combined = " ".join(df["text"].tolist())
        assert "add" in combined or "Module" in combined

    def test_fallback_on_syntax_error(self, tmp_path):
        f = tmp_path / "bad.py"
        f.write_text("def broken(:\n    pass\n")
        df = PythonParser()(f)
        # should still produce something via text fallback
        assert df is not None and len(df) >= 1


class TestAutoTransform:
    def test_csv(self, tmp_path):
        f = tmp_path / "x.csv"
        f.write_text("a,b\n1,2\n")
        assert auto_transform(f) is not None

    def test_json(self, tmp_path):
        f = tmp_path / "x.json"
        f.write_text('[{"x":1}]')
        assert auto_transform(f) is not None

    def test_md(self, tmp_path):
        f = tmp_path / "x.md"
        f.write_text("# Title\n\ncontent here " * 20)
        assert auto_transform(f) is not None

    def test_py(self, tmp_path):
        f = tmp_path / "x.py"
        f.write_text('"""Module."""\ndef foo(): pass\n')
        assert auto_transform(f) is not None

    def test_unknown_returns_none(self, tmp_path):
        f = tmp_path / "x.xyz"
        f.write_text("data")
        assert auto_transform(f) is None


# ── Pipeline integration ──────────────────────────────────────────────────────

class TestPipeline:
    def _csv(self, directory: Path, name: str, rows: list[dict]) -> Path:
        f = directory / name
        if rows:
            headers = list(rows[0].keys())
            lines = [",".join(headers)]
            for r in rows:
                lines.append(",".join(str(r[h]) for h in headers))
            f.write_text("\n".join(lines) + "\n")
        return f

    def _pipe(self, store, src_dir):
        return (
            Pipeline("test", store)
            .source(src_dir, patterns=["*.csv"])
            .transform(CSVParser())
            .columns(["title", "body"])
        )

    def test_first_run_indexes_all(self, store, tmp_path):
        self._csv(tmp_path, "a.csv", [{"title": "A", "body": "hello"}])
        stats = self._pipe(store, tmp_path).run()
        assert stats.new_files == 1 and stats.total_rows_indexed == 1

    def test_second_run_skips_unchanged(self, store, tmp_path):
        self._csv(tmp_path, "a.csv", [{"title": "A", "body": "hello"}])
        pipe = self._pipe(store, tmp_path)
        pipe.run()
        stats = pipe.run()
        assert stats.new_files == 0 and stats.updated_files == 0

    def test_update_on_content_change(self, store, tmp_path):
        f = tmp_path / "a.csv"
        f.write_text("title,body\nOld,old content\n")
        pipe = self._pipe(store, tmp_path)
        pipe.run()
        f.write_text("title,body\nNew,new content\n")
        stats = pipe.run()
        assert stats.updated_files == 1
        assert pipe.search("old content").empty
        assert len(pipe.search("new content")) >= 1

    def test_deleted_file_removed(self, store, tmp_path):
        f = self._csv(tmp_path, "gone.csv", [{"title": "X", "body": "will vanish"}])
        pipe = self._pipe(store, tmp_path)
        pipe.run()
        f.unlink()
        stats = pipe.run()
        assert stats.deleted_files == 1
        assert pipe.search("vanish").empty

    def test_force_reindex(self, store, tmp_path):
        self._csv(tmp_path, "a.csv", [{"title": "T", "body": "B"}])
        pipe = self._pipe(store, tmp_path)
        pipe.run()
        stats = pipe.run(force=True)
        assert stats.new_files + stats.updated_files >= 1

    def test_search_returns_results(self, store, tmp_path):
        self._csv(tmp_path, "docs.csv", [
            {"title": "Python", "body": "Learn Python programming language"},
            {"title": "SQL",    "body": "Database query optimization"},
        ])
        pipe = self._pipe(store, tmp_path)
        pipe.run()
        df = pipe.search("Python programming")
        assert len(df) >= 1 and "_source_path" in df.columns

    def test_pipeline_stats(self, store, tmp_path):
        self._csv(tmp_path, "a.csv", [{"title": "X", "body": "Y"}, {"title": "Z", "body": "W"}])
        pipe = self._pipe(store, tmp_path)
        pipe.run()
        s = pipe.stats()
        assert s["file_count"] == 1 and s["total_rows"] == 2

    def test_multiple_source_dirs(self, store, tmp_path):
        d1, d2 = tmp_path / "src1", tmp_path / "src2"
        d1.mkdir(); d2.mkdir()
        self._csv(d1, "a.csv", [{"title": "A", "body": "alpha"}])
        self._csv(d2, "b.csv", [{"title": "B", "body": "beta"}])
        pipe = (
            Pipeline("multi", store)
            .source(d1, ["*.csv"]).source(d2, ["*.csv"])
            .transform(CSVParser()).columns(["title", "body"])
        )
        stats = pipe.run()
        assert stats.total_files == 2 and stats.total_rows_indexed == 2

    def test_bad_transform_continues(self, store, tmp_path):
        self._csv(tmp_path, "a.csv", [{"title": "T", "body": "B"}])
        def bad_transform(path):
            raise ValueError("deliberate error")
        pipe = (
            Pipeline("err", store)
            .source(tmp_path, ["*.csv"])
            .transform(bad_transform)
            .columns(["title", "body"])
        )
        stats = pipe.run()
        assert len(stats.errors) >= 1  # error logged, pipeline didn't crash


# ── Session memory ────────────────────────────────────────────────────────────

class TestSessionMemory:
    def test_ensure_creates_session(self, store):
        mem = SessionMemory(store)
        mem.ensure_session("s1")
        row = store.conn.execute("SELECT * FROM sessions WHERE session_key='s1'").fetchone()
        assert row is not None

    def test_log_event_stored(self, store):
        mem = SessionMemory(store)
        mem.log_event("s1", "ctx_search", {"query": "test"}, duration_ms=42)
        row = store.conn.execute("SELECT * FROM tool_calls WHERE session_key='s1'").fetchone()
        assert row["tool_name"] == "ctx_search" and row["duration_ms"] == 42

    def test_log_file_edit(self, store):
        mem = SessionMemory(store)
        mem.log_file_edit("s1", "/data/file.csv", "added", new_hash="abc123")
        row = store.conn.execute("SELECT * FROM file_edits WHERE session_key='s1'").fetchone()
        assert row["action"] == "added"

    def test_resume_snapshot_contains_events(self, store):
        mem = SessionMemory(store)
        mem.ensure_session("s2")
        mem.log_event("s2", "search", {"q": "AAPL"}, duration_ms=10)
        mem.log_file_edit("s2", "/f.csv", "added")
        snap = mem.get_resume_snapshot("s2")
        assert "search" in snap
        assert "/f.csv" in snap
        assert "DataPipe Session Snapshot" in snap

    def test_store_summary(self, store):
        mem = SessionMemory(store)
        mem.ensure_session("s3")
        mem.store_summary("s3", "Analysed Q1 data")
        snap = mem.get_resume_snapshot("s3")
        assert "Analysed Q1 data" in snap

    def test_unknown_session_returns_empty(self, store):
        mem = SessionMemory(store)
        assert mem.get_resume_snapshot("no-such-key") == ""
