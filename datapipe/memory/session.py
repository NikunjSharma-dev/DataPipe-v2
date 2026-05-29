"""
datapipe/memory/session.py — Session memory.

Logs every tool call and file mutation to SQLite so sessions survive
process restarts and context truncation.

Usage
-----
mem = SessionMemory(store)
mem.ensure_session("my-session")
mem.log_event("my-session", "search", {"query": "AAPL"}, duration_ms=12)
print(mem.get_resume_snapshot("my-session"))
"""

from __future__ import annotations

import datetime
import functools
import json
import logging
import time
from typing import Any, Callable

from datapipe.engine import Store

logger = logging.getLogger("datapipe.memory")


class SessionMemory:
    """CRUD interface over sessions / tool_calls / file_edits tables."""

    def __init__(self, store: Store) -> None:
        self.store = store
        self._bootstrap()

    def _bootstrap(self) -> None:
        with self.store.transaction() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_key TEXT    NOT NULL UNIQUE,
                    started_at  REAL    NOT NULL DEFAULT (unixepoch('now')),
                    last_active REAL    NOT NULL DEFAULT (unixepoch('now')),
                    summary     TEXT
                );
                CREATE TABLE IF NOT EXISTS tool_calls (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_key TEXT    NOT NULL,
                    tool_name   TEXT    NOT NULL,
                    arguments   TEXT,
                    result      TEXT,
                    called_at   REAL    NOT NULL DEFAULT (unixepoch('now')),
                    duration_ms INTEGER,
                    FOREIGN KEY (session_key) REFERENCES sessions(session_key)
                );
                CREATE TABLE IF NOT EXISTS file_edits (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_key TEXT    NOT NULL,
                    file_path   TEXT    NOT NULL,
                    action      TEXT    NOT NULL
                                    CHECK(action IN ('added','updated','deleted')),
                    old_hash    TEXT,
                    new_hash    TEXT,
                    edited_at   REAL    NOT NULL DEFAULT (unixepoch('now')),
                    FOREIGN KEY (session_key) REFERENCES sessions(session_key)
                );
            """)

    # -- session lifecycle ---------------------------------------------------

    def ensure_session(self, session_key: str) -> None:
        with self.store.transaction() as c:
            c.execute(
                """
                INSERT INTO sessions (session_key)
                VALUES (?)
                ON CONFLICT(session_key) DO UPDATE SET
                    last_active = unixepoch('now')
                """,
                (session_key,),
            )

    def store_summary(self, session_key: str, summary: str) -> None:
        self.ensure_session(session_key)
        with self.store.transaction() as c:
            c.execute(
                "UPDATE sessions SET summary=? WHERE session_key=?",
                (summary, session_key),
            )

    # -- event logging -------------------------------------------------------

    def log_event(
        self,
        session_key: str,
        tool_name: str,
        arguments: dict | None = None,
        result: Any = None,
        duration_ms: int | None = None,
    ) -> None:
        self.ensure_session(session_key)
        with self.store.transaction() as c:
            c.execute(
                """
                INSERT INTO tool_calls
                    (session_key, tool_name, arguments, result, duration_ms)
                VALUES (?,?,?,?,?)
                """,
                (
                    session_key,
                    tool_name,
                    json.dumps(arguments) if arguments else None,
                    json.dumps(result, default=str) if result is not None else None,
                    duration_ms,
                ),
            )

    def log_file_edit(
        self,
        session_key: str,
        file_path: str,
        action: str,
        old_hash: str | None = None,
        new_hash: str | None = None,
    ) -> None:
        self.ensure_session(session_key)
        with self.store.transaction() as c:
            c.execute(
                """
                INSERT INTO file_edits
                    (session_key, file_path, action, old_hash, new_hash)
                VALUES (?,?,?,?,?)
                """,
                (session_key, file_path, action, old_hash, new_hash),
            )

    # -- resume snapshot -----------------------------------------------------

    def get_resume_snapshot(self, session_key: str, max_events: int = 20) -> str:
        """
        Compile a compact plain-text block for LLM context injection.
        Returns an empty string if the session is not found.
        """
        conn = self.store.conn
        session = conn.execute(
            "SELECT * FROM sessions WHERE session_key=?", (session_key,)
        ).fetchone()
        if not session:
            return ""

        lines: list[str] = [
            "## DataPipe Session Snapshot",
            f"Session     : {session_key}",
            f"Last active : {_fmt_ts(session['last_active'])}",
            f"Summary     : {session['summary'] or '(none)'}",
            "",
        ]

        calls = conn.execute(
            """
            SELECT tool_name, arguments, duration_ms
            FROM   tool_calls
            WHERE  session_key=?
            ORDER  BY called_at DESC LIMIT ?
            """,
            (session_key, max_events),
        ).fetchall()
        if calls:
            lines.append(f"### Recent tool calls (last {len(calls)})")
            for i, c in enumerate(reversed(calls), 1):
                ms = f"{c['duration_ms']}ms" if c["duration_ms"] else "?"
                lines.append(f"  {i}. {c['tool_name']}({c['arguments'] or '{}'}) → {ms}")
            lines.append("")

        edits = conn.execute(
            """
            SELECT action, file_path
            FROM   file_edits
            WHERE  session_key=?
            ORDER  BY edited_at DESC LIMIT 20
            """,
            (session_key,),
        ).fetchall()
        if edits:
            lines.append("### File activity")
            for e in reversed(edits):
                lines.append(f"  - {e['action'].upper():<8} {e['file_path']}")
            lines.append("")

        return "\n".join(lines)


# -- decorator ---------------------------------------------------------------

def log_tool_call(session_key: str, memory: SessionMemory) -> Callable:
    """
    Decorator factory: wrap a function to auto-log call + timing to session memory.

    Usage
    -----
    @log_tool_call("my-session", mem)
    def ctx_search(query: str) -> str:
        ...
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            t0 = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                duration = int((time.monotonic() - t0) * 1000)
                memory.log_event(
                    session_key, fn.__name__,
                    arguments={"args": str(args)[:200], **{k: str(v)[:100] for k, v in kwargs.items()}},
                    duration_ms=duration,
                )
                return result
            except Exception as exc:
                duration = int((time.monotonic() - t0) * 1000)
                memory.log_event(
                    session_key, fn.__name__,
                    arguments={"args": str(args)[:200]},
                    result={"error": str(exc)},
                    duration_ms=duration,
                )
                raise
        return wrapper
    return decorator


def _fmt_ts(unix_ts: float | None) -> str:
    if not unix_ts:
        return "(unknown)"
    return datetime.datetime.fromtimestamp(unix_ts).strftime("%Y-%m-%d %H:%M:%S")
