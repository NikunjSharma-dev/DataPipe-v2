"""
datapipe CLI v2

Commands
--------
datapipe update  <config> [-L] [-f]   Run indexing (--live watches after)
datapipe search  <config> <query>     Full-text BM25 search
datapipe sql     <config> <sql>       Raw SQL query
datapipe stats   <config>             Pipeline statistics
datapipe session <config> <key>       Print session resume snapshot
datapipe doctor                       Sanity-check environment
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
from pathlib import Path


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-7s  %(name)s — %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")


def _load_config(config_path: str):
    path = Path(config_path).resolve()
    if not path.exists():
        print(f"Error: config not found: {path}", file=sys.stderr)
        sys.exit(1)
    spec = importlib.util.spec_from_file_location("_dp_cfg", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "pipe"):
        print("Error: config must define a `pipe` variable (datapipe.Pipeline).", file=sys.stderr)
        sys.exit(1)
    return mod.pipe


# ---------------------------------------------------------------------------

def cmd_update(args) -> None:
    _setup_logging(args.verbose)
    pipe = _load_config(args.config)
    print(f"[datapipe] Pipeline: {pipe.name}")
    stats = pipe.run(force=args.force)
    print(f"\n[datapipe] Done:\n{stats.summary()}")
    if stats.errors:
        print("\n[datapipe] Errors:")
        for e in stats.errors:
            print(f"  • {e}")
    if args.live:
        from datapipe.watcher import FileWatcher
        print(f"\n[datapipe] Live mode — Ctrl+C to stop")
        FileWatcher(pipe).watch_until_signal()


def cmd_search(args) -> None:
    _setup_logging(False)
    pipe = _load_config(args.config)
    import pandas as pd
    pd.set_option("display.max_colwidth", 80)
    query = " ".join(args.query)
    print(f"[datapipe] Searching '{pipe.name}' for: {query!r}\n")
    results = pipe.search(query, limit=args.limit)
    if results.empty:
        print("No results found.")
    else:
        display = results.drop(columns=["_rank"], errors="ignore")
        print(display.to_string(index=False))
        print(f"\n{len(results)} result(s).")


def cmd_sql(args) -> None:
    _setup_logging(False)
    pipe = _load_config(args.config)
    import pandas as pd
    pd.set_option("display.max_colwidth", 80)
    sql = " ".join(args.sql)
    print(f"[datapipe] SQL: {sql}\n")
    try:
        df = pipe.sql(sql)
        print(df.to_string(index=False) if not df.empty else "(empty)")
        if not df.empty:
            print(f"\n{len(df)} row(s).")
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


def cmd_stats(args) -> None:
    _setup_logging(False)
    pipe = _load_config(args.config)
    s = pipe.stats()
    last_run = pipe.store.get_meta(f"{pipe.name}:last_run")
    import datetime
    ts = (
        datetime.datetime.fromtimestamp(float(last_run)).strftime("%Y-%m-%d %H:%M:%S")
        if last_run else "never"
    )
    print(f"Pipeline   : {pipe.name}")
    print(f"Database   : {pipe.store.db_path}")
    print(f"Last run   : {ts}")
    print(f"Files      : {s.get('file_count', 0)}")
    print(f"Total rows : {s.get('total_rows', 0)}")


def cmd_session(args) -> None:
    _setup_logging(False)
    pipe = _load_config(args.config)
    from datapipe.memory import SessionMemory
    mem = SessionMemory(pipe.store)
    snapshot = mem.get_resume_snapshot(args.session_key)
    if snapshot:
        print(snapshot)
    else:
        print(f"No session found for key: {args.session_key!r}")


def cmd_doctor(args) -> None:
    _setup_logging(False)
    import sqlite3
    print("=== DataPipe Doctor v2 ===\n")

    v = sys.version_info
    print(f"[{'x' if v >= (3,9) else ' '}] Python {v.major}.{v.minor}.{v.micro}  (≥3.9 required)")

    for pkg, note in [
        ("pandas",   "core"),
        ("xxhash",   "fast hashing"),
        ("watchdog", "real-time watching"),
        ("openpyxl", "Excel"),
        ("pypdf",    "PDF  — optional"),
        ("bs4",      "HTML — optional (beautifulsoup4)"),
        ("docx",     "DOCX — optional (python-docx)"),
        ("pytesseract", "OCR — optional"),
        ("libcst",   "Python AST — optional"),
    ]:
        try:
            mod = __import__(pkg)
            ver = getattr(mod, "__version__", "?")
            print(f"[x] {pkg} {ver}  ({note})")
        except ImportError:
            mark = " " if "optional" in note else "!"
            print(f"[{mark}] {pkg}  — NOT FOUND  ({note})")

    # SQLite FTS5
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(col, tokenize='porter ascii')")
        print("[x] SQLite FTS5 + Porter stemming — available")
    except sqlite3.OperationalError:
        print("[!] SQLite FTS5 — NOT AVAILABLE (search won't work)")
    finally:
        conn.close()

    print("\nDoctor done.")


# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="datapipe",
        description="Incremental multimodal indexing — Python + pandas + SQLite",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("update",  help="Run the pipeline")
    p.add_argument("config")
    p.add_argument("-L", "--live",  action="store_true")
    p.add_argument("-f", "--force", action="store_true")
    p.set_defaults(func=cmd_update)

    p = sub.add_parser("search", help="Full-text search")
    p.add_argument("config")
    p.add_argument("query", nargs="+")
    p.add_argument("-n", "--limit", type=int, default=20)
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("sql",   help="Raw SQL query")
    p.add_argument("config")
    p.add_argument("sql", nargs="+")
    p.set_defaults(func=cmd_sql)

    p = sub.add_parser("stats", help="Pipeline statistics")
    p.add_argument("config")
    p.set_defaults(func=cmd_stats)

    p = sub.add_parser("session", help="Print session resume snapshot")
    p.add_argument("config")
    p.add_argument("session_key")
    p.set_defaults(func=cmd_session)

    p = sub.add_parser("doctor", help="Sanity check environment")
    p.set_defaults(func=cmd_doctor)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)
    args.func(args)


if __name__ == "__main__":
    main()
