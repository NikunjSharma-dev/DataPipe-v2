# DataPipe v2

**Incremental multimodal data indexing — Python + pandas + SQLite FTS5.**

Watches a directory of files (CSV, JSON, Markdown, Python, PDF, HTML, DOCX, images) and keeps a SQLite FTS5 search index always in sync. Only changed files are re-processed. A pluggable parser registry + session memory make it easy to extend.

```
[ Local Files ] ──→ [ Delta Engine ] ──→ [ SQLite FTS5 DB ] ──→ [ Search / SQL / MCP ]
  CSV, JSON, MD       xxHash fingerprint   Porter stemming         BM25 ranked results
  Python, DOCX        Pandas outer-merge   Session memory log      CLI · Python API
  PDF, HTML, Images   watchdog watcher     Resume snapshots
```

---

## What's new in v2 (upgrade from DeltaContext)

| Feature | v2 (this project) |
|---|---|
| Hashing | **xxHash xxh64** (~10× faster) |
| Delta computation | **Pandas outer-merge** (same as DeltaContext) |
| File watching |**watchdog** (inotify / FSEvents / kqueue) |
| Parsers | **Plugin registry** — Strategy pattern |
| Python files | **libcst AST-aware** chunking (class/func/doc) |
| PDF / HTML / DOCX | ✓ (optional deps, lazy-loaded) |
| Image OCR | ✓ Tesseract (optional) |
| Session memory | ✓ SQLite log + resume snapshots |
| FTS5 stemming | **porter ascii** |
| Tests | **43** |

---

## Install
### Option A: Local Installation
```Bash
git clone https://github.com/your-handle/datapipe.git
cd datapipe

python -m venv .venv
source .venv/bin/activate

# Core (CSV, JSON, Markdown, Python, Excel)
pip install pandas xxhash watchdog openpyxl


# Optional: richer parsing
pip install pypdf beautifulsoup4 python-docx pytesseract Pillow libcst
```
### Option B: Docker (Recommended for OCR)
Docker provides a pre-configured environment with all system dependencies (like Tesseract OCR) and Python libraries ready to go.

```Bash
git clone https://github.com/your-handle/datapipe.git
cd datapipe

# Build and start the container in the background
docker-compose up -d --build
(When you are done working, you can stop the container with docker-compose down)
```


---

## Quick start

### 1. Write a pipeline config

```python
# my_pipeline.py
from datapipe import Pipeline, Store
from datapipe.parsers import auto_transform

store = Store("./index.db")

pipe = (
    Pipeline("docs", store)
    .source("./data", patterns=["*.csv", "*.json", "*.md", "*.py"])
    .transform(auto_transform)
    .columns(["text", "chunk_index"])
)
```

### 2. Run it

```bash
# One-shot index
datapipe update my_pipeline.py

# Live mode — re-index whenever files change
datapipe update my_pipeline.py --live

# Force full re-index (ignore unchanged hashes)
datapipe update my_pipeline.py --force
```

### 3. Search

```bash
datapipe search my_pipeline.py "machine learning Python"
datapipe search my_pipeline.py "SQLite WAL mode" -n 5
```

### 4. SQL analytics

```bash
datapipe sql my_pipeline.py \
  "SELECT source_path, COUNT(*) as chunks FROM dp_docs_rows GROUP BY source_path"
```

### 5. Session memory

```python
from datapipe import Store
from datapipe.memory import SessionMemory

store = Store("./index.db")
mem   = SessionMemory(store)

mem.ensure_session("my-agent-session")
mem.log_event("my-agent-session", "search", {"query": "AAPL"}, duration_ms=12)
mem.log_file_edit("my-agent-session", "/data/equities.csv", "added")

print(mem.get_resume_snapshot("my-agent-session"))
```

```
## DataPipe Session Snapshot
Session     : my-agent-session
Last active : 2026-05-29 12:00:00
Summary     : (none)

### Recent tool calls (last 1)
  1. search({"query": "AAPL"}) → 12ms

### File activity
  - ADDED    /data/equities.csv
```

---

## Repository structure

```
datapipe/
├── datapipe/
│   ├── engine.py          # Store (SQLite), Pipeline, compute_delta, FileDelta
│   ├── watcher.py         # watchdog-based real-time watcher (+ polling fallback)
│   ├── transforms.py      # Backward-compat shim → parsers/
│   ├── cli.py             # datapipe CLI (update / search / sql / stats / session / doctor)
│   ├── parsers/
│   │   ├── __init__.py    # Plugin registry + auto_transform
│   │   ├── base.py        # BaseParser ABC + shared utilities
│   │   ├── csv_parser.py  # CSV / TSV
│   │   ├── json_parser.py # JSON / JSONL
│   │   ├── text_parser.py # Plain text / Markdown (chunked)
│   │   ├── python_parser.py # libcst AST-aware (class/func/doc chunks)
│   │   ├── excel_parser.py  # .xlsx / .xls
│   │   ├── pdf_parser.py    # pypdf (optional)
│   │   ├── html_parser.py   # bs4 (optional)
│   │   ├── docx_parser.py   # python-docx (optional)
│   │   └── image_parser.py  # Tesseract OCR (optional)
│   └── memory/
│       ├── __init__.py
│       └── session.py     # SessionMemory, log_tool_call decorator, resume snapshot
├── examples/
│   ├── data/              # Sample CSV, JSON, Markdown
│   ├── 01_products.py     # CSV catalog pipeline
│   ├── 02_employees.py    # JSON employee directory
│   └── 03_knowledge_base.py # Markdown section chunker
├── tests/
│   └── test_datapipe.py   # 43 tests: Store, Delta, Parsers, Pipeline, SessionMemory
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## CLI reference

```
datapipe update  <config.py> [-L] [-f]     Index pipeline; -L = live mode
datapipe search  <config.py> <query>       BM25 full-text search
datapipe sql     <config.py> <sql>         Raw SQL against the index DB
datapipe stats   <config.py>               File count, row count, last run
datapipe session <config.py> <key>         Print session resume snapshot
datapipe doctor                            Check all dependencies
```

---

## Adding a new parser

1. Create `datapipe/parsers/myformat.py` — subclass `BaseParser`, implement `extract(path) → DataFrame | None`.
2. Register in `datapipe/parsers/__init__.py`:
   ```python
   from datapipe.parsers.myformat import MyFormatParser
   PARSER_REGISTRY["myformat"] = MyFormatParser()
   ```
3. Map the extension in `datapipe/engine.py` `EXTENSION_MAP`:
   ```python
   ".xyz": "myformat",
   ```
4. Write tests in `tests/test_datapipe.py`.

---

## Architecture notes

**Why xxHash?** xxh64 is ~10× faster than SHA-256 and non-cryptographic — perfect for content fingerprinting. Collision probability is negligible for file-watching workloads.

**Why Pandas outer-merge for delta?** It's vectorised, readable, and identical to DeltaContext's approach — scanning millions of paths in a single merge instead of looping.

**Why standalone FTS5 (not `content=`)?** SQLite's `content=` aliased FTS5 tables require manual trigger maintenance for deletes. A standalone FTS5 table with a `source_path UNINDEXED` column lets us issue a simple `DELETE FROM fts WHERE source_path = ?` — no rowid tracking, no trigger bugs.

**Why watchdog?** It uses the OS's native file-system notification API (inotify on Linux, FSEvents on macOS, ReadDirectoryChangesW on Windows) — sub-second latency with zero CPU usage at rest. The polling fallback activates if watchdog isn't installed.

---

## Running tests

```bash
pytest                    # all 43 tests
pytest -v --tb=short      # verbose
pytest tests/ -k "Delta"  # filter by name
```

---

## License

Apache 2.0
