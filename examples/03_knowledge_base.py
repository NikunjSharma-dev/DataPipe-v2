"""
Example 3 — Knowledge Base Pipeline  (multi-source + custom transform)
=======================================================================
Indexes Markdown docs by splitting them into searchable chunks.
Demonstrates combining multiple source directories and a custom transform.

Usage:
    datapipe update  examples/03_knowledge_base.py
    datapipe search  examples/03_knowledge_base.py "SQLite WAL mode production"
    datapipe search  examples/03_knowledge_base.py "incremental pipeline delta"
    datapipe search  examples/03_knowledge_base.py "error handling resilient"
    datapipe stats   examples/03_knowledge_base.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datapipe import Pipeline, Store

store = Store("./datapipe_index.db")


# ── Custom transform ────────────────────────────────────────────────────────
# Split markdown by ## headings, keeping section title + body together.
# Returns a DataFrame with columns: section, text, char_count.

def markdown_section_transform(path: Path) -> pd.DataFrame | None:
    text = path.read_text(encoding="utf-8", errors="replace")
    sections = []
    current_title = "(intro)"
    current_body: list[str] = []

    for line in text.splitlines():
        if line.startswith("## "):
            if current_body:
                body = " ".join(current_body).strip()
                sections.append({
                    "section": current_title,
                    "text": body,
                    "char_count": str(len(body)),
                })
            current_title = line[3:].strip()
            current_body = []
        else:
            current_body.append(line.strip())

    if current_body:
        body = " ".join(current_body).strip()
        sections.append({
            "section": current_title,
            "text": body,
            "char_count": str(len(body)),
        })

    return pd.DataFrame(sections) if sections else None


# ── Pipeline ────────────────────────────────────────────────────────────────

pipe = (
    Pipeline("knowledge_base", store)
    .source(
        Path(__file__).parent / "data",
        patterns=["*.md", "*.txt"],
    )
    .transform(markdown_section_transform)
    .columns(["section", "text", "char_count"])
)
