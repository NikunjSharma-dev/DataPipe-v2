"""
datapipe/parsers/python_parser.py — Python AST-aware parser.

Uses libcst to index each module / class / function as a separate searchable
chunk with its name, signature, docstring, and body. Falls back to plain-text
chunking when libcst is unavailable or the file has syntax errors.
"""

from __future__ import annotations

import logging
import textwrap
from pathlib import Path
from typing import Iterator

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class PythonParser(BaseParser):
    file_type = "python"

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            logger.error("PythonParser: cannot read %s — %s", path, exc)
            return None

        if not source.strip():
            return None

        chunks: list[str] = []
        try:
            import libcst as cst
            tree = cst.parse_module(source)
            visitor = _StructureVisitor(path.name)
            tree.walk(visitor)
            chunks = visitor.chunks
        except Exception as exc:
            logger.warning(
                "PythonParser: libcst failed for %s (%s) — text fallback", path.name, exc
            )
            header = f"File: {path.name} (Python source)\n\n"
            chunks = self.split_text(self.clean(header + source))

        if not chunks:
            return None

        records = [
            {
                "chunk_index": str(i),
                "text": self.clean(c),
                "char_count": str(len(c)),
            }
            for i, c in enumerate(chunks)
            if c.strip()
        ]
        return pd.DataFrame(records) if records else None


# ---------------------------------------------------------------------------
# libcst visitor
# ---------------------------------------------------------------------------

class _StructureVisitor:
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.chunks: list[str] = []
        self._scope: list[str] = []

    def on_visit(self, node) -> bool:
        try:
            import libcst as cst
            if isinstance(node, cst.Module):
                self._visit_module(node)
            elif isinstance(node, cst.ClassDef):
                self._visit_class(node)
            elif isinstance(node, cst.FunctionDef):
                self._visit_function(node)
        except Exception:
            pass
        return True

    def on_leave(self, node) -> None:
        try:
            import libcst as cst
            if isinstance(node, cst.ClassDef):
                if self._scope:
                    self._scope.pop()
        except Exception:
            pass

    def _visit_module(self, node) -> None:
        import libcst as cst
        doc = _docstring(node)
        imports: list[str] = []
        for stmt in node.body:
            if isinstance(stmt, cst.SimpleStatementLine):
                for s in stmt.body:
                    if isinstance(s, (cst.Import, cst.ImportFrom)):
                        try:
                            imports.append(node.code_for_node(s).strip())
                        except Exception:
                            pass

        lines = [f"File: {self.filename} (Python module)"]
        if doc:
            lines.append(f"Docstring: {doc}")
        if imports:
            lines.append("Imports: " + ", ".join(imports[:15]))
        self.chunks.append("\n".join(lines))

    def _visit_class(self, node) -> None:
        import libcst as cst
        name = node.name.value
        bases = ", ".join(
            _safe_code(b.value) for b in node.bases
        ) if node.bases else ""
        doc = _docstring(node)
        methods = [
            item.name.value
            for item in node.body.body
            if isinstance(item, cst.FunctionDef)
        ]
        scope = ".".join(self._scope + [name])
        lines = [f"Class: {scope}"]
        if bases:
            lines.append(f"Bases: {bases}")
        if doc:
            lines.append(f"Docstring: {doc}")
        if methods:
            lines.append(f"Methods: {', '.join(methods)}")
        lines.append(f"File: {self.filename}")
        self.chunks.append("\n".join(lines))
        self._scope.append(name)

    def _visit_function(self, node) -> None:
        name = node.name.value
        doc = _docstring(node)
        params = _params(node.params)
        scope = ".".join(self._scope + [name])
        lines = [f"Function: {scope}({params})"]
        if doc:
            lines.append(f"Docstring: {doc}")
        lines.append(f"File: {self.filename}")
        self.chunks.append("\n".join(lines))


def _docstring(node) -> str:
    import libcst as cst
    body = getattr(node, "body", None)
    stmts = getattr(body, "body", []) if body else []
    if not stmts:
        return ""
    first = stmts[0]
    if isinstance(first, cst.SimpleStatementLine) and first.body:
        expr = first.body[0]
        if isinstance(expr, cst.Expr) and isinstance(
            expr.value, (cst.SimpleString, cst.FormattedString, cst.ConcatenatedString)
        ):
            try:
                raw = _safe_code(expr.value)
                raw = raw.strip("\"'").strip()
                return textwrap.shorten(raw, width=300, placeholder="...")
            except Exception:
                pass
    return ""


def _params(params) -> str:
    import libcst as cst
    parts: list[str] = []
    for p in params.params:
        part = p.name.value
        if p.annotation:
            part += ": " + _safe_code(p.annotation.annotation)
        if p.default:
            part += " = " + _safe_code(p.default)
        parts.append(part)
    if params.star_kwarg:
        parts.append("**" + params.star_kwarg.name.value)
    return ", ".join(parts)


def _safe_code(node) -> str:
    try:
        import libcst as cst
        return cst.parse_module("").code_for_node(node)
    except Exception:
        return "?"
