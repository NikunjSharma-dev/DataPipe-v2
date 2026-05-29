"""
DataPipe v2 — Incremental multimodal indexing. Python + pandas + SQLite.

Quick start
-----------
>>> from datapipe import Pipeline, Store
>>> from datapipe.parsers import auto_transform
>>>
>>> store = Store("./index.db")
>>> pipe = (
...     Pipeline("docs", store)
...     .source("./data", patterns=["*.csv", "*.json", "*.md", "*.py"])
...     .transform(auto_transform)
...     .columns(["text", "chunk_index"])
... )
>>> stats = pipe.run()
>>> print(stats.summary())
>>> df = pipe.search("machine learning Python")
"""

from datapipe.engine import FileDelta, IndexStats, Pipeline, SourceFile, Store, compute_delta
from datapipe.parsers import auto_transform, get_parser, PARSER_REGISTRY
from datapipe.watcher import FileWatcher
from datapipe.memory import SessionMemory, log_tool_call

__all__ = [
    # engine
    "Pipeline", "Store", "SourceFile", "IndexStats", "FileDelta", "compute_delta",
    # parsers
    "auto_transform", "get_parser", "PARSER_REGISTRY",
    # watcher
    "FileWatcher",
    # memory
    "SessionMemory", "log_tool_call",
]

__version__ = "2.0.0"
