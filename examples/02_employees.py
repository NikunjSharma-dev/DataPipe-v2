"""
Example 2 — Employee Directory Pipeline
========================================
Indexes JSON employee records. Demonstrates custom transforms and SQL analytics.

Usage:
    datapipe update  examples/02_employees.py
    datapipe search  examples/02_employees.py "machine learning Python"
    datapipe search  examples/02_employees.py "kubernetes infrastructure"
    datapipe sql     examples/02_employees.py "SELECT json_extract(data_json,'$.department') as dept, COUNT(*) as headcount FROM dp_employees_rows GROUP BY dept ORDER BY headcount DESC"
    datapipe stats   examples/02_employees.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datapipe import Pipeline, Store, json_transform

store = Store("./datapipe_index.db")

pipe = (
    Pipeline("employees", store)
    .source(
        Path(__file__).parent / "data",
        patterns=["employees*.json"],
    )
    .transform(json_transform)
    .columns(["name", "department", "role", "location", "skills", "years_exp"])
)
