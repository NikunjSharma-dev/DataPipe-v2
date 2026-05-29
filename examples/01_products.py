"""
Example 1 — Products Catalog Pipeline
======================================
Indexes CSV product data with full-text search over name, category, description.

Usage:
    datapipe update  examples/01_products.py
    datapipe search  examples/01_products.py "wireless headphones"
    datapipe search  examples/01_products.py "home appliance under 100"
    datapipe sql     examples/01_products.py "SELECT name, price FROM dp_products_rows WHERE json_extract(data_json,'$.category')='Electronics' ORDER BY CAST(json_extract(data_json,'$.price') AS REAL)"
    datapipe stats   examples/01_products.py
"""

import sys
from pathlib import Path

# Make the datapipe package importable when running from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapipe import Pipeline, Store, csv_transform

# --- store -----------------------------------------------------------------
store = Store("./datapipe_index.db")

# --- pipeline --------------------------------------------------------------
pipe = (
    Pipeline("products", store)
    .source(
        Path(__file__).parent / "data",
        patterns=["products*.csv"],
    )
    .transform(csv_transform)
    .columns(["name", "category", "description", "price", "brand", "in_stock"])
)
