"""
Shared test setup.

The ingest/export modules read credentials from the environment at import time
(populated from .env locally). In CI there is no .env, so provide harmless
defaults before any `pipeline.*` import. setdefault never overrides real values.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("FIRMS_MAP_KEY", "test-map-key")
os.environ.setdefault("DATABRICKS_HOST", "https://test.cloud.databricks.com")
os.environ.setdefault("DATABRICKS_TOKEN", "test-token")
os.environ.setdefault("DATABRICKS_SQL_HTTP_PATH", "/sql/1.0/warehouses/test")
