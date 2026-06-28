#!/usr/bin/env python3
"""
PreToolUse hook: blocks Databricks job triggers when spark_pipeline_databricks.py
has local changes that haven't been uploaded to the Databricks workspace.
"""
import json
import subprocess
import sys

try:
    data = json.load(sys.stdin)
    cmd = data.get("tool_input", {}).get("command", "")
except Exception:
    sys.exit(0)  # fail open — don't block if we can't parse

TRIGGER_SIGNALS = ["jobs.run_now", "run_now(job_id", "DATABRICKS_JOB_ID"]
PIPELINE_SCRIPT = "spark_pipeline_databricks.py"

if not any(s in cmd for s in TRIGGER_SIGNALS):
    sys.exit(0)

# Job trigger detected — check if the pipeline script has uncommitted local changes.
try:
    result = subprocess.run(
        ["git", "diff", "HEAD", "--", PIPELINE_SCRIPT],
        capture_output=True, text=True,
    )
    has_changes = bool(result.stdout.strip())
except Exception:
    has_changes = False

if has_changes:
    print(
        f"BLOCKED: {PIPELINE_SCRIPT} has local changes that haven't been uploaded "
        f"to the Databricks workspace.\n"
        f"Upload the script to /Workspace/Users/timkhaiet@gmail.com/{PIPELINE_SCRIPT} first."
    )
    sys.exit(2)
