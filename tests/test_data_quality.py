"""Run the data-quality gate against the committed dashboard export."""

import pytest

from pipeline.validate_export import EVENTS_PATH, run


@pytest.mark.skipif(not EVENTS_PATH.exists(), reason="no exported data present")
def test_exported_data_passes_quality_gate():
    failures = run()
    assert not failures, "data-quality failures:\n" + "\n".join(failures)
