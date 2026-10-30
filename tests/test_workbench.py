"""The one test the template ships: the simulators import and report stats. Replace with your own."""
import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))


def test_simulators_import_and_report_stats():
    for name in ("app_events", "eod_files", "saas_api", "telemetry"):
        mod = importlib.import_module(name)
        stats = mod.stats()
        assert isinstance(stats, dict) and stats, name
