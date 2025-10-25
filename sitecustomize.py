"""
Auto-start coverage for pytest-cov. This file ensures that coverage is gathered
from subprocesses in CLI tests.
"""

import os

if os.getenv("COVERAGE_PROCESS_START"):
    import coverage

    coverage.process_startup()
