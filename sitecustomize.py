# This file ensures that we can collect coverage data for the CLI when it's running in a subprocess
import os

if os.getenv("COVERAGE_PROCESS_START"):
    import coverage

    coverage.process_startup()
