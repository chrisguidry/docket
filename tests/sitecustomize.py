import os

if os.getenv("COVERAGE_PROCESS_START"):
    import coverage

    coverage.process_startup()
