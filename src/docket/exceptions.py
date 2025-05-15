class ForcedRetry(Exception):
    """Raised when a task requests a retry via `in_` or `at`"""
