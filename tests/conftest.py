from datetime import datetime, timezone
from functools import partial
from typing import Callable

import pytest


@pytest.fixture
def now() -> Callable[[], datetime]:
    return partial(datetime.now, timezone.utc)
