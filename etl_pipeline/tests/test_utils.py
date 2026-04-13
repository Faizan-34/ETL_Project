import logging

from utils import with_retry


def test_with_retry_succeeds_after_transient_failures():
    state = {"count": 0}

    def flaky_op():
        state["count"] += 1
        if state["count"] < 3:
            raise RuntimeError("temporary")
        return "ok"

    logger = logging.getLogger("test_retry")
    result = with_retry(
        flaky_op,
        retries=3,
        delay_seconds=0,
        logger=logger,
        operation_name="flaky_op",
    )

    assert result == "ok"
    assert state["count"] == 3
