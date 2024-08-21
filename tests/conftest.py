import pytest

XFAIL_SCHEMA_UPDATE = pytest.mark.xfail(reason="Target not suport schema updates")
XFAIL_ENCODED_STRING = pytest.mark.xfail(reason="Target not suport encoded string")


SCHEMA_UPDATE = {
    "test_target_schema_updates"
}

ENCODED_STRING = {
    "test_target_encoded_string_data"
}


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip tests that require a live API key."""
    test_name = item.name

    if test_name in SCHEMA_UPDATE:
        item.add_marker(XFAIL_SCHEMA_UPDATE)

    if test_name in ENCODED_STRING:
        item.add_marker(XFAIL_ENCODED_STRING)