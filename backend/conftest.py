from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-optional",
        action="store_true",
        default=False,
        help="Run tests marked optional (requires optional dependency extras).",
    )
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run tests marked integration (network/service credentials required).",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_optional = config.getoption("--run-optional")
    run_integration = config.getoption("--run-integration")
    skip_optional = pytest.mark.skip(reason="optional dependency test; pass --run-optional to include")
    skip_integration = pytest.mark.skip(reason="integration test; pass --run-integration to include")

    for item in items:
        if "optional" in item.keywords and not run_optional:
            item.add_marker(skip_optional)
        if "integration" in item.keywords and not run_integration:
            item.add_marker(skip_integration)
