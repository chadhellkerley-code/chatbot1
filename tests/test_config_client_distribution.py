from __future__ import annotations

from config import _client_distribution_flag


def test_client_distribution_flag_prefers_env_override() -> None:
    assert _client_distribution_flag({"CLIENT_DISTRIBUTION": "1"}, {}, False) is True
    assert _client_distribution_flag({"CLIENT_DISTRIBUTION": "0"}, {}, True) is False


def test_client_distribution_flag_reads_app_config_channel() -> None:
    assert _client_distribution_flag({}, {"channel": "client"}, False) is True
    assert _client_distribution_flag({}, {"channel": "owner"}, True) is False


def test_client_distribution_flag_reads_app_config_boolean_field() -> None:
    assert _client_distribution_flag({}, {"client_distribution": "true"}, False) is True
    assert _client_distribution_flag({}, {"client_distribution": "false"}, True) is False
