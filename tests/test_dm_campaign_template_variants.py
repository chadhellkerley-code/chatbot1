from unittest import mock

from src.dm_campaign.proxy_workers_runner import (
    TemplateRotator,
    _expand_template_variants,
    _normalize_templates,
    calculate_workers,
)


def test_expand_template_variants_splits_per_non_empty_line() -> None:
    text = "hola\n\ncomo estas?\r\n   \nque tal"
    assert _expand_template_variants(text) == ["hola", "como estas?", "que tal"]


def test_normalize_templates_accepts_multiple_payload_shapes_and_dedupes() -> None:
    raw = [
        {"text": "msg1\nmsg2"},
        {"content": "msg2\nmsg3"},
        {"message": "msg4"},
        "msg4\nmsg5",
    ]
    assert _normalize_templates(raw) == ["msg1", "msg2", "msg3", "msg4", "msg5"]


def test_template_rotator_cycles_variants_round_robin() -> None:
    rotator = TemplateRotator(["a", "b", "c"])
    sequence = [rotator.next_variant()[0] for _ in range(7)]
    assert sequence == ["a", "b", "c", "a", "b", "c", "a"]


def test_calculate_workers_prioritizes_proxy_groups_with_ready_sessions() -> None:
    accounts = [
        {"username": "pendiente_a", "assigned_proxy_id": "proxy-a"},
        {"username": "lista_b", "assigned_proxy_id": "proxy-b"},
    ]
    with mock.patch(
        "src.dm_campaign.proxy_workers_runner._account_has_storage_state",
        side_effect=lambda account: str(account.get("username") or "") == "lista_b",
    ):
        payload = calculate_workers(accounts)

    assert payload["ordered_worker_ids"][:2] == ["proxy:proxy-b", "proxy:proxy-a"]
