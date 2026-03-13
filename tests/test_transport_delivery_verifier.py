from __future__ import annotations

from src.transport.delivery_verifier import DeliveryDecision, DeliveryVerifier


def test_decide_confirmation_prefers_network_signal() -> None:
    result = DeliveryVerifier.decide_confirmation(
        net_ok=True,
        dom_ok=False,
        bubble_ok=False,
        composer_cleared=False,
        allow_unverified=False,
    )

    assert result == DeliveryDecision(
        ok=True,
        detail="sent_verified",
        stage="SEND_OK",
        verified=True,
        verify_source="network",
    )


def test_decide_confirmation_accepts_dom_and_bubble() -> None:
    result = DeliveryVerifier.decide_confirmation(
        net_ok=False,
        dom_ok=True,
        bubble_ok=True,
        composer_cleared=True,
        allow_unverified=False,
    )

    assert result == DeliveryDecision(
        ok=True,
        detail="sent_verified",
        stage="SEND_OK",
        verified=True,
        verify_source="dom_bubble_composer",
    )


def test_decide_confirmation_blocks_unverified_when_disabled() -> None:
    result = DeliveryVerifier.decide_confirmation(
        net_ok=False,
        dom_ok=False,
        bubble_ok=False,
        composer_cleared=True,
        allow_unverified=False,
    )

    assert result == DeliveryDecision(
        ok=False,
        detail="send_unverified_blocked",
        stage="SEND_UNVERIFIED_BLOCKED",
        verified=False,
        reason_code="SENT_UNVERIFIED",
    )


def test_decide_confirmation_allows_unverified_when_enabled() -> None:
    result = DeliveryVerifier.decide_confirmation(
        net_ok=False,
        dom_ok=False,
        bubble_ok=False,
        composer_cleared=True,
        allow_unverified=True,
    )

    assert result == DeliveryDecision(
        ok=True,
        detail="sent_unverified",
        stage="SEND_UNVERIFIED_ALLOWED",
        verified=False,
        reason_code="SENT_UNVERIFIED",
        sent_unverified=True,
    )
