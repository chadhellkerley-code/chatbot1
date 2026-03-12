from __future__ import annotations

from src.licensing.device_id import collect_device_identity, generate_device_id


def test_generate_device_id_is_deterministic() -> None:
    first = generate_device_id(
        hostname="host-a",
        os_user="owner",
        mac_address="aa:bb:cc:dd:ee:ff",
    )
    second = generate_device_id(
        hostname="host-a",
        os_user="owner",
        mac_address="aa:bb:cc:dd:ee:ff",
    )
    assert first == second
    assert len(first) == 64


def test_collect_device_identity_normalizes_components() -> None:
    identity = collect_device_identity(
        hostname=" workstation ",
        os_user=" admin ",
        mac_address="AABBCCDDEEFF",
    )
    assert identity.hostname == "workstation"
    assert identity.os_user == "admin"
    assert identity.mac_address == "aa:bb:cc:dd:ee:ff"
    assert identity.device_id == generate_device_id(
        hostname="workstation",
        os_user="admin",
        mac_address="aa:bb:cc:dd:ee:ff",
    )
