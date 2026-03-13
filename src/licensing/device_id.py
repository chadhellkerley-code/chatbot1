from __future__ import annotations

import getpass
import hashlib
import os
import socket
import uuid
from dataclasses import dataclass


def _normalize_text(value: object, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def _normalize_mac(value: object) -> str:
    if isinstance(value, int):
        raw = f"{value:012x}"
    else:
        raw = "".join(ch for ch in str(value or "") if ch.isalnum()).lower()
    raw = raw.zfill(12)[-12:]
    return ":".join(raw[index : index + 2] for index in range(0, 12, 2))


@dataclass(frozen=True)
class DeviceIdentity:
    hostname: str
    os_user: str
    mac_address: str
    device_id: str

    def as_dict(self) -> dict[str, str]:
        return {
            "hostname": self.hostname,
            "os_user": self.os_user,
            "mac_address": self.mac_address,
            "device_id": self.device_id,
        }


def collect_device_identity(
    *,
    hostname: str | None = None,
    os_user: str | None = None,
    mac_address: str | None = None,
) -> DeviceIdentity:
    resolved_hostname = _normalize_text(
        hostname or socket.gethostname() or os.environ.get("COMPUTERNAME"),
        "unknown-host",
    )
    resolved_user = _normalize_text(
        os_user or getpass.getuser() or os.environ.get("USERNAME"),
        "unknown-user",
    )
    resolved_mac = _normalize_mac(mac_address if mac_address is not None else uuid.getnode())
    seed = f"{resolved_hostname}|{resolved_user}|{resolved_mac}".encode("utf-8")
    device_id = hashlib.sha256(seed).hexdigest()
    return DeviceIdentity(
        hostname=resolved_hostname,
        os_user=resolved_user,
        mac_address=resolved_mac,
        device_id=device_id,
    )


def generate_device_id(
    *,
    hostname: str | None = None,
    os_user: str | None = None,
    mac_address: str | None = None,
) -> str:
    return collect_device_identity(
        hostname=hostname,
        os_user=os_user,
        mac_address=mac_address,
    ).device_id
