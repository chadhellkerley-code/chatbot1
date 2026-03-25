from __future__ import annotations

import pytest

from application.services.base import ServiceContext, ServiceError
from application.services.system_service import SystemService


@pytest.mark.parametrize(
    ("method_name", "kwargs"),
    [
        ("list_licenses", {}),
        ("fetch_license", {"license_key": "demo"}),
        (
            "create_license",
            {
                "client_name": "Cliente Demo",
                "plan_name": "standard",
                "max_devices": 2,
                "expires_at": "2030-01-01T00:00:00+00:00",
                "notes": "",
            },
        ),
        ("extend_license", {"license_key": "demo", "days": 30}),
        ("deactivate_license", {"license_key": "demo"}),
        ("reset_device_activations", {"license_key": "demo"}),
        ("list_license_activations", {"license_key": "demo"}),
        ("check_updates", {}),
        ("update_config", {}),
        ("save_update_config", {"updates": {"channel": "stable"}}),
        ("supabase_config", {}),
        (
            "save_supabase_config",
            {"supabase_url": "https://example.supabase.co", "supabase_key": "key"},
        ),
    ],
)
def test_system_service_admin_methods_require_owner(monkeypatch, method_name: str, kwargs: dict[str, object]) -> None:
    monkeypatch.setenv("INSTACRM_BOOTSTRAP_MODE", "client")

    service = SystemService(ServiceContext.default())
    service._license_admin = lambda: pytest.fail("_license_admin no deberia ejecutarse en client")  # type: ignore[method-assign]

    method = getattr(service, method_name)
    with pytest.raises(ServiceError, match="solo esta disponible en modo owner"):
        method(**kwargs)
