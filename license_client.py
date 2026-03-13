from __future__ import annotations

from src.licensing.license_client import (
    DEVICE_LIMIT_MESSAGE,
    INVALID_LICENSE_MESSAGE,
    LICENSE_FILE_MISSING_MESSAGE,
    LICENSE_EXPIRED_MESSAGE,
    SUPABASE_NOT_CONFIGURED_MESSAGE,
    LICENSE_VALIDATION_UNAVAILABLE_MESSAGE,
    LicenseRuntimeContext,
    LicenseStartupError,
    SupabaseLicenseClient,
    clear_runtime_context,
    generate_license_key,
    get_runtime_context,
    launch_with_license,
    load_local_license_key,
    set_runtime_context,
)

__all__ = [
    "DEVICE_LIMIT_MESSAGE",
    "INVALID_LICENSE_MESSAGE",
    "LICENSE_FILE_MISSING_MESSAGE",
    "LICENSE_EXPIRED_MESSAGE",
    "SUPABASE_NOT_CONFIGURED_MESSAGE",
    "LICENSE_VALIDATION_UNAVAILABLE_MESSAGE",
    "LicenseRuntimeContext",
    "LicenseStartupError",
    "SupabaseLicenseClient",
    "clear_runtime_context",
    "generate_license_key",
    "get_runtime_context",
    "launch_with_license",
    "load_local_license_key",
    "set_runtime_context",
]
