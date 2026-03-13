from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .content_extract_service import ContentExtractService
    from .content_library_service import ContentLibraryService, ContentPublisherError
    from .content_publish_service import ContentPublishService


_LAZY_EXPORTS = {
    "ContentExtractService": ".content_extract_service",
    "ContentLibraryService": ".content_library_service",
    "ContentPublishService": ".content_publish_service",
    "ContentPublisherError": ".content_library_service",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(str(name or "").strip())
    if not module_name:
        raise AttributeError(name)
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value

__all__ = [
    "ContentExtractService",
    "ContentLibraryService",
    "ContentPublishService",
    "ContentPublisherError",
]
