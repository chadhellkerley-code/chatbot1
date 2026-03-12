from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .base import ServiceContext, ServiceError
from src.persistence import get_app_state_store

if TYPE_CHECKING:
    from .account_service import AccountService
    from .alias_lifecycle_service import AliasLifecycleService
    from .automation_service import AutomationService
    from .campaign_service import CampaignService
    from .inbox_service import InboxService
    from .leads_service import LeadsService
    from .system_service import SystemService
    from .warmup_service import WarmupService


_LAZY_EXPORTS = {
    "AccountService": ".account_service",
    "AliasLifecycleService": ".alias_lifecycle_service",
    "AutomationService": ".automation_service",
    "CampaignService": ".campaign_service",
    "InboxService": ".inbox_service",
    "LeadsService": ".leads_service",
    "SystemService": ".system_service",
    "WarmupService": ".warmup_service",
}


class ApplicationServices:
    def __init__(self, context: ServiceContext) -> None:
        from .account_service import AccountService
        from .alias_lifecycle_service import AliasLifecycleService
        from .automation_service import AutomationService
        from .campaign_service import CampaignService
        from .inbox_service import InboxService
        from .leads_service import LeadsService
        from .system_service import SystemService
        from .warmup_service import WarmupService

        self.context = context
        self.state_store = get_app_state_store(context.root_dir)
        self.accounts = AccountService(context)
        self.automation = AutomationService(context)
        self.warmup = WarmupService(context)
        self.aliases = AliasLifecycleService(
            context,
            accounts=self.accounts,
            automation=self.automation,
            warmup=self.warmup,
        )
        self.campaigns = CampaignService(context)
        self.leads = LeadsService(context)
        self.inbox = InboxService(context)
        self.system = SystemService(context)
        try:
            self.state_store.sync_foundation()
        except Exception:
            pass


def build_application_services(root_dir=None) -> ApplicationServices:
    context = ServiceContext.default(root_dir=root_dir)
    return ApplicationServices(context)


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(str(name or "").strip())
    if not module_name:
        raise AttributeError(name)
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


__all__ = [
    "AccountService",
    "AliasLifecycleService",
    "ApplicationServices",
    "AutomationService",
    "CampaignService",
    "InboxService",
    "LeadsService",
    "ServiceContext",
    "ServiceError",
    "SystemService",
    "WarmupService",
    "build_application_services",
]
