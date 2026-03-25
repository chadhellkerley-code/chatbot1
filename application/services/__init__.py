from __future__ import annotations

import logging
import threading
from importlib import import_module
from typing import TYPE_CHECKING, Any, Callable

from core.alias_identity import DEFAULT_ALIAS_DISPLAY_NAME
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


logger = logging.getLogger(__name__)


class ApplicationServices:
    def __init__(self, context: ServiceContext) -> None:
        from .account_service import AccountService
        from .inbox_service import InboxService
        from .system_service import SystemService

        self.context = context
        self._service_lock = threading.RLock()
        self._lazy_services: dict[str, Any] = {}
        self._post_show_lock = threading.RLock()
        self._post_show_started = False
        self._post_show_completed = threading.Event()
        self._post_show_thread: threading.Thread | None = None
        self.state_store = get_app_state_store(context.root_dir)
        self.accounts = AccountService(context)
        self.inbox = InboxService(context)
        self.system = SystemService(context)

    def _get_or_create_lazy_service(
        self,
        service_name: str,
        factory: Callable[[], Any],
    ) -> Any:
        service = self._lazy_services.get(service_name)
        if service is not None:
            return service
        with self._service_lock:
            service = self._lazy_services.get(service_name)
            if service is not None:
                return service
            service = factory()
            self._lazy_services[service_name] = service
            return service

    def _build_aliases(self) -> "AliasLifecycleService":
        from .alias_lifecycle_service import AliasLifecycleService

        return AliasLifecycleService(
            self.context,
            accounts=self.accounts,
            automation_provider=lambda: self.automation,
            warmup_provider=lambda: self.warmup,
        )

    def _build_automation(self) -> "AutomationService":
        from .automation_service import AutomationService

        return AutomationService(self.context, inbox_service=self.inbox)

    def _build_campaigns(self) -> "CampaignService":
        from .campaign_service import CampaignService

        return CampaignService(self.context)

    def _build_leads(self) -> "LeadsService":
        from .leads_service import LeadsService

        return LeadsService(self.context)

    def _build_warmup(self) -> "WarmupService":
        from .warmup_service import WarmupService

        return WarmupService(self.context)

    @property
    def aliases(self) -> "AliasLifecycleService":
        return self._get_or_create_lazy_service("aliases", self._build_aliases)

    @property
    def automation(self) -> "AutomationService":
        return self._get_or_create_lazy_service("automation", self._build_automation)

    @property
    def campaigns(self) -> "CampaignService":
        return self._get_or_create_lazy_service("campaigns", self._build_campaigns)

    @property
    def leads(self) -> "LeadsService":
        return self._get_or_create_lazy_service("leads", self._build_leads)

    @property
    def warmup(self) -> "WarmupService":
        return self._get_or_create_lazy_service("warmup", self._build_warmup)

    def get_initial_active_alias(self) -> str:
        try:
            active_alias = self.state_store.get_active_alias()
        except Exception:
            return DEFAULT_ALIAS_DISPLAY_NAME
        try:
            record = self.accounts.get_alias_record(active_alias)
        except Exception:
            return DEFAULT_ALIAS_DISPLAY_NAME
        return str(record.get("display_name") or DEFAULT_ALIAS_DISPLAY_NAME).strip() or DEFAULT_ALIAS_DISPLAY_NAME

    def start_post_show_hydration(self) -> None:
        with self._post_show_lock:
            if self._post_show_started:
                return
            self._post_show_started = True
            self._post_show_completed.clear()
            thread = threading.Thread(
                target=self._run_post_show_hydration,
                name="application-services-post-show",
                daemon=True,
            )
            self._post_show_thread = thread
            thread.start()

    def wait_for_post_show_hydration(self, timeout: float | None = None) -> bool:
        if not self._post_show_started:
            return True
        return self._post_show_completed.wait(timeout)

    def _run_post_show_hydration(self) -> None:
        try:
            try:
                self.state_store.sync_foundation()
            except Exception:
                logger.exception("No se pudo sincronizar foundation post-show.")
            try:
                self.warmup
            except Exception:
                logger.exception("No se pudo hidratar WarmupService post-show.")
        finally:
            self._post_show_completed.set()


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
