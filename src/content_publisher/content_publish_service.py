from __future__ import annotations

from pathlib import Path
from typing import Any

from core import accounts as accounts_module
from core.accounts import _open_playwright_manual_session
from core.proxy_preflight import account_proxy_preflight

from .content_library_service import ContentLibraryService, ContentPublisherError


def _normalize_username(value: Any) -> str:
    return str(value or "").strip().lstrip("@")


class ContentPublishService:
    def __init__(
        self,
        *,
        root_dir: str | Path | None = None,
        library_service: ContentLibraryService | None = None,
    ) -> None:
        self.library = library_service or ContentLibraryService(root_dir=root_dir)

    def _resolve_account(self, account_id: str) -> dict[str, Any]:
        clean_account_id = _normalize_username(account_id)
        if not clean_account_id:
            raise ContentPublisherError("Selecciona una cuenta destino valida.")
        for row in accounts_module.list_all():
            if not isinstance(row, dict):
                continue
            username = _normalize_username(row.get("username"))
            if username.lower() == clean_account_id.lower():
                return dict(row)
        raise ContentPublisherError(f"No se encontro la cuenta destino @{clean_account_id}.")

    def _publish_start_url(self, media_kind: str) -> tuple[str, str]:
        normalized = str(media_kind or "").strip().lower()
        if normalized == "video":
            return "https://www.instagram.com/reels/create/", "Reel"
        return "https://www.instagram.com/create/select/", "Post"

    def publish(
        self,
        *,
        account_id: str,
        media_path: str,
        caption: str = "",
    ) -> dict[str, Any]:
        account = self._resolve_account(account_id)
        username = _normalize_username(account.get("username"))
        proxy_preflight = account_proxy_preflight(account)
        if bool(proxy_preflight.get("blocking")):
            detail = str(proxy_preflight.get("message") or "Proxy bloqueado.").strip() or "Proxy bloqueado."
            raise ContentPublisherError(
                f"No se puede abrir la publicacion manual para @{username}: {detail}"
            )
        bundle = self.library.resolve_media_bundle(media_path)
        files = [Path(item) for item in bundle.get("files") or [] if str(item or "").strip()]
        if not files:
            raise ContentPublisherError("El contenido seleccionado no tiene archivos para publicar.")

        media_kind = str(bundle.get("media_type") or "image").strip().lower()
        final_caption = str(caption or "").strip()
        start_url, publish_label = self._publish_start_url(media_kind)
        logs = [
            f"Preparando publicacion manual en @{username}.",
            f"Tipo detectado: {media_kind}.",
            f"Archivos listos: {len(files)}.",
        ]
        if final_caption:
            logs.append("Caption listo para pegar en el flujo manual.")

        _open_playwright_manual_session(
            account,
            start_url=start_url,
            action_label=f"Publicar {publish_label} (manual)",
        )

        return {
            "account_id": username,
            "media_type": media_kind,
            "media_path": str(bundle.get("media_path") or media_path),
            "caption": final_caption,
            "published_media": {},
            "logs": logs,
            "summary": f"Sesion manual de publicacion abierta para @{username}.",
        }
