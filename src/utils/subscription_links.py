import html
import logging
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from config import Config

logger = logging.getLogger(__name__)


def render_direct_slot_notice() -> str:
    return ""


def _build_subscription_name(*, user_id: Optional[int] = None, plan_name: Optional[str] = None) -> str:
    base_name = (Config.SIDR_SUBSCRIPTION_NAME or "Kakoito VPN").strip() or "Kakoito VPN"
    if plan_name:
        return f"{base_name} - {plan_name}"
    if user_id is not None:
        return f"{base_name} - {user_id}"
    return base_name


def build_sidr_subscription_url(
    vpn_url: str,
    *,
    user_id: Optional[int] = None,
    plan_name: Optional[str] = None,
) -> str:
    clean_url = (vpn_url or "").strip()
    template = (Config.SIDR_SUBSCRIPTION_TEMPLATE or "").strip()
    if not clean_url or not template:
        return ""

    profile_name = _build_subscription_name(user_id=user_id, plan_name=plan_name)
    try:
        return template.format(
            url=clean_url,
            url_quoted=quote(clean_url, safe=""),
            name=profile_name,
            name_quoted=quote(profile_name, safe=""),
        )
    except Exception as exc:
        logger.warning("Failed to build SIDR subscription URL from template: %s", exc)
        return ""


def build_merged_subscription_url(
    client_uuid: str,
    *,
    base_subscription_url: str = "",
    output_format: Optional[str] = None,
) -> str:
    api_base_url = (Config.MERGED_SUBSCRIPTION_API_BASE or "").strip().rstrip("/")
    clean_uuid = (client_uuid or "").strip()
    if not api_base_url or not clean_uuid:
        return ""

    query: dict[str, str] = {}
    fmt = (output_format or Config.MERGED_SUBSCRIPTION_FORMAT or "base64").strip().lower()
    if fmt and fmt != "base64":
        query["format"] = fmt

    clean_base_subscription_url = (base_subscription_url or "").strip()
    if Config.MERGED_SUBSCRIPTION_INCLUDE_BASE_URL and clean_base_subscription_url:
        query["base_url"] = clean_base_subscription_url

    if not query:
        return f"{api_base_url}/sub/{clean_uuid}"
    return f"{api_base_url}/sub/{clean_uuid}?{urlencode(query)}"


def build_primary_subscription_url(*, client_uuid: str = "", sub_id: str = "") -> str:
    direct_primary_url = (Config.PRIMARY_SUBSCRIPTION_URL or "").strip()
    if direct_primary_url:
        return direct_primary_url

    base_subscription_url = ""
    clean_sub_id = (sub_id or "").strip()
    if clean_sub_id and (Config.SUB_PANEL_BASE or "").strip():
        base_subscription_url = f"{Config.SUB_PANEL_BASE}{clean_sub_id}"

    merged_url = build_merged_subscription_url(
        client_uuid,
        base_subscription_url=base_subscription_url,
    )
    if merged_url:
        return merged_url
    return base_subscription_url


def resolve_display_subscription_url(vpn_url: str) -> str:
    direct_primary_url = (Config.PRIMARY_SUBSCRIPTION_URL or "").strip()
    if direct_primary_url:
        return direct_primary_url
    return (vpn_url or "").strip()


def build_grace_subscription_url(vpn_url: str) -> str:
    clean_url = (vpn_url or "").strip()
    if not clean_url:
        return ""
    try:
        parsed = urlparse(clean_url)
        if "/sub/" not in parsed.path:
            return ""
        grace_path = parsed.path.replace("/sub/", "/sub-grace/", 1)
        return urlunparse(parsed._replace(path=grace_path))
    except Exception as exc:
        logger.warning("Failed to build grace subscription URL: %s", exc)
        return ""


def render_connection_info(
    vpn_url: str,
    *,
    user_id: Optional[int] = None,
    plan_name: Optional[str] = None,
    include_sidr: bool = True,
) -> str:
    clean_url = resolve_display_subscription_url(vpn_url)
    if not clean_url:
        return ""

    lines = [
        "🔗 Ссылка для подключения:",
        f"<blockquote><code>{html.escape(clean_url)}</code></blockquote>",
        "",
        "После импорта в подписке будут доступны:",
        "• Основной сервер",
        "• Белые списки для резервного подключения",
    ]
    if include_sidr:
        sidr_url = build_sidr_subscription_url(clean_url, user_id=user_id, plan_name=plan_name)
        if sidr_url:
            lines.extend(
                [
                    "",
                    "📲 Sidr:",
                    f"<code>{html.escape(sidr_url)}</code>",
                ]
            )
    return "\n".join(lines)
