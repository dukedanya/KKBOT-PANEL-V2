from __future__ import annotations

import asyncio
from typing import Any

from config import Config
from db import Database
from services.panel import PanelAPI
from services.payment_gateway import build_payment_gateway
from tariffs import get_all_active
from utils.templates import render_template


def _status_line(ok: bool, label: str, note: str = "") -> str:
    suffix = f" • {note}" if note else ""
    return f"{label}: {'OK' if ok else 'FAIL'}{suffix}"


async def build_self_check_report(db: Database, panel: PanelAPI, payment_gateway) -> dict[str, Any]:
    report: dict[str, Any] = {
        "db_ok": False,
        "panel_ok": False,
        "tariffs_ok": False,
        "payment_ok": False,
        "templates_ok": False,
        "referral_ok": False,
        "profile_render_ok": False,
        "payment_render_ok": False,
        "inbounds_count": 0,
        "tariffs_count": 0,
        "provider_name": "",
        "payment_note": "",
        "safe_mode_enabled": False,
        "target_inbounds": Config.PANEL_TARGET_INBOUND_IDS or "-",
        "template_keys": [],
        "referral_note": "",
        "profile_render_note": "",
        "payment_render_note": "",
    }

    if hasattr(db, "ping"):
        try:
            report["db_ok"] = bool(await db.ping())
        except Exception as exc:
            report["db_ok"] = False
            report["db_error"] = str(exc)

    try:
        inbounds = await panel.get_inbounds()
        report["panel_ok"] = bool(inbounds and inbounds.get("success"))
        report["inbounds_count"] = len((inbounds or {}).get("obj") or [])
    except Exception as exc:
        report["panel_ok"] = False
        report["panel_error"] = str(exc)

    tariffs = get_all_active()
    report["tariffs_ok"] = bool(tariffs)
    report["tariffs_count"] = len(tariffs)

    provider_name = str(getattr(payment_gateway, "provider_name", Config.PAYMENT_PROVIDER) or Config.PAYMENT_PROVIDER)
    report["provider_name"] = provider_name
    payment_ok = True
    payment_note = provider_name
    if provider_name == "yookassa":
        payment_ok = bool(Config.YOOKASSA_SHOP_ID and Config.YOOKASSA_SECRET_KEY)
        payment_note = f"ЮKassa shop={Config.YOOKASSA_SHOP_ID or '-'}"
    elif provider_name == "telegram_stars":
        payment_note = "Telegram Stars"
    elif provider_name == "itpay":
        payment_ok = bool(getattr(Config, "ITPAY_API_KEY", "") or getattr(Config, "ITPAY_MERCHANT_ID", ""))
        payment_note = "ITPAY"
    report["payment_ok"] = payment_ok
    report["payment_note"] = payment_note

    report["safe_mode_enabled"] = str(await db.get_setting("system:safe_mode", "0") or "0") == "1"

    template_keys = ["main_message", "support_menu", "payment_success_user", "referral_menu"]
    template_results: list[tuple[str, bool]] = []
    for key in template_keys:
        try:
            rendered, _photo = await render_template(
                db,
                key,
                plan_name="Месяц",
                ip_limit=3,
                duration="30 дней",
                connection_info="🔗 Ссылка для подключения:\n<code>https://example.com/sub</code>",
                bonus_days=5,
                amount=100.0,
                request_id=1,
            )
            template_results.append((key, bool((rendered or "").strip())))
        except Exception:
            template_results.append((key, False))
    report["templates_ok"] = all(ok for _, ok in template_results)
    report["template_keys"] = [f"{key}:{'OK' if ok else 'FAIL'}" for key, ok in template_results]

    try:
        report["referral_ok"] = all(
            [
                float(Config.REF_PERCENT_LEVEL1) >= 0,
                float(Config.REF_PERCENT_LEVEL2) >= 0,
                float(Config.REF_PERCENT_LEVEL3) >= 0,
                float(Config.REF_FIRST_PAYMENT_DISCOUNT_PERCENT) >= 0,
                int(Config.REFERRED_BONUS_DAYS) >= 0,
            ]
        )
        report["referral_note"] = (
            f"L1={Config.REF_PERCENT_LEVEL1}% / "
            f"L2={Config.REF_PERCENT_LEVEL2}% / "
            f"L3={Config.REF_PERCENT_LEVEL3}% / "
            f"first={Config.REF_FIRST_PAYMENT_DISCOUNT_PERCENT}% / "
            f"+{Config.REFERRED_BONUS_DAYS}d"
        )
    except Exception as exc:
        report["referral_ok"] = False
        report["referral_note"] = str(exc)

    try:
        profile_text, _ = await render_template(db, "main_message")
        report["profile_render_ok"] = bool((profile_text or "").strip())
        report["profile_render_note"] = "main_message rendered"
    except Exception as exc:
        report["profile_render_ok"] = False
        report["profile_render_note"] = str(exc)

    try:
        payment_status_text, _ = await render_template(
            db,
            "payment_success_user",
            plan_name="Месяц",
            ip_limit=3,
            duration="35 дней",
            connection_info="🔗 Ссылка для подключения:\n<code>https://example.com/sub</code>",
        )
        report["payment_render_ok"] = bool((payment_status_text or "").strip())
        report["payment_render_note"] = "payment_success_user rendered"
    except Exception as exc:
        report["payment_render_ok"] = False
        report["payment_render_note"] = str(exc)

    return report


def report_to_cli_text(report: dict[str, Any]) -> str:
    lines = [
        "KKBOT self-check",
        _status_line(report.get("db_ok", False), "db", report.get("db_error", "")),
        _status_line(report.get("panel_ok", False), "panel", f"{report.get('inbounds_count', 0)} inbounds"),
        _status_line(report.get("tariffs_ok", False), "tariffs", f"{report.get('tariffs_count', 0)} active"),
        _status_line(report.get("payment_ok", False), "payment", str(report.get("payment_note", ""))),
        _status_line(report.get("templates_ok", False), "templates", ", ".join(report.get("template_keys", []))),
        _status_line(report.get("referral_ok", False), "referral", str(report.get("referral_note", ""))),
        _status_line(report.get("profile_render_ok", False), "profile_render", str(report.get("profile_render_note", ""))),
        _status_line(report.get("payment_render_ok", False), "payment_render", str(report.get("payment_render_note", ""))),
        f"panel_target_inbounds: {report.get('target_inbounds', '-')}",
        f"safe_mode: {'1' if report.get('safe_mode_enabled') else '0'}",
    ]
    return "\n".join(lines)


def report_to_html_text(report: dict[str, Any]) -> str:
    def yesno(flag: bool) -> str:
        return "✅ OK" if flag else "❌ FAIL"

    template_note = ", ".join(report.get("template_keys", []))
    return (
        "🩺 <b>Self-check бота</b>\n\n"
        f"База данных: <b>{yesno(bool(report.get('db_ok')))}</b>\n"
        f"Панель 3x-ui: <b>{yesno(bool(report.get('panel_ok')))}</b>\n"
        f"Inbounds в панели: <b>{int(report.get('inbounds_count', 0) or 0)}</b>\n"
        f"Тарифы загружены: <b>{yesno(bool(report.get('tariffs_ok')))}</b> • активных <b>{int(report.get('tariffs_count', 0) or 0)}</b>\n"
        f"Платёжный контур: <b>{yesno(bool(report.get('payment_ok')))}</b> • {report.get('payment_note', '')}\n"
        f"Шаблоны: <b>{yesno(bool(report.get('templates_ok')))}</b> • {template_note}\n"
        f"Реферальные условия: <b>{yesno(bool(report.get('referral_ok')))}</b> • {report.get('referral_note', '')}\n"
        f"Рендер кабинета: <b>{yesno(bool(report.get('profile_render_ok')))}</b> • {report.get('profile_render_note', '')}\n"
        f"Рендер оплаты: <b>{yesno(bool(report.get('payment_render_ok')))}</b> • {report.get('payment_render_note', '')}\n"
        f"Safe mode: <b>{'включён' if report.get('safe_mode_enabled') else 'выключен'}</b>\n"
        f"Target inbounds: <code>{report.get('target_inbounds', '-')}</code>"
    )


async def main() -> None:
    db = Database(Config.DATA_FILE)
    panel = PanelAPI()
    gateway = build_payment_gateway()
    try:
        await db.connect()
        await panel.start()
        report = await build_self_check_report(db, panel, gateway)
        print(report_to_cli_text(report))
    finally:
        await gateway.close()
        await panel.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
