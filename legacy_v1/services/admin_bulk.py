import asyncio
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

from config import Config
from db import Database
from services.panel import PanelAPI

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BroadcastResult:
    total: int = 0
    sent: int = 0
    failed: int = 0
    blocked: int = 0


@dataclass(slots=True)
class BulkExtendResult:
    total: int = 0
    extended: int = 0
    failed: int = 0


@dataclass(slots=True)
class BulkJob:
    job_id: str
    kind: str
    initiator_id: int
    audience: str = ""
    status: str = "queued"
    total: int = 0
    processed: int = 0
    sent: int = 0
    blocked: int = 0
    failed: int = 0
    extended: int = 0
    add_days: int = 0
    created_at: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0
    error: str = ""


_BULK_JOBS: dict[str, BulkJob] = {}
_BULK_JOB_ORDER: list[str] = []
_MAX_BULK_JOBS = 100


def _new_job_id(kind: str) -> str:
    prefix = (kind or "job")[:4]
    return f"{prefix}-{int(time.time())}-{secrets.token_hex(2)}"


def _register_job(job: BulkJob) -> None:
    _BULK_JOBS[job.job_id] = job
    _BULK_JOB_ORDER.append(job.job_id)
    if len(_BULK_JOB_ORDER) > _MAX_BULK_JOBS:
        stale_id = _BULK_JOB_ORDER.pop(0)
        _BULK_JOBS.pop(stale_id, None)


def _update_progress(job: BulkJob, processed: int, total: int) -> None:
    job.processed = max(0, int(processed))
    job.total = max(job.total, int(total))


def get_bulk_job(job_id: str) -> Optional[BulkJob]:
    return _BULK_JOBS.get(job_id)


def list_bulk_jobs(limit: int = 20) -> list[BulkJob]:
    ids = _BULK_JOB_ORDER[-max(1, int(limit)):]
    ids.reverse()
    return [job for job_id in ids if (job := _BULK_JOBS.get(job_id))]


async def _send_html(bot: Bot, user_id: int, text: str) -> None:
    try:
        await bot.send_message(user_id, text, parse_mode=ParseMode.HTML)
        return
    except TelegramBadRequest:
        await bot.send_message(user_id, text)


async def send_broadcast(
    *,
    bot: Bot,
    user_ids: Iterable[int],
    text: str,
    delay_sec: float = 0.03,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> BroadcastResult:
    users = [int(raw_user_id) for raw_user_id in user_ids]
    total = len(users)
    result = BroadcastResult(total=total)
    if progress_cb:
        progress_cb(0, total)
    for user_id in users:
        try:
            await _send_html(bot, user_id, text)
            result.sent += 1
        except TelegramForbiddenError:
            result.blocked += 1
            result.failed += 1
        except TelegramRetryAfter as exc:
            await asyncio.sleep(float(getattr(exc, "retry_after", 1) or 1))
            try:
                await _send_html(bot, user_id, text)
                result.sent += 1
            except TelegramForbiddenError:
                result.blocked += 1
                result.failed += 1
            except Exception as inner_exc:
                logger.warning("Broadcast retry failed for %s: %s", user_id, inner_exc)
                result.failed += 1
        except Exception as exc:
            logger.warning("Broadcast failed for %s: %s", user_id, exc)
            result.failed += 1
        if progress_cb:
            progress_cb(result.sent + result.failed, total)
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)
    return result


async def extend_active_subscriptions_bulk(
    *,
    db: Database,
    panel: PanelAPI,
    add_days: int,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> BulkExtendResult:
    subscribers = await db.get_all_subscribers()
    result = BulkExtendResult(total=len(subscribers))
    if progress_cb:
        progress_cb(0, result.total)
    if add_days <= 0:
        result.failed = result.total
        if progress_cb:
            progress_cb(result.total, result.total)
        return result

    processed = 0
    for user in subscribers:
        user_id = int(user.get("user_id", 0) or 0)
        if not user_id:
            result.failed += 1
            processed += 1
            if progress_cb:
                progress_cb(processed, result.total)
            continue
        base_email = f"user_{user_id}@{Config.PANEL_EMAIL_DOMAIN}"
        try:
            ok = await panel.extend_client_expiry(base_email, add_days)
        except Exception as exc:
            logger.warning("Bulk extend failed for %s: %s", user_id, exc)
            ok = False
        if ok:
            await db.reset_expiry_notifications(user_id)
            result.extended += 1
        else:
            result.failed += 1
        processed += 1
        if progress_cb:
            progress_cb(processed, result.total)
    return result


async def enqueue_broadcast_job(
    *,
    bot: Bot,
    user_ids: Iterable[int],
    text: str,
    audience: str,
    initiator_id: int,
    delay_sec: float = 0.03,
) -> BulkJob:
    users = [int(u) for u in user_ids]
    job = BulkJob(
        job_id=_new_job_id("broadcast"),
        kind="broadcast",
        initiator_id=int(initiator_id),
        audience=audience,
        created_at=time.time(),
        total=len(users),
    )
    _register_job(job)

    async def _runner() -> None:
        job.status = "running"
        job.started_at = time.time()
        try:
            result = await send_broadcast(
                bot=bot,
                user_ids=users,
                text=text,
                delay_sec=delay_sec,
                progress_cb=lambda processed, total: _update_progress(job, processed, total),
            )
            job.total = result.total
            job.processed = result.total
            job.sent = result.sent
            job.blocked = result.blocked
            job.failed = result.failed
            job.status = "done"
        except Exception as exc:
            logger.exception("Broadcast job %s failed", job.job_id, exc_info=exc)
            job.status = "failed"
            job.error = str(exc)[:500]
        finally:
            job.finished_at = time.time()

    asyncio.create_task(_runner(), name=f"bulk_job:{job.job_id}")
    return job


async def enqueue_extend_job(
    *,
    db: Database,
    panel: PanelAPI,
    add_days: int,
    initiator_id: int,
) -> BulkJob:
    job = BulkJob(
        job_id=_new_job_id("extend"),
        kind="extend_active",
        initiator_id=int(initiator_id),
        add_days=int(add_days),
        created_at=time.time(),
    )
    _register_job(job)

    async def _runner() -> None:
        job.status = "running"
        job.started_at = time.time()
        try:
            result = await extend_active_subscriptions_bulk(
                db=db,
                panel=panel,
                add_days=add_days,
                progress_cb=lambda processed, total: _update_progress(job, processed, total),
            )
            job.total = result.total
            job.processed = result.total
            job.extended = result.extended
            job.failed = result.failed
            job.status = "done"
        except Exception as exc:
            logger.exception("Extend job %s failed", job.job_id, exc_info=exc)
            job.status = "failed"
            job.error = str(exc)[:500]
        finally:
            job.finished_at = time.time()

    asyncio.create_task(_runner(), name=f"bulk_job:{job.job_id}")
    return job
