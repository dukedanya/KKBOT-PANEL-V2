import json
import logging
import os
from datetime import datetime

from app.log_context import get_log_context
from config import Config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
LEVEL_LABELS = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "CRIT",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting only
        log_context = get_log_context()
        payload = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", log_context.get("request_id", "-")),
            "ticket_id": getattr(record, "ticket_id", log_context.get("ticket_id", "-")),
            "payment_id": getattr(record, "payment_id", log_context.get("payment_id", "-")),
            "user_id": getattr(record, "user_id", log_context.get("user_id", "-")),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - formatting only
        log_context = get_log_context()
        record.request_id = getattr(record, "request_id", log_context.get("request_id", "-"))
        record.ticket_id = getattr(record, "ticket_id", log_context.get("ticket_id", "-"))
        record.payment_id = getattr(record, "payment_id", log_context.get("payment_id", "-"))
        record.user_id = getattr(record, "user_id", log_context.get("user_id", "-"))
        return True


class HumanConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting only
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level = LEVEL_LABELS.get(record.levelno, record.levelname)
        logger_name = record.name.replace("services.", "svc.").replace("handlers.", "hdl.")
        logger_name = logger_name.replace("middlewares.", "mw.").replace("utils.", "util.")

        context_parts = []
        for label, attr in (
            ("user", "user_id"),
            ("req", "request_id"),
            ("ticket", "ticket_id"),
            ("pay", "payment_id"),
        ):
            value = getattr(record, attr, "-")
            if value and value != "-":
                context_parts.append(f"{label}={value}")

        context_suffix = f" [{' '.join(context_parts)}]" if context_parts else ""
        message = record.getMessage().strip()
        rendered = f"{timestamp} | {level:<13} | {logger_name} | {message}{context_suffix}"

        if record.exc_info:
            return f"{rendered}\n{self.formatException(record.exc_info)}"
        return rendered


def build_file_formatter() -> logging.Formatter:
    if Config.LOG_JSON:
        return JsonFormatter()
    return logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s "
        "[req=%(request_id)s ticket=%(ticket_id)s payment=%(payment_id)s user=%(user_id)s]"
    )


def tune_external_loggers() -> None:
    # Keep the bot logs readable by default and reduce third-party chatter.
    for name, level in {
        "aiogram": logging.INFO,
        "aiohttp": logging.WARNING,
        "httpx": logging.WARNING,
        "httpcore": logging.WARNING,
        "urllib3": logging.WARNING,
        "asyncio": logging.WARNING,
    }.items():
        logging.getLogger(name).setLevel(level)



def configure_logging() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
    handlers: list[tuple[logging.Handler, logging.Formatter]] = [
        (logging.StreamHandler(), HumanConsoleFormatter())
    ]
    if Config.LOG_TO_FILE:
        file_handler = logging.FileHandler(
            os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log"),
            encoding="utf-8",
        )
        handlers.append((file_handler, build_file_formatter()))

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    context_filter = ContextFilter()
    for handler, formatter in handlers:
        handler.addFilter(context_filter)
        handler.setFormatter(formatter)
        root.addHandler(handler)
    tune_external_loggers()
