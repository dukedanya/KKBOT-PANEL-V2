import logging
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import List

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = Path(BASE_DIR).parent
ENV_FILE_PATH = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(ENV_FILE_PATH)


def str_to_bool(val: str) -> bool:
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    app_mode: str
    webhook_host: str
    webhook_bind_host: str
    webhook_port: int
    itpay_webhook_path: str
    migrations_auto_apply: bool
    environment: str


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    level: str
    json: bool
    to_file: bool
    verify_ssl: bool


@dataclass(frozen=True, slots=True)
class JobsSettings:
    payment_reconcile_interval_sec: int
    expired_check_interval_sec: int
    stale_processing_timeout_min: int
    stale_processing_recovery_interval_sec: int
    healthcheck_interval_sec: int
    health_alert_cooldown_sec: int
    health_pending_age_min: int
    health_max_processing: int
    enable_expired_subscriptions_job: bool
    enable_payment_reconcile_job: bool
    enable_stale_payment_recovery_job: bool
    enable_health_monitor_job: bool
    enable_cleanup_payments_job: bool
    enable_referral_reminder_job: bool
    enable_expiry_notifications_job: bool
    enable_payment_attention_resolver_job: bool


@dataclass(frozen=True, slots=True)
class LimitsSettings:
    payment_create_cooldown_sec: int
    max_pending_payments_per_user: int
    max_withdraw_requests_per_day: int
    max_daily_ref_bonus_rub: float




@dataclass(frozen=True, slots=True)
class PaymentSettings:
    provider: str
    itpay_public_id: str
    itpay_api_secret: str
    itpay_webhook_secret: str
    yookassa_shop_id: str
    yookassa_secret_key: str
    yookassa_return_url: str
    yookassa_webhook_path: str
    yookassa_enforce_ip_check: bool
    telegram_stars_price_multiplier: float


@dataclass(frozen=True, slots=True)
class OperationalSettings:
    healthcheck_path: str
    readiness_path: str
    enable_health_endpoints: bool
    startup_recover_stale_processing: bool
    startup_fail_on_pending_migrations: bool
    startup_fail_on_schema_drift: bool
    payment_attention_operation_age_min: int
    payment_attention_event_lookback_hours: int
    payment_attention_resolve_interval_sec: int
    payment_attention_resolve_limit: int
    payment_attention_retry_base_min: int
    payment_attention_retry_backoff_multiplier: float
    payment_attention_retry_max_attempts: int


class Config:
    DEBUG: bool = str_to_bool(os.getenv("DEBUG", "false"))
    RELEASE_PROFILE_ENFORCED: bool = str_to_bool(os.getenv("RELEASE_PROFILE_ENFORCED", "false"))
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_USER_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()
    ]
    PAYMENT_CARD_NUMBER: str = os.getenv("PAYMENT_CARD_NUMBER", "")
    PANEL_BASE: str = os.getenv("PANEL_BASE", "").rstrip("/")
    SUB_PANEL_BASE: str = os.getenv("SUB_PANEL_BASE", "")
    MERGED_SUBSCRIPTION_API_BASE: str = os.getenv("MERGED_SUBSCRIPTION_API_BASE", "").strip()
    MERGED_SUBSCRIPTION_FORMAT: str = os.getenv("MERGED_SUBSCRIPTION_FORMAT", "base64").strip().lower() or "base64"
    MERGED_SUBSCRIPTION_INCLUDE_BASE_URL: bool = str_to_bool(os.getenv("MERGED_SUBSCRIPTION_INCLUDE_BASE_URL", "true"))
    LTE_REPORT_API_HEALTH_URL: str = os.getenv("LTE_REPORT_API_HEALTH_URL", "http://127.0.0.1:8787/health").strip()
    TOTAL_TRAFFIC_STATE_PATH: str = os.getenv("TOTAL_TRAFFIC_STATE_PATH", "/root/lte-whitelist/server/data/total-traffic-state.json").strip()
    TOTAL_TRAFFIC_STATE_URL: str = os.getenv("TOTAL_TRAFFIC_STATE_URL", "").strip()
    GRACE_STATE_PATH: str = os.getenv("GRACE_STATE_PATH", "/root/lte-whitelist/server/data/grace-state.json").strip()
    TOTAL_TRAFFIC_STATE_MAX_AGE_SEC: int = int(os.getenv("TOTAL_TRAFFIC_STATE_MAX_AGE_SEC", "1800"))
    DIRECT_SLOT_NOTICE_ENABLED: bool = str_to_bool(os.getenv("DIRECT_SLOT_NOTICE_ENABLED", "true"))
    ENABLE_SERVER_BOOTSTRAP: bool = str_to_bool(os.getenv("ENABLE_SERVER_BOOTSTRAP", "false"))
    SERVER_BOOTSTRAP_SOURCE_ROOT: str = os.getenv("SERVER_BOOTSTRAP_SOURCE_ROOT", "").strip()
    SERVER_BOOTSTRAP_REMOTE_ROOT: str = os.getenv("SERVER_BOOTSTRAP_REMOTE_ROOT", "/root/lte-whitelist").strip()
    SERVER_BOOTSTRAP_SSH_HOST: str = os.getenv("SERVER_BOOTSTRAP_SSH_HOST", "").strip()
    SERVER_BOOTSTRAP_SSH_PORT: int = int(os.getenv("SERVER_BOOTSTRAP_SSH_PORT", "22"))
    SERVER_BOOTSTRAP_SSH_USER: str = os.getenv("SERVER_BOOTSTRAP_SSH_USER", "root").strip()
    SERVER_BOOTSTRAP_SSH_PASSWORD: str = os.getenv("SERVER_BOOTSTRAP_SSH_PASSWORD", "").strip()
    SERVER_BOOTSTRAP_INSTALL_SYSTEMD: bool = str_to_bool(os.getenv("SERVER_BOOTSTRAP_INSTALL_SYSTEMD", "true"))
    SIDR_SUBSCRIPTION_TEMPLATE: str = os.getenv("SIDR_SUBSCRIPTION_TEMPLATE", "").strip()
    SIDR_SUBSCRIPTION_NAME: str = os.getenv("SIDR_SUBSCRIPTION_NAME", "Kakoito VPN").strip() or "Kakoito VPN"
    PANEL_LOGIN: str = os.getenv("PANEL_LOGIN", "")
    PANEL_PASSWORD: str = os.getenv("PANEL_PASSWORD", "")
    PANEL_TARGET_INBOUND_IDS: str = os.getenv("PANEL_TARGET_INBOUND_IDS", "1,2,3,4,5,6,7").strip()
    PANEL_TARGET_INBOUND_COUNT: int = int(os.getenv("PANEL_TARGET_INBOUND_COUNT", "0"))
    VERIFY_SSL: bool = str_to_bool(os.getenv("VERIFY_SSL", "true"))
    DATABASE_URL: str = os.getenv("DATABASE_URL", "").strip()
    DATABASE_MIN_POOL: int = int(os.getenv("DATABASE_MIN_POOL", "1"))
    DATABASE_MAX_POOL: int = int(os.getenv("DATABASE_MAX_POOL", "10"))
    DATA_DIR: str = os.getenv("DATA_DIR", str((PROJECT_ROOT / "data").resolve()))
    DATA_FILE: str = os.getenv("DATA_FILE", os.path.join(os.getenv("DATA_DIR", str((PROJECT_ROOT / "data").resolve())), "users.db"))
    SITE_URL: str = os.getenv("SITE_URL", "")
    TG_CHANNEL: str = os.getenv("TG_CHANNEL", "https://t.me/+XsoxseRgJa8yN2Ni")
    SUPPORT_URL: str = os.getenv("SUPPORT_URL", "")
    REF_BONUS_DAYS: int = int(os.getenv("REF_BONUS_DAYS", "7"))
    REF_PERCENT_LEVEL1: float = float(os.getenv("REF_PERCENT_LEVEL1", "25"))
    REF_PERCENT_LEVEL2: float = float(os.getenv("REF_PERCENT_LEVEL2", "10"))
    REF_PERCENT_LEVEL3: float = float(os.getenv("REF_PERCENT_LEVEL3", "5"))
    REFERRAL_MAX_NEW_INVITES_24H: int = int(os.getenv("REFERRAL_MAX_NEW_INVITES_24H", "30"))
    MIN_WITHDRAW: float = float(os.getenv("MIN_WITHDRAW", "300"))
    REF_FIRST_PAYMENT_DISCOUNT_PERCENT: float = float(os.getenv("REF_FIRST_PAYMENT_DISCOUNT_PERCENT", "15"))
    REFERRED_BONUS_DAYS: int = int(os.getenv("REFERRED_BONUS_DAYS", "5"))
    PANEL_EMAIL_DOMAIN: str = os.getenv("PANEL_EMAIL_DOMAIN", "kakoitovpn")
    PANEL_EMAIL_PREFIX: str = os.getenv("PANEL_EMAIL_PREFIX", "").strip()
    PAYMENT_PROVIDER: str = os.getenv("PAYMENT_PROVIDER", "itpay").strip().lower() or "itpay"
    PAYMENT_PROVIDERS: str = os.getenv("PAYMENT_PROVIDERS", "").strip()
    PUBLIC_BASE_URL: str = os.getenv("PUBLIC_BASE_URL", os.getenv("SITE_URL", "")).strip()
    ITPAY_PUBLIC_BASE_FALLBACK: str = os.getenv("ITPAY_PUBLIC_BASE_FALLBACK", "http://127.0.0.1").strip()
    ITPAY_PUBLIC_ID: str = os.getenv("ITPAY_PUBLIC_ID", "")
    ITPAY_API_SECRET: str = os.getenv("ITPAY_API_SECRET", "")
    ITPAY_WEBHOOK_SECRET: str = os.getenv("ITPAY_WEBHOOK_SECRET", "")
    YOOKASSA_SHOP_ID: str = os.getenv("YOOKASSA_SHOP_ID", "")
    YOOKASSA_SECRET_KEY: str = os.getenv("YOOKASSA_SECRET_KEY", "")
    YOOKASSA_RETURN_URL: str = os.getenv("YOOKASSA_RETURN_URL", "")
    YOOKASSA_WEBHOOK_PATH: str = os.getenv("YOOKASSA_WEBHOOK_PATH", "/yookassa/webhook")
    YOOKASSA_ENFORCE_IP_CHECK: bool = str_to_bool(os.getenv("YOOKASSA_ENFORCE_IP_CHECK", "false"))
    TELEGRAM_STARS_PRICE_MULTIPLIER: float = float(os.getenv("TELEGRAM_STARS_PRICE_MULTIPLIER", "1.0"))
    APP_MODE: str = os.getenv("APP_MODE", "polling").strip().lower() or "polling"
    WEBHOOK_HOST: str = os.getenv("WEBHOOK_HOST", "")
    WEBHOOK_BIND_HOST: str = os.getenv("WEBHOOK_BIND_HOST", "0.0.0.0")
    WEBHOOK_PORT: int = int(os.getenv("WEBHOOK_PORT", "8080"))
    ITPAY_WEBHOOK_PATH: str = os.getenv("ITPAY_WEBHOOK_PATH", "/itpay/webhook")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_JSON: bool = str_to_bool(os.getenv("LOG_JSON", "false"))
    LOG_TO_FILE: bool = str_to_bool(os.getenv("LOG_TO_FILE", "true"))
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    PAYMENT_RECONCILE_INTERVAL_SEC: int = int(os.getenv("PAYMENT_RECONCILE_INTERVAL_SEC", "120"))
    EXPIRED_CHECK_INTERVAL_SEC: int = int(os.getenv("EXPIRED_CHECK_INTERVAL_SEC", "300"))
    STALE_PROCESSING_TIMEOUT_MIN: int = int(os.getenv("STALE_PROCESSING_TIMEOUT_MIN", "15"))
    STALE_PROCESSING_RECOVERY_INTERVAL_SEC: int = int(os.getenv("STALE_PROCESSING_RECOVERY_INTERVAL_SEC", "300"))
    HEALTHCHECK_INTERVAL_SEC: int = int(os.getenv("HEALTHCHECK_INTERVAL_SEC", "120"))
    HEALTH_ALERT_COOLDOWN_SEC: int = int(os.getenv("HEALTH_ALERT_COOLDOWN_SEC", "900"))
    HEALTH_PENDING_AGE_MIN: int = int(os.getenv("HEALTH_PENDING_AGE_MIN", "10"))
    HEALTH_MAX_PROCESSING: int = int(os.getenv("HEALTH_MAX_PROCESSING", "3"))
    PAYMENT_CREATE_COOLDOWN_SEC: int = int(os.getenv("PAYMENT_CREATE_COOLDOWN_SEC", "10"))
    MAX_PENDING_PAYMENTS_PER_USER: int = int(os.getenv("MAX_PENDING_PAYMENTS_PER_USER", "3"))
    MAX_WITHDRAW_REQUESTS_PER_DAY: int = int(os.getenv("MAX_WITHDRAW_REQUESTS_PER_DAY", "3"))
    MAX_DAILY_REF_BONUS_RUB: float = float(os.getenv("MAX_DAILY_REF_BONUS_RUB", "5000"))
    MIGRATIONS_AUTO_APPLY: bool = str_to_bool(os.getenv("MIGRATIONS_AUTO_APPLY", "true"))
    ENABLE_EXPIRED_SUBSCRIPTIONS_JOB: bool = str_to_bool(os.getenv("ENABLE_EXPIRED_SUBSCRIPTIONS_JOB", "true"))
    ENABLE_PAYMENT_RECONCILE_JOB: bool = str_to_bool(os.getenv("ENABLE_PAYMENT_RECONCILE_JOB", "true"))
    ENABLE_STALE_PAYMENT_RECOVERY_JOB: bool = str_to_bool(os.getenv("ENABLE_STALE_PAYMENT_RECOVERY_JOB", "true"))
    ENABLE_HEALTH_MONITOR_JOB: bool = str_to_bool(os.getenv("ENABLE_HEALTH_MONITOR_JOB", "true"))
    ENABLE_CLEANUP_PAYMENTS_JOB: bool = str_to_bool(os.getenv("ENABLE_CLEANUP_PAYMENTS_JOB", "true"))
    ENABLE_REFERRAL_REMINDER_JOB: bool = str_to_bool(os.getenv("ENABLE_REFERRAL_REMINDER_JOB", "true"))
    ENABLE_EXPIRY_NOTIFICATIONS_JOB: bool = str_to_bool(os.getenv("ENABLE_EXPIRY_NOTIFICATIONS_JOB", "true"))
    ENABLE_PAYMENT_ATTENTION_RESOLVER_JOB: bool = str_to_bool(os.getenv("ENABLE_PAYMENT_ATTENTION_RESOLVER_JOB", "true"))
    HEALTHCHECK_PATH: str = os.getenv("HEALTHCHECK_PATH", "/healthz")
    READINESS_PATH: str = os.getenv("READINESS_PATH", "/readyz")
    ENABLE_HEALTH_ENDPOINTS: bool = str_to_bool(os.getenv("ENABLE_HEALTH_ENDPOINTS", "true"))
    STARTUP_RECOVER_STALE_PROCESSING: bool = str_to_bool(os.getenv("STARTUP_RECOVER_STALE_PROCESSING", "true"))
    STARTUP_FAIL_ON_PENDING_MIGRATIONS: bool = str_to_bool(os.getenv("STARTUP_FAIL_ON_PENDING_MIGRATIONS", "false"))
    STARTUP_FAIL_ON_SCHEMA_DRIFT: bool = str_to_bool(os.getenv("STARTUP_FAIL_ON_SCHEMA_DRIFT", "false"))
    PAYMENT_ATTENTION_OPERATION_AGE_MIN: int = int(os.getenv("PAYMENT_ATTENTION_OPERATION_AGE_MIN", "20"))
    PAYMENT_ATTENTION_EVENT_LOOKBACK_HOURS: int = int(os.getenv("PAYMENT_ATTENTION_EVENT_LOOKBACK_HOURS", "24"))
    PAYMENT_ATTENTION_RESOLVE_INTERVAL_SEC: int = int(os.getenv("PAYMENT_ATTENTION_RESOLVE_INTERVAL_SEC", "300"))
    PAYMENT_ATTENTION_RESOLVE_LIMIT: int = int(os.getenv("PAYMENT_ATTENTION_RESOLVE_LIMIT", "10"))
    PAYMENT_ATTENTION_RETRY_BASE_MIN: int = int(os.getenv("PAYMENT_ATTENTION_RETRY_BASE_MIN", "15"))
    PAYMENT_ATTENTION_RETRY_BACKOFF_MULTIPLIER: float = float(os.getenv("PAYMENT_ATTENTION_RETRY_BACKOFF_MULTIPLIER", "2.0"))
    PAYMENT_ATTENTION_RETRY_MAX_ATTEMPTS: int = int(os.getenv("PAYMENT_ATTENTION_RETRY_MAX_ATTEMPTS", "5"))
    PAYMENT_ACTIVATION_RETRY_BASE_SEC: int = int(os.getenv("PAYMENT_ACTIVATION_RETRY_BASE_SEC", "60"))
    PAYMENT_ACTIVATION_RETRY_MAX_SEC: int = int(os.getenv("PAYMENT_ACTIVATION_RETRY_MAX_SEC", "1800"))
    PAYMENT_ACTIVATION_MAX_ATTEMPTS: int = int(os.getenv("PAYMENT_ACTIVATION_MAX_ATTEMPTS", "5"))
    START_COMMAND_DEDUP_WINDOW_SEC: float = float(os.getenv("START_COMMAND_DEDUP_WINDOW_SEC", "2.0"))
    COMMAND_RATE_LIMIT_SEC: float = float(os.getenv("COMMAND_RATE_LIMIT_SEC", "0.8"))
    CALLBACK_RATE_LIMIT_SEC: float = float(os.getenv("CALLBACK_RATE_LIMIT_SEC", "0.35"))
    CALLBACK_DEDUP_WINDOW_SEC: float = float(os.getenv("CALLBACK_DEDUP_WINDOW_SEC", "1.5"))
    ERROR_ALERT_COOLDOWN_SEC: int = int(os.getenv("ERROR_ALERT_COOLDOWN_SEC", "120"))
    DAILY_ADMIN_REPORT_HOUR_UTC: int = int(os.getenv("DAILY_ADMIN_REPORT_HOUR_UTC", "6"))
    DAILY_INCIDENT_REPORT_HOUR_UTC: int = int(os.getenv("DAILY_INCIDENT_REPORT_HOUR_UTC", "7"))
    SUPPORT_ARCHIVE_AFTER_DAYS: int = int(os.getenv("SUPPORT_ARCHIVE_AFTER_DAYS", "14"))
    SUPPORT_BLACKLIST_NOTIFY_ADMINS: bool = str_to_bool(os.getenv("SUPPORT_BLACKLIST_NOTIFY_ADMINS", "true"))
    SUPPORT_TICKET_REMINDER_AFTER_MIN: int = int(os.getenv("SUPPORT_TICKET_REMINDER_AFTER_MIN", "45"))
    SUPPORT_TICKET_REMINDER_INTERVAL_MIN: int = int(os.getenv("SUPPORT_TICKET_REMINDER_INTERVAL_MIN", "180"))
    GIFT_LINK_REMINDER_AFTER_HOURS: int = int(os.getenv("GIFT_LINK_REMINDER_AFTER_HOURS", "24"))
    GIFT_LINK_REMINDER_INTERVAL_HOURS: int = int(os.getenv("GIFT_LINK_REMINDER_INTERVAL_HOURS", "24"))
    SERVICE_MESSAGE_CLEANUP_INTERVAL_SEC: int = int(os.getenv("SERVICE_MESSAGE_CLEANUP_INTERVAL_SEC", "1800"))
    TRANSIENT_MESSAGE_DEFAULT_TTL_HOURS: int = int(os.getenv("TRANSIENT_MESSAGE_DEFAULT_TTL_HOURS", "24"))
    BACKUP_DIR: str = os.getenv("BACKUP_DIR", os.path.join(DATA_DIR, "backups")).strip()
    BACKUP_KEEP: int = int(os.getenv("BACKUP_KEEP", "14"))


    @classmethod
    def stars_price_multiplier(cls) -> float:
        return float(cls.TELEGRAM_STARS_PRICE_MULTIPLIER or 1.0)

    @classmethod
    def is_production(cls) -> bool:
        return (cls.ENVIRONMENT or "").strip().lower() == "production"

    @classmethod
    def effective_startup_fail_on_pending_migrations(cls) -> bool:
        if cls.STARTUP_FAIL_ON_PENDING_MIGRATIONS:
            return True
        return cls.is_production() and cls.RELEASE_PROFILE_ENFORCED

    @classmethod
    def effective_startup_fail_on_schema_drift(cls) -> bool:
        if cls.STARTUP_FAIL_ON_SCHEMA_DRIFT:
            return True
        return cls.is_production() and cls.RELEASE_PROFILE_ENFORCED

    @classmethod
    def public_base_url(cls) -> str:
        for value in (cls.PUBLIC_BASE_URL, cls.WEBHOOK_HOST, cls.SITE_URL):
            value = (value or "").strip().rstrip("/")
            if value and not value.startswith("https://t.me/") and not value.startswith("http://t.me/"):
                return value
        return ""

    @classmethod
    def set_stars_price_multiplier(cls, value: float) -> None:
        cls.TELEGRAM_STARS_PRICE_MULTIPLIER = float(value)

    @classmethod
    def set_panel_target_inbound_ids(cls, value: str) -> None:
        cls.PANEL_TARGET_INBOUND_IDS = (value or "").strip()

    @classmethod
    def set_panel_target_inbound_count(cls, value: int) -> None:
        cls.PANEL_TARGET_INBOUND_COUNT = max(0, int(value))

    @classmethod
    def runtime_settings(cls) -> RuntimeSettings:
        return RuntimeSettings(
            app_mode=cls.APP_MODE,
            webhook_host=cls.WEBHOOK_HOST,
            webhook_bind_host=cls.WEBHOOK_BIND_HOST,
            webhook_port=cls.WEBHOOK_PORT,
            itpay_webhook_path=cls.ITPAY_WEBHOOK_PATH,
            migrations_auto_apply=cls.MIGRATIONS_AUTO_APPLY,
            environment=cls.ENVIRONMENT,
        )

    @classmethod
    def logging_settings(cls) -> LoggingSettings:
        return LoggingSettings(
            level=cls.LOG_LEVEL,
            json=cls.LOG_JSON,
            to_file=cls.LOG_TO_FILE,
            verify_ssl=cls.VERIFY_SSL,
        )

    @classmethod
    def jobs_settings(cls) -> JobsSettings:
        return JobsSettings(
            payment_reconcile_interval_sec=cls.PAYMENT_RECONCILE_INTERVAL_SEC,
            expired_check_interval_sec=cls.EXPIRED_CHECK_INTERVAL_SEC,
            stale_processing_timeout_min=cls.STALE_PROCESSING_TIMEOUT_MIN,
            stale_processing_recovery_interval_sec=cls.STALE_PROCESSING_RECOVERY_INTERVAL_SEC,
            healthcheck_interval_sec=cls.HEALTHCHECK_INTERVAL_SEC,
            health_alert_cooldown_sec=cls.HEALTH_ALERT_COOLDOWN_SEC,
            health_pending_age_min=cls.HEALTH_PENDING_AGE_MIN,
            health_max_processing=cls.HEALTH_MAX_PROCESSING,
            enable_expired_subscriptions_job=cls.ENABLE_EXPIRED_SUBSCRIPTIONS_JOB,
            enable_payment_reconcile_job=cls.ENABLE_PAYMENT_RECONCILE_JOB,
            enable_stale_payment_recovery_job=cls.ENABLE_STALE_PAYMENT_RECOVERY_JOB,
            enable_health_monitor_job=cls.ENABLE_HEALTH_MONITOR_JOB,
            enable_cleanup_payments_job=cls.ENABLE_CLEANUP_PAYMENTS_JOB,
            enable_referral_reminder_job=cls.ENABLE_REFERRAL_REMINDER_JOB,
            enable_expiry_notifications_job=cls.ENABLE_EXPIRY_NOTIFICATIONS_JOB,
            enable_payment_attention_resolver_job=cls.ENABLE_PAYMENT_ATTENTION_RESOLVER_JOB,
        )

    @classmethod
    def limits_settings(cls) -> LimitsSettings:
        return LimitsSettings(
            payment_create_cooldown_sec=cls.PAYMENT_CREATE_COOLDOWN_SEC,
            max_pending_payments_per_user=cls.MAX_PENDING_PAYMENTS_PER_USER,
            max_withdraw_requests_per_day=cls.MAX_WITHDRAW_REQUESTS_PER_DAY,
            max_daily_ref_bonus_rub=cls.MAX_DAILY_REF_BONUS_RUB,
        )

    @classmethod
    def payment_settings(cls) -> PaymentSettings:
        return PaymentSettings(
            provider=cls.PAYMENT_PROVIDER,
            itpay_public_id=cls.ITPAY_PUBLIC_ID,
            itpay_api_secret=cls.ITPAY_API_SECRET,
            itpay_webhook_secret=cls.ITPAY_WEBHOOK_SECRET,
            yookassa_shop_id=cls.YOOKASSA_SHOP_ID,
            yookassa_secret_key=cls.YOOKASSA_SECRET_KEY,
            yookassa_return_url=cls.YOOKASSA_RETURN_URL,
            yookassa_webhook_path=cls.YOOKASSA_WEBHOOK_PATH,
            yookassa_enforce_ip_check=cls.YOOKASSA_ENFORCE_IP_CHECK,
            telegram_stars_price_multiplier=cls.TELEGRAM_STARS_PRICE_MULTIPLIER,
        )

    @classmethod
    def operational_settings(cls) -> OperationalSettings:
        return OperationalSettings(
            healthcheck_path=cls.HEALTHCHECK_PATH,
            readiness_path=cls.READINESS_PATH,
            enable_health_endpoints=cls.ENABLE_HEALTH_ENDPOINTS,
            startup_recover_stale_processing=cls.STARTUP_RECOVER_STALE_PROCESSING,
            startup_fail_on_pending_migrations=cls.STARTUP_FAIL_ON_PENDING_MIGRATIONS,
            startup_fail_on_schema_drift=cls.STARTUP_FAIL_ON_SCHEMA_DRIFT,
            payment_attention_operation_age_min=cls.PAYMENT_ATTENTION_OPERATION_AGE_MIN,
            payment_attention_event_lookback_hours=cls.PAYMENT_ATTENTION_EVENT_LOOKBACK_HOURS,
            payment_attention_resolve_interval_sec=cls.PAYMENT_ATTENTION_RESOLVE_INTERVAL_SEC,
            payment_attention_resolve_limit=cls.PAYMENT_ATTENTION_RESOLVE_LIMIT,
            payment_attention_retry_base_min=cls.PAYMENT_ATTENTION_RETRY_BASE_MIN,
            payment_attention_retry_backoff_multiplier=cls.PAYMENT_ATTENTION_RETRY_BACKOFF_MULTIPLIER,
            payment_attention_retry_max_attempts=cls.PAYMENT_ATTENTION_RETRY_MAX_ATTEMPTS,
        )

    @classmethod
    def validate_startup(cls) -> list[str]:
        errors: list[str] = []
        runtime = cls.runtime_settings()
        jobs = cls.jobs_settings()
        limits = cls.limits_settings()
        payment = cls.payment_settings()
        operational = cls.operational_settings()

        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN is required")
        if not cls.ADMIN_USER_IDS:
            errors.append("ADMIN_USER_IDS must contain at least one Telegram user id")
        if not cls.PANEL_BASE:
            errors.append("PANEL_BASE is required")
        if not cls.PANEL_LOGIN:
            errors.append("PANEL_LOGIN is required")
        if not cls.PANEL_PASSWORD:
            errors.append("PANEL_PASSWORD is required")
        if payment.provider not in {"itpay", "yookassa", "telegram_stars"}:
            errors.append("PAYMENT_PROVIDER must be one of: itpay, yookassa, telegram_stars")

        if cls.PAYMENT_PROVIDERS:
            parsed = [(item or "").strip().lower() for item in cls.PAYMENT_PROVIDERS.split(",") if (item or "").strip()]
            invalid = [item for item in parsed if item not in {"itpay", "yookassa", "telegram_stars"}]
            if invalid:
                errors.append("PAYMENT_PROVIDERS contains invalid values: " + ", ".join(invalid))

        if payment.provider == "itpay":
            if not cls.ITPAY_PUBLIC_ID:
                errors.append("ITPAY_PUBLIC_ID is required for PAYMENT_PROVIDER=itpay")
            if not cls.ITPAY_API_SECRET:
                errors.append("ITPAY_API_SECRET is required for PAYMENT_PROVIDER=itpay")

        if payment.provider == "yookassa":
            if not cls.YOOKASSA_SHOP_ID:
                errors.append("YOOKASSA_SHOP_ID is required for PAYMENT_PROVIDER=yookassa")
            if not cls.YOOKASSA_SECRET_KEY:
                errors.append("YOOKASSA_SECRET_KEY is required for PAYMENT_PROVIDER=yookassa")
            if not (cls.YOOKASSA_RETURN_URL or cls.TG_CHANNEL):
                errors.append("YOOKASSA_RETURN_URL or TG_CHANNEL is required for PAYMENT_PROVIDER=yookassa")
            if not cls.YOOKASSA_WEBHOOK_PATH.startswith("/"):
                errors.append("YOOKASSA_WEBHOOK_PATH must start with '/'")

        if payment.provider == "telegram_stars" and payment.telegram_stars_price_multiplier <= 0:
            errors.append("TELEGRAM_STARS_PRICE_MULTIPLIER must be greater than 0 for PAYMENT_PROVIDER=telegram_stars")

        if runtime.app_mode not in {"polling", "webhook"}:
            errors.append("APP_MODE must be either polling or webhook")
        if runtime.app_mode == "webhook":
            if not runtime.webhook_host:
                errors.append("WEBHOOK_HOST is required when APP_MODE=webhook")
            if not runtime.itpay_webhook_path.startswith("/"):
                errors.append("ITPAY_WEBHOOK_PATH must start with '/'")
        if runtime.webhook_port < 1 or runtime.webhook_port > 65535:
            errors.append("WEBHOOK_PORT must be between 1 and 65535")
        if not operational.healthcheck_path.startswith("/"):
            errors.append("HEALTHCHECK_PATH must start with '/'")
        if not operational.readiness_path.startswith("/"):
            errors.append("READINESS_PATH must start with '/'")
        if operational.healthcheck_path == operational.readiness_path:
            errors.append("HEALTHCHECK_PATH and READINESS_PATH must be different")
        if not os.path.isdir(cls.DATA_DIR):
            try:
                os.makedirs(cls.DATA_DIR, exist_ok=True)
            except OSError as exc:
                errors.append(f"DATA_DIR is not writable: {exc}")
        if os.path.isdir(cls.DATA_DIR) and not os.access(cls.DATA_DIR, os.W_OK):
            errors.append("DATA_DIR must be writable")

        if jobs.payment_reconcile_interval_sec < 30:
            errors.append("PAYMENT_RECONCILE_INTERVAL_SEC must be >= 30")
        if jobs.expired_check_interval_sec < 60:
            errors.append("EXPIRED_CHECK_INTERVAL_SEC must be >= 60")
        if jobs.stale_processing_timeout_min < 1:
            errors.append("STALE_PROCESSING_TIMEOUT_MIN must be >= 1")
        if jobs.stale_processing_recovery_interval_sec < 60:
            errors.append("STALE_PROCESSING_RECOVERY_INTERVAL_SEC must be >= 60")
        if jobs.healthcheck_interval_sec < 30:
            errors.append("HEALTHCHECK_INTERVAL_SEC must be >= 30")
        if jobs.health_alert_cooldown_sec < 60:
            errors.append("HEALTH_ALERT_COOLDOWN_SEC must be >= 60")
        if jobs.health_pending_age_min < 1:
            errors.append("HEALTH_PENDING_AGE_MIN must be >= 1")
        if jobs.health_max_processing < 1:
            errors.append("HEALTH_MAX_PROCESSING must be >= 1")
        if limits.payment_create_cooldown_sec < 0:
            errors.append("PAYMENT_CREATE_COOLDOWN_SEC must be >= 0")
        if limits.max_pending_payments_per_user < 1:
            errors.append("MAX_PENDING_PAYMENTS_PER_USER must be >= 1")
        if limits.max_withdraw_requests_per_day < 1:
            errors.append("MAX_WITHDRAW_REQUESTS_PER_DAY must be >= 1")
        if limits.max_daily_ref_bonus_rub < 0:
            errors.append("MAX_DAILY_REF_BONUS_RUB must be >= 0")
        if cls.PAYMENT_ACTIVATION_RETRY_BASE_SEC < 5:
            errors.append("PAYMENT_ACTIVATION_RETRY_BASE_SEC must be >= 5")
        if cls.PAYMENT_ACTIVATION_RETRY_MAX_SEC < cls.PAYMENT_ACTIVATION_RETRY_BASE_SEC:
            errors.append("PAYMENT_ACTIVATION_RETRY_MAX_SEC must be >= PAYMENT_ACTIVATION_RETRY_BASE_SEC")
        if cls.PAYMENT_ACTIVATION_MAX_ATTEMPTS < 1:
            errors.append("PAYMENT_ACTIVATION_MAX_ATTEMPTS must be >= 1")
        if cls.START_COMMAND_DEDUP_WINDOW_SEC < 0:
            errors.append("START_COMMAND_DEDUP_WINDOW_SEC must be >= 0")
        if cls.COMMAND_RATE_LIMIT_SEC < 0:
            errors.append("COMMAND_RATE_LIMIT_SEC must be >= 0")
        if cls.CALLBACK_RATE_LIMIT_SEC < 0:
            errors.append("CALLBACK_RATE_LIMIT_SEC must be >= 0")
        if cls.CALLBACK_DEDUP_WINDOW_SEC < 0:
            errors.append("CALLBACK_DEDUP_WINDOW_SEC must be >= 0")
        if cls.ERROR_ALERT_COOLDOWN_SEC < 0:
            errors.append("ERROR_ALERT_COOLDOWN_SEC must be >= 0")
        if cls.DAILY_ADMIN_REPORT_HOUR_UTC < 0 or cls.DAILY_ADMIN_REPORT_HOUR_UTC > 23:
            errors.append("DAILY_ADMIN_REPORT_HOUR_UTC must be between 0 and 23")
        if cls.SUPPORT_ARCHIVE_AFTER_DAYS < 1:
            errors.append("SUPPORT_ARCHIVE_AFTER_DAYS must be >= 1")
        if cls.SERVICE_MESSAGE_CLEANUP_INTERVAL_SEC < 60:
            errors.append("SERVICE_MESSAGE_CLEANUP_INTERVAL_SEC must be >= 60")
        if cls.TRANSIENT_MESSAGE_DEFAULT_TTL_HOURS < 1:
            errors.append("TRANSIENT_MESSAGE_DEFAULT_TTL_HOURS must be >= 1")

        if cls.is_production() and cls.RELEASE_PROFILE_ENFORCED:
            if cls.DEBUG:
                errors.append("DEBUG must be false in production when RELEASE_PROFILE_ENFORCED=true")
            if cls.LOG_LEVEL == "DEBUG":
                errors.append("LOG_LEVEL=DEBUG is not allowed in production when RELEASE_PROFILE_ENFORCED=true")
            if not cls.VERIFY_SSL:
                errors.append("VERIFY_SSL must be true in production when RELEASE_PROFILE_ENFORCED=true")
            if cls.PAYMENT_CREATE_COOLDOWN_SEC < 3:
                errors.append("PAYMENT_CREATE_COOLDOWN_SEC must be >= 3 in production when RELEASE_PROFILE_ENFORCED=true")
            if not cls.effective_startup_fail_on_pending_migrations():
                errors.append("Pending migrations must fail startup in production when RELEASE_PROFILE_ENFORCED=true")
            if not cls.effective_startup_fail_on_schema_drift():
                errors.append("Schema drift must fail startup in production when RELEASE_PROFILE_ENFORCED=true")
        return errors

    @classmethod
    def startup_summary(cls) -> dict:
        runtime = cls.runtime_settings()
        logging_settings = cls.logging_settings()
        jobs = cls.jobs_settings()
        limits = cls.limits_settings()
        payment = cls.payment_settings()
        payment = cls.payment_settings()
        operational = cls.operational_settings()
        return {
            "environment": runtime.environment,
            "debug": cls.DEBUG,
            "release_profile_enforced": cls.RELEASE_PROFILE_ENFORCED,
            "app_mode": runtime.app_mode,
            "verify_ssl": logging_settings.verify_ssl,
            "log_level": logging_settings.level,
            "log_json": logging_settings.json,
            "log_to_file": logging_settings.to_file,
            "data_file": cls.DATA_FILE,
            "admin_count": len(cls.ADMIN_USER_IDS),
            "jobs": {
                "payment_reconcile_interval_sec": jobs.payment_reconcile_interval_sec,
                "expired_check_interval_sec": jobs.expired_check_interval_sec,
                "stale_processing_timeout_min": jobs.stale_processing_timeout_min,
                "stale_processing_recovery_interval_sec": jobs.stale_processing_recovery_interval_sec,
                "healthcheck_interval_sec": jobs.healthcheck_interval_sec,
                "health_alert_cooldown_sec": jobs.health_alert_cooldown_sec,
                "health_pending_age_min": jobs.health_pending_age_min,
                "health_max_processing": jobs.health_max_processing,
                "expired_subscriptions": jobs.enable_expired_subscriptions_job,
                "payment_reconcile": jobs.enable_payment_reconcile_job,
                "stale_payment_recovery": jobs.enable_stale_payment_recovery_job,
                "health_monitor": jobs.enable_health_monitor_job,
                "cleanup_payments": jobs.enable_cleanup_payments_job,
                "referral_reminder": jobs.enable_referral_reminder_job,
                "expiry_notifications": jobs.enable_expiry_notifications_job,
            },
            "limits": {
                "payment_create_cooldown_sec": limits.payment_create_cooldown_sec,
                "max_pending_payments_per_user": limits.max_pending_payments_per_user,
                "max_withdraw_requests_per_day": limits.max_withdraw_requests_per_day,
                "max_daily_ref_bonus_rub": limits.max_daily_ref_bonus_rub,
            },
            "payment_provider": payment.provider,
            "migrations_auto_apply": runtime.migrations_auto_apply,
            "health_endpoints_enabled": operational.enable_health_endpoints,
            "itpay_webhook_enabled": payment.provider == "itpay",
            "yookassa_webhook_enabled": payment.provider == "yookassa",
            "telegram_stars_enabled": payment.provider == "telegram_stars",
            "webhook_listener": (
                f"{runtime.webhook_bind_host}:{runtime.webhook_port}{(cls.YOOKASSA_WEBHOOK_PATH if payment.provider == 'yookassa' else runtime.itpay_webhook_path)}"
                if runtime.app_mode == "webhook" and payment.provider in {"itpay", "yookassa"}
                else None
            ),
            "healthcheck_path": operational.healthcheck_path,
            "readiness_path": operational.readiness_path,
            "startup_recover_stale_processing": operational.startup_recover_stale_processing,
            "startup_fail_on_pending_migrations": cls.effective_startup_fail_on_pending_migrations(),
            "startup_fail_on_schema_drift": cls.effective_startup_fail_on_schema_drift(),
            "payment_activation_retry_base_sec": cls.PAYMENT_ACTIVATION_RETRY_BASE_SEC,
            "payment_activation_retry_max_sec": cls.PAYMENT_ACTIVATION_RETRY_MAX_SEC,
            "payment_activation_max_attempts": cls.PAYMENT_ACTIVATION_MAX_ATTEMPTS,
            "start_command_dedup_window_sec": cls.START_COMMAND_DEDUP_WINDOW_SEC,
            "command_rate_limit_sec": cls.COMMAND_RATE_LIMIT_SEC,
            "callback_rate_limit_sec": cls.CALLBACK_RATE_LIMIT_SEC,
            "callback_dedup_window_sec": cls.CALLBACK_DEDUP_WINDOW_SEC,
            "error_alert_cooldown_sec": cls.ERROR_ALERT_COOLDOWN_SEC,
            "daily_admin_report_hour_utc": cls.DAILY_ADMIN_REPORT_HOUR_UTC,
            "support_archive_after_days": cls.SUPPORT_ARCHIVE_AFTER_DAYS,
            "service_message_cleanup_interval_sec": cls.SERVICE_MESSAGE_CLEANUP_INTERVAL_SEC,
            "transient_message_default_ttl_hours": cls.TRANSIENT_MESSAGE_DEFAULT_TTL_HOURS,
            "backup_dir": cls.BACKUP_DIR,
            "backup_keep": cls.BACKUP_KEEP,
        }

    @classmethod
    def sync_missing_env_variables(cls) -> list[str]:
        env_path = Path(ENV_FILE_PATH)
        example_paths = [
            Path(BASE_DIR) / ".env.example",
            Path(BASE_DIR) / ".env.release.example",
            Path(BASE_DIR) / ".env.polling.example",
            Path(BASE_DIR) / ".env.webhook.example",
        ]

        existing_lines: list[str] = []
        existing_keys: set[str] = set()
        if env_path.exists():
            existing_lines = env_path.read_text(encoding="utf-8").splitlines()
            for line in existing_lines:
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key = stripped.split("=", 1)[0].strip()
                if key:
                    existing_keys.add(key)

        missing_lines: list[str] = []
        seen_missing_keys: set[str] = set()
        for path in example_paths:
            if not path.exists():
                continue
            for raw_line in path.read_text(encoding="utf-8").splitlines():
                stripped = raw_line.strip()
                if not stripped or stripped.startswith("#") or "=" not in raw_line:
                    continue
                key = raw_line.split("=", 1)[0].strip()
                if not key or key in existing_keys or key in seen_missing_keys:
                    continue
                missing_lines.append(raw_line)
                seen_missing_keys.add(key)

        if not missing_lines:
            return []

        output_lines = list(existing_lines)
        if output_lines and output_lines[-1].strip():
            output_lines.append("")
        output_lines.append("# Auto-added missing variables from .env examples")
        output_lines.extend(missing_lines)
        env_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
        return [line.split("=", 1)[0].strip() for line in missing_lines]

    @classmethod
    def detect_duplicate_env_variables(cls) -> list[str]:
        env_path = Path(ENV_FILE_PATH)
        if not env_path.exists():
            return []

        keys: list[str] = []
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key = stripped.split("=", 1)[0].strip()
            if key:
                keys.append(key)

        counts = Counter(keys)
        return [key for key, count in counts.items() if count > 1]


try:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
except OSError as exc:
    logger.warning("Не удалось создать DATA_DIR %s: %s", Config.DATA_DIR, exc)
