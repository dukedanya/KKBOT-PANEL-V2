from .loader import (
    load_tariffs,
    get_all_active,
    get_by_id,
    is_trial_plan,
    get_minimal_by_price,
    format_traffic,
    format_duration,
    format_price,
    format_stars_price,
    has_stars_provider_enabled,
    build_tariffs_text,
    build_buy_text,
    TARIFFS_ALL,
    TARIFFS_ACTIVE,
    TARIFFS_BY_ID,
)

try:
    load_tariffs()
except Exception:
    # Tariffs may be unavailable in some tooling contexts; runtime handlers will surface the error.
    pass
