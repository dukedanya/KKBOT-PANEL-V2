CREATE TABLE IF NOT EXISTS antifraud_events (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    event_type TEXT NOT NULL DEFAULT '',
    severity TEXT NOT NULL DEFAULT 'warning',
    details TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_antifraud_events_user_id_created_at
    ON antifraud_events(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS admin_user_actions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    admin_user_id BIGINT NOT NULL,
    action TEXT NOT NULL DEFAULT '',
    details TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_admin_user_actions_user_id_created_at
    ON admin_user_actions(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS payment_admin_actions (
    id BIGSERIAL PRIMARY KEY,
    payment_id TEXT NOT NULL DEFAULT '',
    admin_user_id BIGINT NOT NULL,
    action TEXT NOT NULL DEFAULT '',
    provider TEXT NOT NULL DEFAULT '',
    result TEXT NOT NULL DEFAULT '',
    details TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_admin_actions_payment_id_created_at
    ON payment_admin_actions(payment_id, created_at DESC);

CREATE TABLE IF NOT EXISTS payment_event_dedup (
    event_key TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL DEFAULT '',
    payload_excerpt TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_event_dedup_payment_id_created_at
    ON payment_event_dedup(payment_id, created_at DESC);
