CREATE TABLE IF NOT EXISTS withdraw_requests (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount NUMERIC(14, 2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_withdraw_requests_user_id ON withdraw_requests(user_id);

CREATE TABLE IF NOT EXISTS payment_intents (
    payment_id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    plan_id TEXT NOT NULL DEFAULT '',
    amount NUMERIC(14, 2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    provider TEXT NOT NULL DEFAULT '',
    provider_payment_id TEXT NOT NULL DEFAULT '',
    msg_id BIGINT,
    recipient_user_id BIGINT,
    promo_code TEXT NOT NULL DEFAULT '',
    promo_discount_percent NUMERIC(8, 2) NOT NULL DEFAULT 0,
    gift_label TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    activation_attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    processing_started_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_payment_intents_user_id ON payment_intents(user_id);
CREATE INDEX IF NOT EXISTS idx_payment_intents_status_created_at ON payment_intents(status, created_at);

CREATE TABLE IF NOT EXISTS payment_status_history (
    id BIGSERIAL PRIMARY KEY,
    payment_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_payment_status_history_payment_id ON payment_status_history(payment_id);

CREATE TABLE IF NOT EXISTS support_tickets (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    assigned_admin_id BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS support_messages (
    id BIGINT PRIMARY KEY,
    ticket_id BIGINT NOT NULL,
    sender_role TEXT NOT NULL DEFAULT '',
    sender_user_id BIGINT NOT NULL,
    text TEXT NOT NULL DEFAULT '',
    media_type TEXT NOT NULL DEFAULT '',
    media_file_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_support_messages_ticket_id ON support_messages(ticket_id);

CREATE TABLE IF NOT EXISTS legacy_import_runs (
    id BIGSERIAL PRIMARY KEY,
    source_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'started',
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS legacy_users_archive (
    user_id BIGINT PRIMARY KEY,
    payload JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS legacy_withdraw_requests_archive (
    id BIGINT PRIMARY KEY,
    payload JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS legacy_payment_intents_archive (
    payment_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS legacy_support_tickets_archive (
    id BIGINT PRIMARY KEY,
    payload JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS legacy_support_messages_archive (
    id BIGINT PRIMARY KEY,
    payload JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
