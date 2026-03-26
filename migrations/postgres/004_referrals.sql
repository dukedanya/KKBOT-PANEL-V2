CREATE TABLE IF NOT EXISTS ref_history (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    ref_user_id BIGINT NOT NULL DEFAULT 0,
    amount NUMERIC(14, 2) NOT NULL DEFAULT 0,
    bonus_days INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ref_history_user_id_created_at
    ON ref_history(user_id, created_at DESC);
