CREATE INDEX IF NOT EXISTS idx_pending_payments_status_created ON pending_payments(status, created_at);
CREATE INDEX IF NOT EXISTS idx_pending_payments_user_status ON pending_payments(user_id, status);
