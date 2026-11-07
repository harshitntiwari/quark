CREATE TABLE IF NOT EXISTS schedules (
    name           TEXT         PRIMARY KEY,
    cron_expr      TEXT        NOT NULL,
    queue          TEXT        NOT NULL DEFAULT 'default',
    kind           TEXT        NOT NULL,
    args           JSONB       NOT NULL DEFAULT '{}',
    priority       INT         NOT NULL DEFAULT 0,
    max_attempts   INT         NOT NULL DEFAULT 3,
    timeout_ms     BIGINT      NOT NULL DEFAULT 60000,
    misfire_policy TEXT        NOT NULL DEFAULT 'run_once' CHECK (misfire_policy IN ('skip', 'run_once', 'catch_up')),
    next_run_at    TIMESTAMPTZ NOT NULL,
    last_run_at    TIMESTAMPTZ,
    enabled        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules (next_run_at) WHERE enabled = TRUE;