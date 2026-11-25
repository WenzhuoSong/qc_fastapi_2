-- Today's to-do list (user's request 2026-11-25): an item comes back until it is handled; "done" hides it for good,
-- "snoozed" until snooze_until. Keys name the event (e.g. sell:<trade id>:<signal date>), so a new event is a new item.
CREATE TABLE IF NOT EXISTS scout.todo_state (
    key           VARCHAR(96) PRIMARY KEY,
    status        VARCHAR(12) NOT NULL DEFAULT 'open',      -- open | snoozed | done
    snooze_until  DATE,
    first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
