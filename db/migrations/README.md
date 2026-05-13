# Manual Database Migrations

This project uses explicit, reviewed SQL files for schema changes. Do not rely
on `Base.metadata.create_all()` to alter existing tables; it only creates tables
that do not exist.

Process:

1. When changing an ORM model in `db/models.py`, add a matching SQL file here.
2. Name it with the date and a short description, for example:
   `20260513_add_memory_daily_decision.sql`.
3. Use idempotent SQL where practical, especially `ADD COLUMN IF NOT EXISTS`.
4. Execute the SQL manually before deploying code that depends on the new field.
5. Keep the SQL file committed so production schema history is auditable.

Current required migration:

```sql
ALTER TABLE memory_daily
ADD COLUMN IF NOT EXISTS decision JSONB;
```

```sql
ALTER TABLE holdings_factors
ADD COLUMN IF NOT EXISTS price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS close_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS daily_return_pct NUMERIC(8,6);
```
