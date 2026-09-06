# Database maintenance

Operator runbook for keeping the Supabase Postgres database healthy: what runs on its
own, what to check and when, and what each failure signal means.

This is the **ongoing upkeep** doc. For first-time setup / moving to a new Supabase
project, see [`supabase_operations.md`](supabase_operations.md) instead.

Everything here needs `POSTGRES_URL` (Supabase connection string, `sslmode=require`) in
your environment or `.env`.

---

## 1. What runs automatically

`.github/workflows/daily_ingest.yml` — **06:00 UTC daily**:

1. bootstraps the schema (base + all migrations, idempotent)
2. `python -m src.pipeline.run_daily`
3. `ANALYZE`s the core tables (keeps planner estimates honest)
4. `bash scripts/apply_analytical.sh` — rebuilds the analytical layer, which doubles as
   the daily matview refresh

`.github/workflows/watchdog.yml` runs a couple of hours later, calls
`scripts/check_staleness.py`, and re-runs the daily ingest if a slice looks stale
**or** if unhealed ingest errors remain in the same 26h **daily window** DB Health
uses (including weekends — a Saturday `CVMHostUnreachable` burst is a failed cron,
not a quiet market day). Historical backfill errors outside `DAILY_LOOKBACK_MONTHS`
do not count. Recovery probes `dados.cvm.gov.br` first so a blocked
runner IP fails in a second instead of writing another round of error rows.

`.github/workflows/backfill.yml` is on-demand only (see §4).

> **⚠️ All three share one concurrency group (`supabase-ingest`, `cancel-in-progress:
false`), and GitHub keeps only ONE pending run per group.** A queued run is therefore
> not safe: when a newer run enters the group, the older _pending_ one is **cancelled**,
> silently, with no failure anywhere.
>
> Observed 2026-08-28: a `cia_aberta` backfill ran for hours; an `analytics-only`
> deploy dispatched behind it sat pending for 70 minutes and was then evicted by the
> scheduled Ingest Watchdog entering the group. The deploy reported `cancelled` — easy
> to read as "someone cancelled it" rather than "it never ran".
>
> **So: do not queue a deploy behind a long backfill.** Wait until the group is free
> (no in-progress or pending run on daily_ingest / backfill / watchdog), then dispatch.
> If a deploy shows `cancelled` with no logs, this is almost certainly why — re-dispatch
> it, nothing is broken.

> **A green run used to mean nothing.** In June 2026 a backfill spent 4h22m failing every
> download, printed `0 total rows`, exited 0, and left `cvm_fi_diario` 2024 **and** 2025
> completely empty behind a green check. `run_daily` now exits non-zero when any source
> fails — including CVM slices that logged `error` while the function returned 0 —
> a backfill that lands zero rows exits 1, and a slice that fetched rows but wrote
> none is logged `error`. Trust green _more_ than before — but §2 still exists for a
> reason.

---

## 2. Checks and cadence

| When                 | Command                                      | Looking for                                                                                                               |
| -------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Daily 07:30 UTC      | Actions → **DB Health** (`health.yml`)       | unhealed ingest errors, stalled monthly families, `api.catalog()`/`coverage()`, disk size (warn-only; `PLAN_DISK_GB=135`) |
| After any run        | `python scripts/check_staleness.py`          | exit `0` fresh · `10` daily stale **or unhealed errors** · `11` monthly (ANBIMA) stale                                    |
| Weekly               | `python scripts/verify_pipeline.py`          | presence, field-population rates, sample business metrics per entity                                                      |
| Weekly               | the audit-log triage query (§3)              | `error` slices, slices stuck `running`, entities missing entirely                                                         |
| Monthly              | `POSTGRES_URL=… python scripts/db_parity.py` | table/view inventory + row estimates and sizes (`--exact` for true `COUNT(*)`)                                            |
| Monthly              | `SELECT * FROM data_coverage();`             | per-entity date coverage — the gap detector                                                                               |
| **Yearly (Nov–Dec)** | partition rollover (§6)                      | next year's partitions must exist before January                                                                          |

`data_coverage(p_entity_type, start_date, end_date)` and
`ingest_log_summary(start_date, end_date)` (defaults: last 7 days) are analytical-layer
functions — they exist only after `apply_analytical.sh` has run.

---

## 3. Reading `cvm_ingest_log`

Exactly one row per ingest run, written by `_log_start` / `_log_finish` in
`src/pipeline/cvm_pipeline.py` (CVM slices) and by `src/pipeline/ingest_log.py`
(`bacen/*`, keyed on the fetch window's start month; partial counts on error).

| Status    | Meaning                                                                                                | Action                |
| --------- | ------------------------------------------------------------------------------------------------------ | --------------------- |
| `ok`      | Rows upserted (or the published file was genuinely empty)                                              | none                  |
| `skipped` | Source 404'd — a **not-yet-published** month. Normal on the trailing daily window; CVM lags 1–2 months | none                  |
| `error`   | Fetch/parse/write failed, **or** rows were fetched and none survived parsing                           | investigate — see §11 |
| `running` | Never finalized: the process died mid-slice                                                            | re-run that slice     |

Triage query:

```sql
SELECT entity, doc_type, status, count(*) AS n,
       min(period_year) AS yr_lo, max(period_year) AS yr_hi,
       sum(rows_upserted) AS rows
FROM cvm_ingest_log
GROUP BY entity, doc_type, status
ORDER BY entity, doc_type, status;
```

Two signals worth knowing, both learned from real outages:

- **Rows stuck `running`** mean the run died before finalizing. Historically this happened
  when the DB connection idled out during a long fetch, so the status update failed
  silently. `_log_finish` now reconnects and retries once, so fresh `running` rows point
  at a hard crash or a cancelled job rather than an idle connection.
- **No rows at all for an entity** is more severe than an `error` row, and means the
  ingest died _before_ any write. This is exactly how the ANBIMA ingest was found to have
  been failing on every single daily run for months — it wrote a non-existent audit-log
  column, and the exception was downgraded to a warning. If an entity you expect is simply
  absent from the query above, suspect its logging/startup path, not its parser.

Also note: **`ok` with `rows_upserted = 0` is no longer possible** when the source
returned rows. That combination was what let `cvm_fiagro_mensal` sit empty behind 34
`ok` slices; it is now an `error` naming the likely cause.

The DB Health workflow (`.github/workflows/health.yml`) fails on **unhealed**
error slices **that daily ingest would retry**: an `error` whose slice has no
later `ok` **or** `skipped`, and whose period is undated, current-year yearly,
or a month inside `DAILY_LOOKBACK_MONTHS` (4, same as `CVM_DAILY_LOOKBACK_MONTHS`).
A `TimeoutError` on the current unpublished month, followed by the daily window's
404 `skipped`, is a recovered probe — not a broken warehouse. Run 33164105326
went red on exactly that (`fidc/mensal_tab_x2` 2026-08). Historical backfill
errors (DB Health #14: 31 `fi/cda_cotas` 2010–2022 yearly + `fi/cda_acoes`
2025-12..2026-05 slices after CVM refused the runner) do **not** fail this
gate: `run_daily` never touches those years, and the backfill workflow already
went red. Disk size is a warning only; do not DROP landing tables to clear it.

A later `ok` that still leaves the slice unhealed is a classification bug, not
a missed cron. Run 33299581405 (DB Health #6) failed on `b3/corporate_events`
after Daily CVM Ingest #199 had already upserted 11,632 event rows: 35 issuers
(first ADMF) return HTTP 200 / empty from GetListedSupplementCompany because
`left(codneg,4)` is not always B3's listed-company key (ADMF3's catalog code is
B100). Those are skipped, not slice errors; a transport failure on one issuer
still is. Re-run daily ingest after that fix so the later `ok` heals the row.

---

## 3b. Ingest write throughput

`CVM_DB_POOL_SIZE` (default **4**) sets how many Postgres connections the ingest
holds. It is not the connection ceiling — the instance reports
`max_connections = 120`; the _10_ people remember is the GoTrue/Auth pool, which
ingest never touches. The real limit is compute: `max_parallel_workers = 2` and
`max_worker_processes = 6` (a ~2 vCPU box), plus every concurrent writer
maintaining the same indexes on one unpartitioned table.

Measured on an ephemeral PG16 (4 workers x 60k rows, `ON CONFLICT DO UPDATE`,
5000-row chunks):

| pool | elapsed | rows/s | vs pool=1 |
| ---- | ------- | ------ | --------- |
| 1    | 9.18 s  | 26,157 | 1.00x     |
| 2    | 5.84 s  | 41,077 | 1.57x     |
| 4    | 4.20 s  | 57,106 | **2.18x** |
| 8    | 4.03 s  | 59,547 | 2.28x     |

Past 4 the curve is flat — 8 buys 4% for double the connections. Raise it only
against a fresh measurement on the box you are actually running against; that
table came from a machine with more cores than the Supabase instance, so treat
the shape (diminishing past 4) as the transferable part, not the absolute
numbers.

Pool size and task concurrency are different knobs: `CVM_FI_CONCURRENCY`
controls how many slices are in flight, `CVM_DB_POOL_SIZE` how many can write at
once. Raising one without the other does nothing.

## 4. Healing gaps (backfill)

**Entity / per-year backfill** — GitHub → Actions → **CVM Historical Backfill** → _Run
workflow_ (inputs: `entity`, `start_year`, `end_year`, `fi_doc_type`). The default is `fi`;
choose one
entity and a narrow year range. Matrix jobs use `max-parallel: 1`, print
`cvm_ingest_log`/coverage first, and FI skips a year only when both diario and perfil are
already complete. Choose `all` only when the database can absorb a full historical run.
For the known FI balance-sheet gap, set `entity=fi` and `fi_doc_type=balancete`; the
workflow then checks and fetches only balancete months.
Before inspecting coverage, the workflow preserves any audit row stuck in `running` for
more than 24 hours and closes it as `error` with `finished_at` and an explanatory message.

### Repairing only the missing months

`fi_repair_gaps=true` (with a specific `fi_doc_type`) fetches **only** the months absent
from that document's table — nothing else. `fi_months=2019-04,2023-01` names them
explicitly instead. Each year job takes the months belonging to its own year; a year with
none exits immediately.

Both bypass the "year already complete" check, on purpose. That check counts `ok` rows in
`cvm_ingest_log`, and the audit log records _attempts_, not coverage — the exact signal
that produced this gap. Coverage in repair mode is decided by probing the table.

> **Never diagnose coverage from `cvm_ingest_log` alone.** On 2026-08-27 `fi/balancete`
> 2026-06 had a fresh `error` / `TimeoutError` row sitting on top of 2,178,163 real rows
> from an earlier `ok` attempt. The newest audit row for a slice is the newest _attempt_.
> Ask the table:
>
> ```sql
> -- cheap: one indexed EXISTS probe per month
> SELECT to_char(m, 'YYYY-MM') AS ym,
>        EXISTS (SELECT 1 FROM cvm_fi_balancete b
>                WHERE b.dt_comptc >= m::date
>                  AND b.dt_comptc < (m + INTERVAL '1 month')::date) AS has_rows
> FROM generate_series(date '2019-01-01', date_trunc('month', CURRENT_DATE),
>                      INTERVAL '1 month') m
> ORDER BY m;
> ```
>
> The full `GROUP BY date_trunc('month', dt_comptc)` gives exact counts but scans 111M
> rows / 24 GB — fine once, not on a schedule.

A month CVM has not published yet is `skipped`, not a gap: `--repair-gaps` excludes months
whose only audit outcome is `skipped`, and stops two months short of today for publication
lag. It will not chase a file that does not exist.

**BACEN only (Focus / SGS / PTAX)** — Actions → **CVM Historical Backfill** →
_Run workflow_ with `bacen_only=true`. Skips every CVM entity and the ETF jobs;
applies schema, then `python -m src.pipeline.run_backfill --bacen-only --bacen-start 2019-01-01`.
Use this after the Focus unique-key / `baseCalculo` fetch filters landed: daily ingest
only covers a trailing window, so older `bacen_expectativas` rows that collapsed under
the pre-horizon key stay sparse until this runs. The `/macro` 24-month Focus chart
pins `horizon = to_char(reference_date, 'YYYY')` and will show blanks for months
that were never re-fetched.

This input has never been dispatched against production (as of the dashboard
integrity audit). Do not confuse it with a full CVM matrix run.

**One entity** — use the Historical Backfill `entity` input, or locally:

```bash
python -m src.pipeline.run_backfill --start-year 2019 --cvm-only --entity fidc
```

Full CVM history is Actions → **CVM Historical Backfill**.

**Locally / one slice at a time** — pipeline CLI (needs `POSTGRES_URL`):

```bash
# one entity, one year
python -m src.pipeline.run_backfill --cvm-only --entity fidc --start-year 2024 --end-year 2024

# one FI document type, selected years
python -m src.pipeline.run_backfill --cvm-only --entity fi --doc-type balancete --start-year 2021 --end-year 2025

# only the months that are missing from cvm_fi_balancete (reads the table)
python -m src.pipeline.run_backfill --cvm-only --entity fi --doc-type balancete --repair-gaps

# or name them yourself — nothing outside this list is fetched
python -m src.pipeline.run_backfill --cvm-only --entity fi --doc-type balancete \
    --months 2019-04,2019-07,2023-01

# one entity, full history
python -m src.pipeline.run_backfill --cvm-only --entity fidc --start-year 2019
```

One month of one dataset — call the ingestor method:

```python
import asyncio
from src.pipeline.cvm_pipeline import CVMIngestor

asyncio.run(CVMIngestor().ingest_fidc_tranche(2024, 5))
```

### If CVM refuses connections

CVM blocks GitHub runner IPs from time to time. Retrying does not help: the TCP/TLS
handshake never completes. After `CVM_CONNECT_FAILURE_LIMIT` consecutive connect
failures (default **8**) the fetcher raises `CVMHostUnreachable` and aborts instead of
grinding through every remaining slice. Daily ingest and watchdog recovery also
run `scripts/check_cvm_reachable.py` first so a blocked IP fails in about a
second without writing a `cvm_ingest_log` error for every remaining slice (that
is what turned DB Health red on 2026-08-29: 44 unhealed rows from the 06:00
run, then a Saturday watchdog no-op).

**Fix: re-dispatch the workflow** — a fresh runner usually gets an unblocked IP (in the
June 2026 incident FI 2022 and 2026 succeeded while 2024/2025 were blocked, in the same
run). Do **not** raise the limit to push through; that just restores the 4-hour grind.
The counter resets on any HTTP response, including a 404, so the daily window's routine
not-yet-published misses can never trip it.

---

## 5. Schema changes

- Edit `src/store/schema.sql` **and** add a new `src/store/migrations/NNN_*.sql`.
- **Never edit a historical migration** — they are append-only.
- Apply with `python scripts/apply_schema.py` (base schema + all migrations, idempotent).
  CI also bootstraps this on every run.
- CI applies with `psql -v ON_ERROR_STOP=1`, so migrations must be **psql-clean**
  (real SQL comments, no client-specific syntax).
- Keep everything idempotent: `CREATE TABLE IF NOT EXISTS`, named `UNIQUE` constraints,
  `ADD COLUMN IF NOT EXISTS`.

Adding a whole dataset is a different recipe — see "Adding a dataset" in `CLAUDE.md`.

---

## 6. Yearly partition rollover ⚠️

`cvm_fi_diario`, `cia_account`, and `b3_cotahist` are **range-partitioned by year**.
Partitions are declared through **2026**, plus a `_future` catch-all.

This will not fail loudly. Once 2027 data arrives it lands in `cvm_fi_diario_future` /
`cia_account_future` / `b3_cotahist_future`, which silently forfeits partition pruning
and grows without bound — the same quiet-degradation shape as the bugs above.

Check (should be `0`):

```sql
SELECT 'fi_diario_future' AS t, count(*) FROM cvm_fi_diario_future
UNION ALL SELECT 'cia_account_future', count(*) FROM cia_account_future
UNION ALL SELECT 'b3_cotahist_future', count(*) FROM b3_cotahist_future;
```

Each year, before January:

1. Add the next partition to `schema.sql` and a new migration, following the existing
   pattern:
   ```sql
   CREATE TABLE IF NOT EXISTS cvm_fi_diario_2027 PARTITION OF cvm_fi_diario
       FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');
   ```
   (and the matching `cia_account_2027`, which partitions on `dt_refer`, and
   `b3_cotahist_2027`, which partitions on `trade_date`).
2. Apply it, then move any rows already parked in `_future` into the real partition
   (`INSERT … SELECT` + `DELETE`, inside a transaction).

---

## 7. Analytical layer

```bash
bash scripts/apply_analytical.sh     # run AFTER data exists
```

Applies `src/store/analytical/01…17` in order, re-creating dims, matviews
(`dim_fund`, `fact_fund_monthly`, `fact_security_monthly`), fraud screens and the
ranking/ETF functions. The re-create _is_ the daily refresh, so dashboards see fresh
aggregates without a separate cron.

- Several files carry **smoke guards that RAISE on an empty database** — never run this
  before ingesting.
- A missing `pg_cron` extension (`08_cron_schedules.sql`) is tolerated with a warning.
  **Any other failure is fatal** and the script exits non-zero.
- Consumers (`dashboard/`, `webapp/`) read these objects directly and are read-only.

---

## 8. Security / RLS

**The boundary is the grant, not RLS — and it is closed.** `anon` and
`authenticated` hold no privilege on any landing table: `12_grants_and_rls.sql`
revokes them and grants only `USAGE` on schema `api`, `SELECT` on its eight
views and `EXECUTE` on its thirteen functions. A request with
`Accept-Profile: public`, or any path naming `cvm_*` / `b3_cotahist` / `cia_*`,
answers **401 for every caller**. `.github/workflows/health.yml` probes exactly
that on every run and fails the job if a landing table ever answers 200.

This section previously read "52 of 59 public tables have RLS disabled, so the
anon key can read — and potentially write — almost everything." That was true
before the privilege sweep and is not true now. RLS being off is not the same
statement as the data being reachable: with no grant there is no row for a
policy to filter. Enabling RLS on top would be defence in depth, not the
boundary itself.

`docs/security/enable_rls.sql` remains as that optional second layer. It is
deliberately outside `migrations/` so the CI bootstrap cannot run it, and it is
**not** what stands between the publishable key and the warehouse today:

```bash
psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -f docs/security/enable_rls.sql
```

It enables RLS and adds a SELECT-only `anon_read` policy to every public base table
(including partitioned parents) in one transaction. Two things to understand before
running it:

- The read policy is **not optional**. `ENABLE ROW LEVEL SECURITY` without a SELECT
  policy returns zero rows to anon — the dashboards would go blank.
- It deliberately does **not** use `FORCE ROW LEVEL SECURITY`, so the owner/service role
  the pipeline connects as keeps bypassing RLS and ingestion is unaffected.

Verify afterwards with the query in the file's footer.

---

## 9. Storage, vacuum, `ANALYZE`

- Autovacuum is managed by Supabase; no manual `VACUUM` scheduling needed.
- CI `ANALYZE`s the core tables after every ingest. Row estimates from `db_parity.py`
  come from `pg_class.reltuples` and are only as fresh as the last `ANALYZE` — use
  `--exact` when a number needs to be authoritative.
- Largest objects (2026-08-28 health gate, `pg_total_relation_size`):

  | relation             | size       |
  | -------------------- | ---------- |
  | `cvm_fi_balancete`   | 30 GB      |
  | `cia_account_2021`   | 3.7 GB     |
  | `cvm_fi_perfil`      | 3.6 GB     |
  | `cia_account_2020`   | 3.3 GB     |
  | `cia_account_2019`   | 2.6 GB     |
  | `cia_account_2022`…  | ~2 GB each |
  | `b3_cotahist_2025`   | 1.5 GB     |
  | `cvm_fi_diario_2026` | 1.4 GB     |

  Whole database **~72 GB** on 2026-08-28; **104 GB** on 2026-09-01 after the
  holdings, FIP and debenture backfills. `db_parity.py` prints live sizes.

### Where the bytes go, and what is actually reclaimable

`scripts/health_diagnostics/14_disk_what_is_reclaimable.sql` (DB Health,
`mode=diagnostics`) is the measurement to run before any reclaim. It is
read-only and catalog-only. Its 2026-09-01 reading, which is what a future
run should be compared against:

- `pg_stat_database.stats_reset` is NULL, so an `idx_scan = 0` is real.
- TOAST is 8 KB on every large table: `raw` JSONB is stored inline. There is
  no JSONB lever.
- The only index that met migration 22's bar was `cvm_fi_cda_acoes_pkey`
  (517 MB, never scanned) — migration 37. `cvm_fi_cda_cotas_pkey` looks the
  same but its 30 scans are the `$cotas_dedup$` guard in `schema.sql` reading
  `id`; it is load-bearing.
- **The growth is not new data.** `cia_account_2019…2022` showed 55–78
  updates per insert and `cvm_fi_perfil` 31: whole yearly files re-upserted
  daily, unchanged. Before 2026-09-01 `upsert_rows` rewrote every conflicting
  row unconditionally, and each rewrite is a dead tuple. It now updates only
  `WHERE (cols) IS DISTINCT FROM (EXCLUDED.cols)`. If `n_tup_upd` climbs
  again relative to `n_tup_ins`, that guard has been lost.
- `cvm_fi_balancete` was 8.1% dead (15M tuples) with its last autovacuum five
  days old: autovacuum's default 20% scale factor on 172M rows does not fire
  until 34M dead. A per-table `autovacuum_vacuum_scale_factor` is the
  reversible lever if that ratio keeps rising; a plain `VACUUM` (not FULL)
  marks the space reusable without the ACCESS EXCLUSIVE lock. Neither
  returns disk to the OS — only a rewrite does, and §9 item 3 still applies.

### Disk vs the plan allowance

The DB Health workflow (`health.yml`) reports `pg_database_size` as an absolute
GB figure. `PLAN_DISK_GB` is **empty by default**: a placeholder of 8 GB against
a ~72 GB warehouse printed "899%" and trained everyone to ignore the line. Set
it only to a real purchased allowance (included + addon). The gate **warns, it
does not fail**, and it must not be "fixed" by dropping landing tables: those
relations _are_ the warehouse.

What to do (operator, not a migration):

1. **Add disk / raise the spend cap** in the Supabase dashboard for project
   `zcjbtpxuhdekpwcxmepn`. If extra disk is purchased, set `PLAN_DISK_GB` in
   `.github/workflows/health.yml` to the included+addon total so the percentage
   is meaningful.
2. **Do not `DROP` yearly `cia_account_*` / `b3_cotahist_*` partitions or
   `cvm_fi_balancete` to reclaim space.** There is no retention policy on
   landing data; historical ITR/DFP and FI balance sheets are the product.
3. **Do not `VACUUM FULL` the 30 GB balancete table** from CI — it takes
   `ACCESS EXCLUSIVE` for a long time and is exactly the lock that previously
   killed schema apply. Autovacuum handles bloat; `DROP INDEX` (migration 22)
   unlinks files immediately, no extra vacuum required for those indexes.
4. Partitioning `cvm_fi_balancete` (still unpartitioned at 30 GB) is a future
   lifecycle change, not a cleanup. Do it as its own migration with a
   measured cutover, never as a panic drop.

The health job is read-only and does not apply schema. Diagnostics live in
`scripts/health_diagnostics/*.sql` (one session each); a missing view skips that
file and the rest still run.

---

## 10. Supabase performance advisor (do not "fix" these blindly)

The dashboard **Performance Advisor** stays red after a compute upgrade. That is
expected. The lints are schema-shape checks, not CPU/RAM. The generic remediations
(add a primary key, drop unused indexes, switch Auth to Percentage) are **wrong on
this warehouse** and some of them take `ACCESS EXCLUSIVE` on the tables that already
blocked schema apply when a Vercel build was running.

Classify with:

```bash
psql "$POSTGRES_URL" -f scripts/queries/14_advisor_triage.sql
```

| Advisor lint                                                | What it is here                                                                                                                                                                                                                                                                                                      | Do                                                                                                                                                                                                       |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Auth DB Connection Strategy is not Percentage**           | GoTrue is capped at 10 connections. This project does **not** use Supabase Auth for ingest or the read API.                                                                                                                                                                                                          | **Leave it.** Percentage would let unused Auth compete with ingest writers for the pool we just paid to enlarge. Dashboard-only; there is no repo setting.                                               |
| **Unindexed FK `public.messages(messages_sender_id_fkey)`** | `messages` is **not in this repo**. Not in `schema.sql`, not in any migration. Leftover on the project (chat demo / old app).                                                                                                                                                                                        | If the triage query shows it and it is empty (or junk), `DROP TABLE public.messages CASCADE` from the SQL editor — **not** from a pipeline migration. Do not add an index to keep a table we do not own. |
| **`no_primary_key` (many tables)**                          | Almost entirely **partition children** of `cvm_fi_diario`, `b3_cotahist`, and `cia_account`. Postgres stores the UNIQUE/PK on the parent; the linter counts each yearly slice as a table without its own PK. Parents use a named `UNIQUE` on the natural key (required for `ON CONFLICT`) rather than `PRIMARY KEY`. | **Do not** `ALTER TABLE … ADD PRIMARY KEY` to silence the lint. That locks the largest relations in the database. Upserts already have a named UNIQUE that includes the partition key.                   |
| **`unused_index`**                                          | `idx_scan = 0` after a stats reset, a compute move, or because the planner prefers the UNIQUE. The vista covering index, BRINs, and CNPJ/date indexes exist for ingest, `api.quotes`, and the dashboard.                                                                                                             | **Do not drop — with one narrow exception (below).** A previous dashboard bug was a sequential scan of millions of rows to print four numbers. Dropping "unused" indexes recreates that.                 |

### The one case where dropping is right

A zero is only evidence when the window is real. Before dropping any index, prove **all three**, and record the numbers in the migration:

1. `pg_stat_database.stats_reset` is `NULL` (or old enough to cover heavy use) — otherwise `idx_scan = 0` just means the counters were cleared;
2. `pg_stat_user_tables.n_tup_ins` on the table is large in that same window — a cold table trivially has unused indexes;
3. the index is not the only support for a constraint, a foreign key, or a `ON CONFLICT` target.

`cvm_fi_balancete` met all three on 2026-08-27: `stats_reset` NULL, 112,110,933 inserts, and three indexes at `idx_scan = 0` — `cvm_fi_balancete_pkey` (2,513 MB, surrogate `id` read nowhere in the repo), `idx_fi_balancete_cnpj` (930 MB, redundant because `uq_fi_balancete` already leads with `cnpj`), and `idx_fi_balancete_conta` (756 MB). Migration 22 drops them: 4.2 GB back, and **+20% insert throughput measured** (25.4k → 30.6k rows/s single-writer). Rollback DDL is in the migration header.

What stayed: `uq_fi_balancete` (the `ON CONFLICT` target, 143M scans) and `idx_fi_balancete_date` (gap scans, dashboards).

A leftover `messages` table is the only advisor hit that might deserve a DROP. Everything
else is either a false positive from partitioning or an index we would miss the next
time a query planner needs it.

---

## 11. Known gaps register

Live as of 2026-08-27. Keep this current — it exists so the next person doesn't have to
rediscover these by querying the warehouse from scratch.

| Gap                                                                                                                                                                                                                                                                      | Closes by                                                                        |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| `cvm_fi_balancete`: 32 published months missing (2019-04/07/10/11/12, 2020-02/05/06/09/12, 2021-04/07/08, 2022-07/09/10/12, 2023-01/03/04/06/07/09/10/12, 2024-02/04, 2025-04/05/10, 2026-01/04). Verified from the table 2026-08-27: 59 of 91 published months present. | Backfill with `fi_doc_type=balancete` + `fi_repair_gaps=true` (§4)               |
| `b3_cotahist` starts 2025-01-02 — every B3 endpoint serves ~20 months                                                                                                                                                                                                    | `daily_ingest.yml` → `mode=b3-backfill`, one year at a time from 2019            |
| `cvm_fi_diario` 2024 + 2025 empty; 2026 starts Mar 2; 2019/2020 thin                                                                                                                                                                                                     | Re-dispatch the backfill (§4)                                                    |
| `cvm_fiagro_mensal` empty                                                                                                                                                                                                                                                | Field map fixed in PR #72 — needs a backfill run                                 |
| `anbima_class_monthly` empty                                                                                                                                                                                                                                             | Audit-log bug fixed in PR #72 — next daily run fills it                          |
| `etf_market_snapshot` empty                                                                                                                                                                                                                                              | Set the `APIFY_TOKEN` secret, then verify the scrape's selectors on one real run |
| SECURIT (all tables) 2026 only                                                                                                                                                                                                                                           | Undiagnosed — earlier years sit stuck `running`                                  |
| `cia_account` 2026 partition only                                                                                                                                                                                                                                        | Undiagnosed — pre-2026 ITR/DFP never backfilled                                  |
| `cvm_fii_mensal` starts 2021                                                                                                                                                                                                                                             | Undiagnosed — 2019–2020 never landed                                             |
| RLS off on 52 tables                                                                                                                                                                                                                                                     | Apply §8 when ready                                                              |

---

## 12. Troubleshooting

| Symptom                                                                  | Likely cause                                                         | Fix                                                                                                                                                |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Green CI run, table still empty                                          | A source returned rows but all were dropped                          | Check `cvm_ingest_log` for `error` with "fetched N … upserted 0"; compare the field map to the current source header                               |
| `FieldMapMismatch: … no longer matches the source header`                | The source renamed its columns (e.g. a CVM-175 regime change)        | Update the map in `src/parsers/field_maps/`, putting the new name first and keeping the legacy name as a fallback. This is what happened to FIAGRO |
| Many slices `error` with "cannot connect to host" / `CVMHostUnreachable` | CVM is blocking this runner's IP                                     | Re-dispatch the workflow (§4). Don't raise the failure limit                                                                                       |
| Slices stuck `running`                                                   | Run died mid-slice (crash, cancelled job, timeout)                   | Re-run those slices; check the job log for the real cause                                                                                          |
| An entity has **no** `cvm_ingest_log` rows                               | It failed before any write — usually startup or logging, not parsing | Run its ingest directly and read the traceback; check every logged key is a real `cvm_ingest_log` column                                           |
| `run_daily` exits 1 on `etf_market` only, CVM rows already upserted | Apify `web-scraper` (or another full-permission store actor) needs a one-time Console grant; HTTP 403 `full-permission-actor-not-approved` | Default actor is now `apify/playwright-scraper` (limited permissions). If you pin `APIFY_ETF_ACTOR` to `apify/web-scraper`, approve it once at the `approvalUrl` in the log. The daily run skips this 403 so ANALYZE + analytical still run. |
| `run_daily` exits 1 on `etf_market` only; log shows HTTP 408 `run-timeout-exceeded` | Apify's `run-sync-get-dataset-items` hard-caps at 300s; ~187 playwright ETF pages take longer (Daily CVM Ingest 33721538761, 2026-09-03) | Fetcher now starts the actor asynchronously and polls (wait budget `APIFY_ETF_TIMEOUT_SECS`, default 2400s — the first async run took 1,145 s for 178 tickers, so 1200 s was a 55 s margin). A remaining timeout is skipped like an unset token so ANALYZE + analytical still run. |
| `run_daily` exits 1 on `etf_market` only; log shows `ended ABORTED` | Apify killed the actor run before a dataset landed (Daily CVM Ingest #219, run 34015471961, 2026-09-06 — ~11 min after start, run `d2UCTxohVY9IcQX9a`) | `ABORTED` / `ABORTING` map to `ApifyRunAbortedError` and skip like an unset token so ANALYZE + analytical still run. An actor that ended `FAILED` or returned an empty dataset still fails the daily run. |
| `run_daily` exits 1                                                                  | One or more sources failed (others still ran)                        | Read the final "FAILED for N source(s)" line, which names each                                                                                     |
| `apply_analytical.sh` fails a smoke check                                | Ran against an empty/partial DB                                      | Ingest first, then re-run                                                                                                                          |
| Dashboard suddenly empty after a security change                         | RLS enabled without a SELECT policy                                  | Ensure the `anon_read` policy exists (§8)                                                                                                          |
| Rows appearing in `*_future` partitions                                  | Missing year partition                                               | §6 rollover                                                                                                                                        |
| Performance Advisor still red after a compute upgrade                    | Lint of partition children / unused Auth / leftover `messages`       | §10 — do **not** add PKs or drop indexes to clear the badge                                                                                        |

---

## Related

- [`supabase_operations.md`](supabase_operations.md) — one-time setup / project cutover
- `scripts/queries/14_advisor_triage.sql` — classify Performance Advisor lints (§10)
- [`DATA_MODELING.md`](DATA_MODELING.md) — star schema conventions for new data classes
- [`ETF_AND_PERFORMANCE.md`](ETF_AND_PERFORMANCE.md) — ETF carve-out and the CVM-175 CNPJ split
- `CLAUDE.md` — architecture, the "Adding a dataset" recipe, and the non-negotiable
  data-integrity rules
