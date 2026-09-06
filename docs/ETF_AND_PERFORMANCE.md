# ETF separation, performance analytics, and dashboard — notes & data gaps

This documents the analytical + dashboard work that evaluates **ETFs separately**
from funds and adds **per-asset-class performance** (horizontal/vertical), plus the
live-data gaps found while building it. Nothing here fabricates data — gaps are
recorded as gaps.

## What was added

- **ETF carve-out** — `src/store/analytical/01_dim_fund.sql` and
  `04_fact_fund_monthly.sql` now exclude ETF CNPJs (`NOT EXISTS … cvm_etf_registry`)
  from the fund universe, so ETFs aren't double-counted inside the FI buckets.
  `dim_fund_category` and everything downstream inherit the carve-out.
- **`16_etf_analysis.sql`** — ETF-only RPCs over `etf_daily` + `anbima_class_monthly`
  (filtered to `anbima_category = 'ETF'`):
  `etf_performance_series`, `etf_performance_ranking`, `etf_class_performance`.
- **`17_performance_analysis.sql`** — fund RPCs over `fact_fund_monthly` ⋈
  `dim_fund_category`: `fund_performance_series` (horizontal) and
  `fund_performance_ranking` (vertical, ranked within asset class). Return basis is
  per class and is returned as a column — FI `quota_return`, FII `dividend_yield`
  (compounded), FIDC/FIAGRO/FIP `pl_growth` (growth, not a clean return).
- **Dashboard** — `dashboard/pages/performance.md` (fund performance),
  `dashboard/pages/etf.md` (ETF universe), and nav links from `index.md`.

## Applying to prod

These are part of the analytical layer, applied **after** data exists:

```bash
POSTGRES_URL=… bash scripts/apply_analytical.sh   # idempotent; CREATE OR REPLACE
```

(or let the daily CI run it). **Status (2026-06):** the analytical layer — these functions
plus the `dim_fund` materialized view — is **applied to the live Supabase DB**, and the
dashboard pages shipped (PR #56). `dim_fund` is now a **materialized view**: it aggregates
the full `cvm_fi_diario` history and is joined twice by `fund_performance_ranking`, so a
plain view timed out on the full-universe call. It is refreshed by the 06:15 cron job in
`08_cron_schedules.sql` and rebuilt by each `apply_analytical` run.

## Live-data gaps found (2026-06)

| Gap | Detail | Unblocks |
| --- | --- | --- |
| **`etf_daily` is empty** | The 187 registry ETFs' fund-level CNPJs have **zero** overlap with 2026 `cvm_fi_diario` (CVM-175 keys the daily file on share-**class** CNPJs). So ETF price/NAV/return/AUM/flow series can't come from CVM today. | Fix registry↔class-CNPJ linkage, or wire an external ETF price feed (etfsbrasil.com / FMP), or ingest ANBIMA ETF class series. |
| **ETF registry quant fields sparse** | `vl_patrim_liq` populated for only 8/187; `taxa_adm` 0/187. Identity (provider/segment/index) is complete for ~all. | cad_fi enrichment refresh / external feed. |
| **`anbima_class_monthly` empty** | Table exists; the ANBIMA ingest hasn't run on this DB. | Run the ANBIMA pipeline (`python -m src.pipeline.anbima_pipeline`). |
| **Fund history is 2026-only** | FI daily, FII (Q1), FIP present; FIDC/FIAGRO and `cvm_fund_registry` are empty. With the registry empty, FI funds fall into the coarse `Other FI` class. | Historical backfill + registry ingest (migration 11 already applied). |

The ETF page is therefore built on the **real registry universe** (provider /
segment / index / status), with price/return/AUM sections explicitly marked
pending a price feed.

## ETF price feed — Apify scrape of etfsbrasil.com.br

The price feed that fills the gap above is an **Apify** scrape of
`etfsbrasil.com.br/etfs/<ticker>` (it carries NAV/patrimônio líquido, número de cotistas,
price, fees, index, CNPJ and ISIN). NAV/cotistas are **JS-rendered**, so this runs through a
**headless-browser actor with rotating RESIDENTIAL proxies** (etfsbrasil
rate-limits direct scraping) — not a plain HTTP fetch.

Pieces (FETCH → PARSE → STORE):
- `apify/etfsbrasil_scraper.js` — the web-scraper `pageFunction`; it returns the page's
  full rendered text + `__NEXT_DATA__` JSON (field parsing is done server-side in Python).
- `src/fetchers/apify_etf_fetcher.py` — `ApifyETFFetcher` starts
  `apify/playwright-scraper` (limited permissions; override with `APIFY_ETF_ACTOR`)
  asynchronously, polls until the run finishes, then reads the dataset. Input is
  that pageFunction + one startUrl per active registry ticker + the proxy config.
  Raises on any failure / empty result. HTTP 403 `full-permission-actor-not-approved`
  (Apify full-permission store actors, historically `apify/web-scraper` after
  2026-08-31) is `ApifyActorNotApprovedError`. HTTP 408 `run-timeout-exceeded`
  (Apify's sync endpoint caps at 300s; ~187 playwright pages take longer) and a
  wait-budget miss are `ApifyRunTimeoutError`. Platform `ABORTED` /
  `ABORTING` (Daily CVM Ingest #219, run 34015471961) is `ApifyRunAbortedError`.
  All three skip like an unset token. An actor `FAILED` or empty dataset still raises.
- `src/pipeline/ingest_etf_market.py` — parses Brazilian number/date formats and
  upserts into `etf_market_snapshot` (migration `12_etf_market.sql`), idempotent on
  `(ticker, snapshot_date)`.

Run it:

```bash
export APIFY_TOKEN=…            # required
# optional: APIFY_ETF_ACTOR=apify~playwright-scraper  APIFY_PROXY_GROUPS=RESIDENTIAL
python -m src.pipeline.ingest_etf_market
```

Wired into `run_daily` when `APIFY_TOKEN` is set. An unset token skips the scrape
and never fails the daily run. The same skip applies when Apify returns
`full-permission-actor-not-approved` (the actor never started — approve it in
Console if you still pin `APIFY_ETF_ACTOR` to a full-permission store actor),
HTTP 408 `run-timeout-exceeded` (the scrape did not finish in time — we never
got a dataset), or a platform `ABORTED` status (the run was killed before a
dataset landed — Daily CVM Ingest #219).
The label-based parsers keep the full rendered page text **and** `__NEXT_DATA__`
in each row's `raw`, so a moved label never silently drops data.

