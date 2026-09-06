"""
Daily incremental update — run by GitHub Actions cron at 06:00 UTC.

Fetches:
  - CVM: current month + previous month for all entities
  - BACEN: last ~30 days
  - ANBIMA: latest monthly boletim, all classes (idempotent — upserts full history)
  - B3 COTAHIST: last 7 calendar days of daily quotation zips (404 → skipped)

Required env vars: POSTGRES_URL
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.pipeline.cvm_pipeline import CVMIngestor
from src.pipeline.bacen_pipeline import BacenIngestor
from src.pipeline.anbima_pipeline import AnbimaIngestor
from src.pipeline.b3_pipeline import B3Ingestor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("run_daily")


async def main() -> None:
    start_ts = time.monotonic()
    logger.info("Daily update starting")

    # Sub-ingest failures are COLLECTED, not swallowed. Each source still runs
    # even if an earlier one failed (one bad source must not skip the rest), but
    # the process exits non-zero at the end so CI goes red. Previously each of
    # these blocks logged a warning and continued: that is how the ANBIMA ingest
    # failed on every single daily run for months without anyone noticing, and it
    # contradicts the repo's own "no silent except" data-integrity rule (CLAUDE.md).
    failures: list[tuple[str, Exception]] = []

    cvm_ingestor = CVMIngestor()
    totals = await cvm_ingestor.daily_update()
    # Every ingest_* method catches its own exception, writes the audit row
    # and returns 0, so daily_update() itself does not raise when CVM is
    # blocked. Run 33237536770 (2026-08-29 06:00) logged 44 CVMHostUnreachable
    # slices, upserted 0 CVM rows, then continued to BACEN/B3/ANBIMA and only
    # went red because b3_corporate_events hit an SSL EOF. DB Health then
    # failed on those unhealed slices. Fail the process for them — but AFTER
    # the other sources have run, so a CVM IP block does not skip BACEN/B3.
    if cvm_ingestor.failures:
        logger.error(
            "Daily CVM update finished with %d failed slice(s):",
            len(cvm_ingestor.failures),
        )
        for failure in cvm_ingestor.failures:
            logger.error("  %s", failure)
        failures.append((
            "cvm",
            RuntimeError(f"{len(cvm_ingestor.failures)} failed slice(s)"),
        ))

    # BACEN: incremental refresh — re-fetches the last ~30 days; cheap.
    try:
        bacen_ingestor = BacenIngestor()
        from datetime import date, timedelta
        bacen_start = (date.today() - timedelta(days=30)).isoformat()
        bacen_totals = await bacen_ingestor.backfill(start=bacen_start)
        totals.update(bacen_totals)
    except Exception as exc:
        logger.error("BACEN daily refresh failed: %s", exc, exc_info=True)
        failures.append(("bacen", exc))

    # ANBIMA: fetch latest monthly boletim (every ANBIMA class + type);
    # idempotent upsert.
    try:
        anbima_ingestor = AnbimaIngestor()
        anbima_totals = await anbima_ingestor.daily_update()
        totals.update(anbima_totals)
    except Exception as exc:
        logger.error("ANBIMA daily refresh failed: %s", exc, exc_info=True)
        failures.append(("anbima", exc))

    # B3 COTAHIST: public daily quotation zips. Weekends/holidays 404 and are
    # logged skipped; a real fetch/parse failure fails the daily run.
    try:
        b3_ingestor = B3Ingestor()
        b3_totals = await b3_ingestor.daily_update()
        totals.update(b3_totals)
    except Exception as exc:
        logger.error("B3 COTAHIST daily refresh failed: %s", exc, exc_info=True)
        failures.append(("b3", exc))

    # B3 corporate events: published splits, groupings, bonuses, dividends and
    # subscriptions per ISIN. One request per traded issuer (derived from our
    # own tape, not B3's 3,500-company list), so it is a few hundred small
    # calls. Events are near-static history — a failure here must not fail the
    # whole daily run, but it is recorded as a failure, never swallowed.
    try:
        b3_events = await B3Ingestor().ingest_corporate_events()
        totals["b3_corporate_event"] = b3_events
    except Exception as exc:
        logger.error("B3 corporate events refresh failed: %s", exc, exc_info=True)
        failures.append(("b3_corporate_events", exc))

    # ETF market snapshot: scrape etfsbrasil.com.br via Apify (NAV/price/cotistas
    # the post-CVM-175 daily file no longer exposes). The scrape is paid + rate-
    # limited, so it ONLY runs when APIFY_TOKEN is configured — an absent token
    # skips it and never fails the daily run. ApifyScrapeUnavailableError is the
    # same class (403 actor-not-approved, 408 run-timeout-exceeded, wait-budget
    # miss, platform ABORTED): we never got a dataset, so there is nothing to
    # fabricate. A scrape that ran and returned bad/empty data still fails the
    # daily run. One snapshot per ticker per UTC day; idempotent upsert on
    # (ticker, snapshot_date).
    if os.getenv("APIFY_TOKEN"):
        try:
            from src.pipeline.ingest_etf_market import ingest_etf_market
            from src.store.pg_client import get_pg_client
            etf_rows = ingest_etf_market(get_pg_client())
            totals["etf_market_snapshot"] = etf_rows
        except Exception as exc:
            from src.fetchers.apify_etf_fetcher import ApifyScrapeUnavailableError
            if isinstance(exc, ApifyScrapeUnavailableError):
                # No dataset. Same class as an unset token: log loudly and
                # continue so CVM/BACEN/B3 success still refreshes analytical.
                # Daily CVM Ingest #209 (2026-09-02) exited 1 on 403; run
                # 33721538761 (2026-09-03) did the same on 408 after 3.1M CVM
                # rows; #219 (run 34015471961, 2026-09-06) did the same on
                # ABORTED after 3.6M rows — all skipped ANALYZE and analytical.
                logger.error(
                    "ETF market scrape skipped — Apify did not return a dataset: %s",
                    exc,
                )
                print(f"::warning title=ETF scrape skipped::{exc}", flush=True)
            else:
                logger.error("ETF market scrape failed: %s", exc, exc_info=True)
                failures.append(("etf_market", exc))
    else:
        logger.info("APIFY_TOKEN unset — skipping ETF market scrape")

    elapsed = time.monotonic() - start_ts
    total_rows = sum(totals.values())
    logger.info(
        "Daily update done in %.1fs — %d rows upserted: %s",
        elapsed, total_rows, totals,
    )

    if failures:
        summary = "; ".join(f"{name}: {exc}" for name, exc in failures)
        logger.error("Daily update FAILED for %d source(s) — %s",
                     len(failures), summary)
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
