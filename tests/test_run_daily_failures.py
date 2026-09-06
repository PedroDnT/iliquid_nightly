"""run_daily must surface sub-ingest failures instead of warning past them.

Motivation: every optional source (BACEN, ANBIMA, ETF market) used to be wrapped
in `try/except -> logger.warning`, so a broken source looked identical to a
healthy one in CI. That is how the ANBIMA ingest failed on every daily run for
months unnoticed, and it contradicts the repo's "no silent except" rule.

Contract now: all sources still run (one failure must not skip the rest), but the
process exits non-zero if any of them failed.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.pipeline.run_daily as rd


def _patches(cvm_totals=None, bacen=None, anbima=None, b3=None):
    """Patch the daily ingestors; each arg is either a return value or an Exception."""
    cvm = MagicMock()
    cvm.daily_update = AsyncMock(return_value=cvm_totals or {"cvm_fi_diario": 1})
    # A MagicMock for .failures is truthy and iterable — that would make every
    # healthy-path test fail once run_daily checks the CVM slice ledger.
    cvm.failures = []

    bacen_ing = MagicMock()
    bacen_ing.backfill = AsyncMock()
    if isinstance(bacen, Exception):
        bacen_ing.backfill.side_effect = bacen
    else:
        bacen_ing.backfill.return_value = bacen or {"bacen_sgs": 2}

    anbima_ing = MagicMock()
    anbima_ing.daily_update = AsyncMock()
    if isinstance(anbima, Exception):
        anbima_ing.daily_update.side_effect = anbima
    else:
        anbima_ing.daily_update.return_value = anbima or {"anbima_etf": 3}

    b3_ing = MagicMock()
    # Corporate events run in their own guarded block after daily_update, so
    # the mock needs it too — otherwise every daily-run test reports a
    # b3_corporate_events failure that the code under test did not have.
    b3_ing.ingest_corporate_events = AsyncMock(return_value=0)
    b3_ing.daily_update = AsyncMock()
    if isinstance(b3, Exception):
        b3_ing.daily_update.side_effect = b3
    else:
        b3_ing.daily_update.return_value = b3 or {"b3_cotahist": 4}

    return (
        patch.object(rd, "CVMIngestor", return_value=cvm),
        patch.object(rd, "BacenIngestor", return_value=bacen_ing),
        patch.object(rd, "AnbimaIngestor", return_value=anbima_ing),
        patch.object(rd, "B3Ingestor", return_value=b3_ing),
        cvm, bacen_ing, anbima_ing, b3_ing,
    )


@pytest.mark.asyncio
async def test_all_healthy_exits_normally(monkeypatch):
    monkeypatch.delenv("APIFY_TOKEN", raising=False)
    p1, p2, p3, p4, *_ = _patches()
    with p1, p2, p3, p4:
        await rd.main()  # no SystemExit


@pytest.mark.asyncio
async def test_one_failure_exits_nonzero(monkeypatch):
    monkeypatch.delenv("APIFY_TOKEN", raising=False)
    p1, p2, p3, p4, *_ = _patches(anbima=RuntimeError("column does not exist"))
    with p1, p2, p3, p4:
        with pytest.raises(SystemExit) as exc:
            await rd.main()
    assert exc.value.code == 1


@pytest.mark.asyncio
async def test_earlier_failure_does_not_skip_later_sources(monkeypatch):
    """BACEN blowing up must not prevent ANBIMA or B3 from running."""
    monkeypatch.delenv("APIFY_TOKEN", raising=False)
    p1, p2, p3, p4, _cvm, _bacen, anbima_ing, b3_ing = _patches(
        bacen=RuntimeError("bacen down"))
    with p1, p2, p3, p4:
        with pytest.raises(SystemExit):
            await rd.main()
    anbima_ing.daily_update.assert_awaited()  # ran despite the earlier failure
    b3_ing.daily_update.assert_awaited()


@pytest.mark.asyncio
async def test_multiple_failures_all_reported(monkeypatch, caplog):
    monkeypatch.delenv("APIFY_TOKEN", raising=False)
    p1, p2, p3, p4, *_ = _patches(
        bacen=RuntimeError("bacen down"),
        anbima=RuntimeError("anbima down"),
    )
    with p1, p2, p3, p4:
        with caplog.at_level("ERROR"):
            with pytest.raises(SystemExit):
                await rd.main()
    assert "2 source(s)" in caplog.text
    assert "bacen" in caplog.text and "anbima" in caplog.text


@pytest.mark.asyncio
async def test_absent_apify_token_is_not_a_failure(monkeypatch):
    """A missing optional token skips the scrape; it must not fail the run."""
    monkeypatch.delenv("APIFY_TOKEN", raising=False)
    p1, p2, p3, p4, *_ = _patches()
    with p1, p2, p3, p4:
        await rd.main()


@pytest.mark.asyncio
async def test_etf_scrape_failure_exits_nonzero(monkeypatch):
    monkeypatch.setenv("APIFY_TOKEN", "tok")
    p1, p2, p3, p4, *_ = _patches()
    with p1, p2, p3, p4, \
         patch("src.pipeline.ingest_etf_market.ingest_etf_market",
               side_effect=RuntimeError("scrape blocked")), \
         patch("src.store.pg_client.get_pg_client", return_value=MagicMock()):
        with pytest.raises(SystemExit) as exc:
            await rd.main()
    assert exc.value.code == 1


@pytest.mark.asyncio
async def test_etf_actor_not_approved_is_not_a_failure(monkeypatch, caplog):
    """Apify 403 full-permission-actor-not-approved never started a scrape.

    Daily CVM Ingest #209 ingested 3.1M CVM rows then exited 1 on this error,
    which skipped ANALYZE and the analytical layer. Same class as an unset
    APIFY_TOKEN: skip, do not fail the daily run.
    """
    from src.fetchers.apify_etf_fetcher import ApifyActorNotApprovedError

    monkeypatch.setenv("APIFY_TOKEN", "tok")
    p1, p2, p3, p4, *_ = _patches()
    with p1, p2, p3, p4, \
         patch(
             "src.pipeline.ingest_etf_market.ingest_etf_market",
             side_effect=ApifyActorNotApprovedError(
                 "approve at https://console.apify.com/actors/moJRLRc85AitArpNN"
                 "?approvePermissions=true"
             ),
         ), \
         patch("src.store.pg_client.get_pg_client", return_value=MagicMock()), \
         caplog.at_level("ERROR"):
        await rd.main()  # no SystemExit
    assert "did not return a dataset" in caplog.text


@pytest.mark.asyncio
async def test_etf_run_timeout_is_not_a_failure(monkeypatch, caplog):
    """Apify 408 run-timeout-exceeded never delivered a dataset.

    Daily CVM Ingest 33721538761 ingested 3.1M CVM rows then exited 1 on this
    error, which skipped ANALYZE and the analytical layer. Same class as an
    unset APIFY_TOKEN: skip, do not fail the daily run.
    """
    from src.fetchers.apify_etf_fetcher import ApifyRunTimeoutError

    monkeypatch.setenv("APIFY_TOKEN", "tok")
    p1, p2, p3, p4, *_ = _patches()
    with p1, p2, p3, p4, \
         patch(
             "src.pipeline.ingest_etf_market.ingest_etf_market",
             side_effect=ApifyRunTimeoutError(
                 "HTTP 408 run-timeout-exceeded: Actor run exceeded the "
                 "timeout of 300 seconds for this API endpoint"
             ),
         ), \
         patch("src.store.pg_client.get_pg_client", return_value=MagicMock()), \
         caplog.at_level("ERROR"):
        await rd.main()  # no SystemExit
    assert "did not return a dataset" in caplog.text


@pytest.mark.asyncio
async def test_etf_run_aborted_is_not_a_failure(monkeypatch, caplog):
    """Apify ABORTED never delivered a dataset.

    Daily CVM Ingest #219 (run 34015471961, 2026-09-06) ingested ~3.6M
    CVM/BACEN/B3 rows then exited 1 on ended ABORTED after ~11 minutes
    (run d2UCTxohVY9IcQX9a), which skipped ANALYZE and the analytical
    layer. Same class as an unset APIFY_TOKEN: skip, do not fail the
    daily run.
    """
    from src.fetchers.apify_etf_fetcher import ApifyRunAbortedError

    monkeypatch.setenv("APIFY_TOKEN", "tok")
    p1, p2, p3, p4, *_ = _patches()
    with p1, p2, p3, p4, \
         patch(
             "src.pipeline.ingest_etf_market.ingest_etf_market",
             side_effect=ApifyRunAbortedError(
                 "Apify actor apify~playwright-scraper run "
                 "d2UCTxohVY9IcQX9a was aborted (status=ABORTED) — no dataset"
             ),
         ), \
         patch("src.store.pg_client.get_pg_client", return_value=MagicMock()), \
         caplog.at_level("ERROR"):
        await rd.main()  # no SystemExit
    assert "did not return a dataset" in caplog.text
    assert "ABORTED" in caplog.text


@pytest.mark.asyncio
async def test_b3_failure_exits_nonzero(monkeypatch):
    monkeypatch.delenv("APIFY_TOKEN", raising=False)
    p1, p2, p3, p4, *_ = _patches(b3=RuntimeError("cotahist 500"))
    with p1, p2, p3, p4:
        with pytest.raises(SystemExit) as exc:
            await rd.main()
    assert exc.value.code == 1


@pytest.mark.asyncio
async def test_cvm_slice_failures_exit_nonzero_after_other_sources(monkeypatch):
    """CVMHostUnreachable per slice must fail the daily run, not only B3/BACEN.

    Run 33237536770 logged 44 CVM errors, upserted 0 CVM rows, then only
    went red because b3_corporate_events hit SSL EOF. BACEN/B3 must still
    run so a blocked CVM IP does not skip sources that work.
    """
    from src.pipeline.cvm_pipeline import SliceFailure

    monkeypatch.delenv("APIFY_TOKEN", raising=False)
    p1, p2, p3, p4, cvm, _bacen, anbima_ing, b3_ing = _patches()
    cvm.failures = [
        SliceFailure("fii", "mensal_geral", 2026, None, "CVMHostUnreachable"),
        SliceFailure("securit", "cra_mensal", 2026, None, "CVMHostUnreachable"),
    ]
    with p1, p2, p3, p4:
        with pytest.raises(SystemExit) as exc:
            await rd.main()
    assert exc.value.code == 1
    anbima_ing.daily_update.assert_awaited()
    b3_ing.daily_update.assert_awaited()
