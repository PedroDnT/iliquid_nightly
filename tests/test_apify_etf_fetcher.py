"""Apify ETF fetcher: async run + HTTP 403/408 mapping + ABORTED skip.

Daily CVM Ingest #209 (2026-09-02) failed Run daily update solely because
apify/web-scraper started requiring a Console permission grant
(full-permission-actor-not-approved). The scrape never started — that must
not look like a failed fetch of ETF data, and must not take down CVM ingest.

Daily CVM Ingest 33721538761 (2026-09-03) failed the same step on HTTP 408
run-timeout-exceeded: run-sync-get-dataset-items hard-caps at 300s, and ~187
playwright pages take longer. The fetcher must start the actor asynchronously
and treat a timeout as the same skip class as the 403.

Daily CVM Ingest #219 (run 34015471961, 2026-09-06) failed the same step on
a platform ABORTED status after ~11 minutes with no dataset. That is the
same skip class — not a scrape that returned bad data.
"""

import json
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import Request

import pytest

from src.fetchers.apify_etf_fetcher import (
    ApifyActorNotApprovedError,
    ApifyRunAbortedError,
    ApifyRunTimeoutError,
    ApifyScrapeUnavailableError,
    ApifyETFFetcher,
    apify_http_error,
)


_APPROVAL_BODY = """{
  "error": {
    "type": "full-permission-actor-not-approved",
    "message": "This Actor requires full access to your account. You must approve its permissions before running it: https://console.apify.com/actors/moJRLRc85AitArpNN?approvePermissions=true",
    "data": {
      "approvalUrl": "https://console.apify.com/actors/moJRLRc85AitArpNN?approvePermissions=true"
    }
  }
}"""

_TIMEOUT_BODY = """{
  "error": {
    "type": "run-timeout-exceeded",
    "message": "Actor run exceeded the timeout of 300 seconds for this API endpoint"
  }
}"""


class _Resp:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _fetcher(**kwargs) -> ApifyETFFetcher:
    return ApifyETFFetcher(token="tok", **kwargs)


def _http_error(url: str, code: int, msg: str, body: str) -> HTTPError:
    return HTTPError(url, code, msg, hdrs=None, fp=BytesIO(body.encode()))


def _urlopen_script(handlers):
    """Dispatch urlopen by (method, url-substring) → body dict/list or Exception."""

    def urlopen(req, timeout=None):
        assert isinstance(req, Request)
        method = req.get_method()
        url = req.full_url
        assert "run-sync-get-dataset-items" not in url
        for needle_method, needle, result in handlers:
            if method == needle_method and needle in url:
                if isinstance(result, Exception):
                    raise result
                payload = result if isinstance(result, bytes) else json.dumps(result).encode()
                return _Resp(payload)
        raise AssertionError(f"unexpected {method} {url}")

    return urlopen


class TestApifyHttpError:
    def test_permission_403_is_actor_not_approved(self):
        err = apify_http_error("apify~web-scraper", 403, _APPROVAL_BODY)
        assert isinstance(err, ApifyActorNotApprovedError)
        assert isinstance(err, ApifyScrapeUnavailableError)
        assert "approvePermissions=true" in str(err)
        assert "moJRLRc85AitArpNN" in str(err)

    def test_timeout_408_is_run_timeout(self):
        err = apify_http_error("apify~playwright-scraper", 408, _TIMEOUT_BODY)
        assert isinstance(err, ApifyRunTimeoutError)
        assert isinstance(err, ApifyScrapeUnavailableError)
        assert "run-timeout-exceeded" in str(err)
        assert "HTTP 408" in str(err)

    def test_other_403_still_raises_runtime_error(self):
        err = apify_http_error("apify~web-scraper", 403, '{"error":{"type":"billing"}}')
        assert type(err) is RuntimeError
        assert not isinstance(err, ApifyScrapeUnavailableError)
        assert "HTTP 403" in str(err)

    def test_500_is_runtime_error(self):
        err = apify_http_error("apify~playwright-scraper", 500, "boom")
        assert type(err) is RuntimeError
        assert not isinstance(err, ApifyScrapeUnavailableError)
        assert "HTTP 500" in str(err)


class TestBuildInput:
    def test_default_actor_is_limited_permission_playwright(self):
        assert _fetcher()._actor == "apify~playwright-scraper"

    def test_playwright_wait_until_is_string_enum(self):
        payload = _fetcher()._build_input(["BOVA11"])
        assert payload["waitUntil"] == "networkidle"
        assert "injectJQuery" not in payload
        assert payload["linkSelector"] == ""
        assert payload["startUrls"] == [
            {"url": "https://www.etfsbrasil.com.br/etfs/bova11"}
        ]

    def test_web_scraper_keeps_puppeteer_wait_until(self):
        payload = _fetcher(actor="apify~web-scraper")._build_input(["BOVA11"])
        assert payload["waitUntil"] == ["networkidle2"]
        assert payload["injectJQuery"] is False

    def test_empty_tickers_raise(self):
        with pytest.raises(ValueError, match="no tickers"):
            _fetcher()._build_input(["  ", ""])


class TestFetch:
    def test_permission_403_raises_actor_not_approved(self):
        err = _http_error(
            "https://api.apify.com/v2/acts/apify~web-scraper/runs",
            403,
            "Forbidden",
            _APPROVAL_BODY,
        )
        with patch("urllib.request.urlopen", side_effect=err):
            with pytest.raises(ApifyActorNotApprovedError) as exc:
                _fetcher(actor="apify~web-scraper").fetch(["BOVA11"])
        assert "approvePermissions=true" in str(exc.value)

    def test_sync_endpoint_408_raises_run_timeout(self):
        err = _http_error(
            "https://api.apify.com/v2/acts/apify~playwright-scraper/runs",
            408,
            "Request Timeout",
            _TIMEOUT_BODY,
        )
        with patch("urllib.request.urlopen", side_effect=err):
            with pytest.raises(ApifyRunTimeoutError) as exc:
                _fetcher().fetch(["BOVA11"])
        assert "run-timeout-exceeded" in str(exc.value)

    def test_starts_async_polls_and_reads_dataset(self):
        urlopen = _urlopen_script([
            ("POST", "/acts/apify~playwright-scraper/runs", {
                "data": {
                    "id": "run1",
                    "status": "RUNNING",
                    "defaultDatasetId": "ds1",
                }
            }),
            ("GET", "/actor-runs/run1", {
                "data": {
                    "id": "run1",
                    "status": "SUCCEEDED",
                    "defaultDatasetId": "ds1",
                }
            }),
            ("GET", "/datasets/ds1/items", [{"ticker": "BOVA11"}]),
        ])
        with patch("urllib.request.urlopen", side_effect=urlopen):
            items = _fetcher().fetch(["BOVA11"])
        assert items == [{"ticker": "BOVA11"}]

    def test_empty_dataset_raises(self):
        urlopen = _urlopen_script([
            ("POST", "/acts/apify~playwright-scraper/runs", {
                "data": {"id": "run1", "status": "SUCCEEDED", "defaultDatasetId": "ds1"}
            }),
            ("GET", "/datasets/ds1/items", []),
        ])
        with patch("urllib.request.urlopen", side_effect=urlopen):
            with pytest.raises(RuntimeError, match="empty dataset"):
                _fetcher().fetch(["BOVA11"])

    def test_failed_run_raises_runtime_error(self):
        urlopen = _urlopen_script([
            ("POST", "/acts/apify~playwright-scraper/runs", {
                "data": {"id": "run1", "status": "RUNNING", "defaultDatasetId": "ds1"}
            }),
            ("GET", "/actor-runs/run1", {
                "data": {"id": "run1", "status": "FAILED", "defaultDatasetId": "ds1"}
            }),
        ])
        with patch("urllib.request.urlopen", side_effect=urlopen):
            with pytest.raises(RuntimeError, match="ended FAILED") as exc:
                _fetcher().fetch(["BOVA11"])
        assert type(exc.value) is RuntimeError
        assert not isinstance(exc.value, ApifyScrapeUnavailableError)

    def test_aborted_run_is_scrape_unavailable(self):
        """Daily CVM Ingest #219 (run 34015471961): Apify ended ABORTED
        after ~11 min with no dataset. Same skip class as 403/408.
        """
        urlopen = _urlopen_script([
            ("POST", "/acts/apify~playwright-scraper/runs", {
                "data": {"id": "d2UCTxohVY9IcQX9a", "status": "READY"}
            }),
            ("GET", "/actor-runs/d2UCTxohVY9IcQX9a", {
                "data": {
                    "id": "d2UCTxohVY9IcQX9a",
                    "status": "ABORTED",
                    "defaultDatasetId": "ds1",
                }
            }),
        ])
        with patch("urllib.request.urlopen", side_effect=urlopen):
            with pytest.raises(ApifyRunAbortedError, match="status=ABORTED") as exc:
                _fetcher().fetch(["BOVA11"])
        assert isinstance(exc.value, ApifyScrapeUnavailableError)
        assert "d2UCTxohVY9IcQX9a" in str(exc.value)

    def test_aborting_run_is_scrape_unavailable(self):
        urlopen = _urlopen_script([
            ("POST", "/acts/apify~playwright-scraper/runs", {
                "data": {"id": "run1", "status": "RUNNING"}
            }),
            ("GET", "/actor-runs/run1", {
                "data": {"id": "run1", "status": "ABORTING"}
            }),
        ])
        with patch("urllib.request.urlopen", side_effect=urlopen):
            with pytest.raises(ApifyRunAbortedError, match="status=ABORTING") as exc:
                _fetcher().fetch(["BOVA11"])
        assert isinstance(exc.value, ApifyScrapeUnavailableError)

    def test_wait_budget_miss_aborts_and_raises_timeout(self):
        calls = []

        def urlopen(req, timeout=None):
            calls.append(req.get_method() + " " + req.full_url)
            if req.get_method() == "POST" and "/runs" in req.full_url and "/abort" not in req.full_url:
                return _Resp(json.dumps({
                    "data": {"id": "run1", "status": "RUNNING", "defaultDatasetId": "ds1"}
                }).encode())
            if req.get_method() == "POST" and "/abort" in req.full_url:
                return _Resp(json.dumps({"data": {"id": "run1", "status": "ABORTING"}}).encode())
            raise AssertionError(req.full_url)

        with patch("urllib.request.urlopen", side_effect=urlopen):
            with pytest.raises(ApifyRunTimeoutError, match="did not finish within 0s"):
                _fetcher(timeout_secs=0).fetch(["BOVA11"])
        assert any("/abort" in c for c in calls)
        assert all("run-sync-get-dataset-items" not in c for c in calls)

    def test_network_error_raises(self):
        with patch(
            "urllib.request.urlopen",
            side_effect=URLError("timed out"),
        ):
            with pytest.raises(RuntimeError, match="network"):
                _fetcher().fetch(["BOVA11"])


class TestWaitBudgetDefault:
    def test_default_covers_the_measured_scrape_with_margin(self, monkeypatch):
        """Apify run 7RmFKAYQfqWraHaX6 (Daily CVM Ingest 33798733736, 2026-09-03)
        took 1,145 s for 178 tickers. The original 1,200 s default left 55 s of
        margin; a slower day would have skipped the snapshot. The default must
        stay comfortably above the measured time and below the job's 180 min.
        """
        monkeypatch.delenv("APIFY_ETF_TIMEOUT_SECS", raising=False)
        budget = ApifyETFFetcher(token="tok")._timeout
        assert budget >= 2 * 1145, "wait budget must be at least 2x the measured scrape"
        assert budget < 180 * 60

    def test_env_overrides_default(self, monkeypatch):
        monkeypatch.setenv("APIFY_ETF_TIMEOUT_SECS", "900")
        assert ApifyETFFetcher(token="tok")._timeout == 900
