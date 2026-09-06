"""Apify-backed ETF market fetcher — scrapes etfsbrasil.com.br/etfs/<ticker>.

Why this exists
---------------
CVM open data does not expose ETF NAV / quotaholders for post-CVM-175 share
classes (etf_daily is empty — the registry's fund-level CNPJ no longer matches
the class-level CNPJ in cvm_fi_diario). etfsbrasil.com.br carries the per-ETF NAV,
number of cotistas, returns, fees and index, but renders NAV/cotistas via JS and
rate-limits direct scraping — so we drive a headless-browser Apify actor with
rotating RESIDENTIAL proxies.

Default actor is ``apify/playwright-scraper`` (limited permissions). The older
default ``apify/web-scraper`` was upgraded to full permissions on 2026-08-31;
until an operator approves it in Console, the API returns HTTP 403
``full-permission-actor-not-approved``. That error is raised as
``ApifyActorNotApprovedError`` so the daily run can skip the scrape the same
way it skips an unset ``APIFY_TOKEN`` — the scrape never started, so there is
no data to fabricate or to fail the rest of ingest over.

The actor is started asynchronously and polled until it finishes. Apify's
``run-sync-get-dataset-items`` endpoint hard-caps at 300 seconds (HTTP 408
``run-timeout-exceeded``); Daily CVM Ingest run 33721538761 (2026-09-03)
scraped ~187 playwright pages, hit that cap after CVM/BACEN/B3 had already
landed, and exited 1 — which skipped ANALYZE and the analytical layer. A 408
or a wait-budget miss is ``ApifyRunTimeoutError`` (same skip class): we never
got a dataset, so there is nothing to fabricate.

Daily CVM Ingest #219 (run 34015471961, 2026-09-06) then failed the same way
on a platform ``ABORTED`` status after ~11 minutes (run ``d2UCTxohVY9IcQX9a``).
CVM/BACEN/ANBIMA/B3 had already upserted ~3.6M rows. ``ABORTED`` / ``ABORTING``
are ``ApifyRunAbortedError`` (same skip class): the actor was killed before
delivering a dataset. An actor that ran and ended ``FAILED``, or that returned
an empty dataset, still raises a hard ``RuntimeError``.

Public surface
--------------
    ApifyETFFetcher().fetch(tickers) -> list[dict]   # one scraped record per ticker

The actor's pageFunction lives in apify/etfsbrasil_scraper.js (read at call time
and passed in the run input), so there is nothing to pre-deploy on Apify — only an
API token is required.

Configuration (env)
-------------------
    APIFY_TOKEN              required — Apify API token.
    APIFY_ETF_ACTOR          optional — actor id (default 'apify~playwright-scraper').
    APIFY_PROXY_GROUPS       optional — comma list (default 'RESIDENTIAL').
    APIFY_ETF_TIMEOUT_SECS   optional — wait budget for the actor run (default 2400).

Data-integrity: a failed run (non-2xx, actor FAILED, or empty dataset)
RAISES — it never returns a plausible-looking empty/fallback result. Timeouts
raise ``ApifyRunTimeoutError`` (no rows). Platform abort (``ABORTED`` /
``ABORTING``) raises ``ApifyRunAbortedError`` (no rows). Parsing/validation
and the DB upsert live in src/pipeline/ingest_etf_market.py.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_ETF_URL = "https://www.etfsbrasil.com.br/etfs/{ticker}"
_PAGE_FUNCTION_PATH = Path(__file__).resolve().parents[2] / "apify" / "etfsbrasil_scraper.js"
_APIFY_BASE = "https://api.apify.com/v2"
_DEFAULT_ACTOR = "apify~playwright-scraper"
# Wait budget for one scrape, in seconds. Measured: the first async run
# (Apify run 7RmFKAYQfqWraHaX6, Daily CVM Ingest 33798733736, 2026-09-03)
# took 1,145 s for 178 tickers at concurrency 5 — 55 s short of the original
# 1,200 s default, so any slower day would have been skipped. 2,400 s is
# roughly twice the measured time; the daily job's own timeout is 180 min.
_DEFAULT_TIMEOUT_SECS = 2400
_APPROVAL_ERROR_TYPE = "full-permission-actor-not-approved"
_TIMEOUT_ERROR_TYPE = "run-timeout-exceeded"
_TERMINAL_STATUSES = frozenset(
    {"SUCCEEDED", "FAILED", "TIMING-OUT", "TIMED-OUT", "ABORTING", "ABORTED"}
)
_POLL_WAIT_SECS = 60
_HTTP_BUFFER_SECS = 30


class ApifyScrapeUnavailableError(RuntimeError):
    """The Apify scrape never delivered a dataset.

    Distinct from a scrape that ran and returned bad or empty data: we have
    nothing to upsert. Same class of unavailability as an unset APIFY_TOKEN —
    the daily run skips it so CVM/BACEN/B3 success still refreshes analytical.
    """


class ApifyActorNotApprovedError(ApifyScrapeUnavailableError):
    """The Apify store actor needs a one-time Console permission grant.

    Distinct from a scrape that ran and returned bad data: the actor never
    started. Same class of unavailability as an unset APIFY_TOKEN.
    """


class ApifyRunTimeoutError(ApifyScrapeUnavailableError):
    """The actor did not finish within Apify's endpoint cap or our wait budget.

    Distinct from a scrape that returned bad data: we never got a dataset.
    Daily CVM Ingest 33721538761 hit HTTP 408 run-timeout-exceeded on the
    300s sync endpoint after ~187 playwright pages; skipping this (like an
    unset token) is what lets ANALYZE + analytical still run.
    """


class ApifyRunAbortedError(ApifyScrapeUnavailableError):
    """The actor run was aborted on Apify's side before delivering a dataset.

    Distinct from a scrape that returned bad data: we never got a dataset.
    Daily CVM Ingest #219 (run 34015471961, 2026-09-06) ingested ~3.6M
    CVM/BACEN/B3 rows then exited 1 on ``ended ABORTED`` after ~11 minutes
    (run d2UCTxohVY9IcQX9a), which skipped ANALYZE and the analytical layer.
    Same skip class as an unset token, a 403, or a timeout.
    """


class ApifyETFFetcher:
    """Runs the etfsbrasil scraper on Apify and returns the dataset items."""

    def __init__(
        self,
        token: Optional[str] = None,
        actor: Optional[str] = None,
        timeout_secs: Optional[int] = None,
    ) -> None:
        self._token = token or os.getenv("APIFY_TOKEN")
        if not self._token:
            raise RuntimeError(
                "APIFY_TOKEN is not set — required to run the etfsbrasil ETF scraper"
            )
        # Apify actor ids use '~' between owner and name in the REST path.
        self._actor = actor or os.getenv("APIFY_ETF_ACTOR", _DEFAULT_ACTOR)
        if timeout_secs is not None:
            self._timeout = timeout_secs
        else:
            self._timeout = int(os.getenv("APIFY_ETF_TIMEOUT_SECS", str(_DEFAULT_TIMEOUT_SECS)))
        self._proxy_groups = [
            g.strip()
            for g in os.getenv("APIFY_PROXY_GROUPS", "RESIDENTIAL").split(",")
            if g.strip()
        ]
        self._page_function = _PAGE_FUNCTION_PATH.read_text(encoding="utf-8")

    def _uses_playwright(self) -> bool:
        return "playwright" in self._actor.lower()

    def _build_input(self, tickers: List[str]) -> Dict[str, Any]:
        start_urls = [
            {"url": _ETF_URL.format(ticker=t.strip().lower())}
            for t in tickers
            if t and t.strip()
        ]
        if not start_urls:
            raise ValueError("ApifyETFFetcher.fetch called with no tickers")
        payload: Dict[str, Any] = {
            "startUrls": start_urls,
            "pageFunction": self._page_function,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": self._proxy_groups,
            },
            "maxConcurrency": int(os.getenv("APIFY_ETF_CONCURRENCY", "5")),
            "maxRequestRetries": 3,
            "pageLoadTimeoutSecs": 60,
            # Start URLs only — do not enqueue the rest of etfsbrasil.com.br.
            "linkSelector": "",
        }
        if self._uses_playwright():
            # Playwright enum is a string; Puppeteer web-scraper wants a list.
            payload["waitUntil"] = "networkidle"
            payload["pageFunctionTimeoutSecs"] = 90
        else:
            payload["waitUntil"] = ["networkidle2"]
            payload["injectJQuery"] = False
        return payload

    def fetch(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """Run the scraper and return one record per scraped ticker.

        Starts the actor asynchronously, then polls until it finishes or the
        wait budget elapses. Raises on any transport error, non-2xx status,
        a non-SUCCEEDED terminal status, or an empty dataset — a failed scrape
        must surface, never masquerade as "no ETFs". Timeout and abort are
        ``ApifyScrapeUnavailableError`` subclasses (no dataset); ``FAILED``
        and empty datasets stay hard ``RuntimeError``.
        """
        run_input = self._build_input(tickers)
        run = self._start_run(run_input)
        run_id = run["id"]
        status = run.get("status")
        logger.info(
            "etfsbrasil scrape: Apify run %s started (status=%s, wait_budget=%ss, tickers=%d)",
            run_id, status, self._timeout, len(tickers),
        )
        if status not in _TERMINAL_STATUSES:
            run = self._wait_for_run(run_id)
            status = run.get("status")
        if status == "TIMED-OUT" or status == "TIMING-OUT":
            raise ApifyRunTimeoutError(
                f"Apify actor {self._actor} run {run_id} timed out on Apify's side "
                f"(status={status})"
            )
        if status == "ABORTED" or status == "ABORTING":
            raise ApifyRunAbortedError(
                f"Apify actor {self._actor} run {run_id} was aborted "
                f"(status={status}) — no dataset"
            )
        if status != "SUCCEEDED":
            raise RuntimeError(
                f"Apify run failed for etfsbrasil scrape: run {run_id} ended "
                f"{status}"
            )
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise RuntimeError(
                f"Apify run {run_id} succeeded but returned no defaultDatasetId"
            )
        items = self._dataset_items(str(dataset_id))
        if not isinstance(items, list) or not items:
            raise RuntimeError(
                "Apify etfsbrasil scrape returned an empty dataset — refusing to "
                "treat as success (check token, actor, proxy blocks, or selectors)"
            )
        logger.info("etfsbrasil scrape: %d ETF records for %d tickers",
                    len(items), len(tickers))
        return items

    def _start_run(self, run_input: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(run_input).encode("utf-8")
        payload = self._request_json("POST", f"/acts/{self._actor}/runs", body, timeout=60)
        return _unwrap_run(payload)

    def _wait_for_run(self, run_id: str) -> Dict[str, Any]:
        deadline = time.monotonic() + self._timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._abort_run(run_id)
                raise ApifyRunTimeoutError(
                    f"Apify actor {self._actor} run {run_id} did not finish "
                    f"within {self._timeout}s"
                )
            wait = min(_POLL_WAIT_SECS, max(1, int(remaining)))
            started = time.monotonic()
            run = self._get_run(run_id, wait_for_finish=wait)
            if run.get("status") in _TERMINAL_STATUSES:
                return run
            # waitForFinish is best-effort; if the GET returned immediately
            # still RUNNING, do not spin the runner CPU until the budget ends.
            elapsed = time.monotonic() - started
            leftover = deadline - time.monotonic()
            if elapsed < 1 and leftover > 0:
                time.sleep(min(2.0, leftover))

    def _get_run(self, run_id: str, wait_for_finish: int = 0) -> Dict[str, Any]:
        path = f"/actor-runs/{run_id}"
        if wait_for_finish:
            path += f"?waitForFinish={wait_for_finish}"
        payload = self._request_json(
            "GET", path, timeout=wait_for_finish + _HTTP_BUFFER_SECS,
        )
        return _unwrap_run(payload)

    def _dataset_items(self, dataset_id: str) -> Any:
        return self._request_json(
            "GET", f"/datasets/{dataset_id}/items?format=json", timeout=60,
        )

    def _abort_run(self, run_id: str) -> None:
        try:
            self._request("POST", f"/actor-runs/{run_id}/abort", timeout=30)
        except Exception as exc:
            logger.warning("Apify abort of run %s failed: %s", run_id, exc)

    def _request_json(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        *,
        timeout: float,
    ) -> Any:
        return json.loads(self._request(method, path, body, timeout=timeout))

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        *,
        timeout: float,
    ) -> str:
        sep = "&" if "?" in path else "?"
        url = f"{_APIFY_BASE}{path}{sep}token={self._token}"
        data = body
        if data is None and method != "GET":
            data = b""
        headers = {"Content-Type": "application/json"} if body is not None else {}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:500]
            raise apify_http_error(self._actor, exc.code, detail) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Apify run failed (network): {exc}") from exc


def apify_http_error(actor: str, code: int, detail: str) -> RuntimeError:
    """Map an Apify HTTP error body to the exception the daily run should see."""
    approval_url = _approval_url(detail)
    if code == 403 and (
        _APPROVAL_ERROR_TYPE in detail or approval_url is not None
    ):
        where = approval_url or (
            f"https://console.apify.com/actors/{actor.replace('~', '/')}?approvePermissions=true"
        )
        return ApifyActorNotApprovedError(
            f"Apify actor {actor} requires a one-time Console permission "
            f"approval before it can run. Approve at {where} — until then "
            f"the scrape cannot start (HTTP {code})."
        )
    if code == 408 or _TIMEOUT_ERROR_TYPE in detail:
        return ApifyRunTimeoutError(
            f"Apify actor {actor} did not finish within the API wait "
            f"(HTTP {code} {_TIMEOUT_ERROR_TYPE}). The scrape is optional — "
            f"daily CVM ingest must not fail because of it. Detail: {detail}"
        )
    return RuntimeError(
        f"Apify run failed for etfsbrasil scrape: HTTP {code} — {detail}"
    )


def _unwrap_run(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"Apify run payload is not an object: {type(payload).__name__}"
        )
    data = payload.get("data", payload)
    if not isinstance(data, dict) or not data.get("id"):
        raise RuntimeError("Apify run payload missing id")
    return data


def _approval_url(detail: str) -> Optional[str]:
    try:
        payload = json.loads(detail)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    err = payload.get("error") or {}
    if not isinstance(err, dict):
        return None
    if err.get("type") != _APPROVAL_ERROR_TYPE:
        return None
    data = err.get("data") or {}
    if not isinstance(data, dict):
        return None
    url = data.get("approvalUrl")
    return url if isinstance(url, str) and url else None
