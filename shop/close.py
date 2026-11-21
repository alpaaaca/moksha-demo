"""Close the books for one day: four regions in parallel, an anomaly gate.

    moksha start daily_close --param day=2026-10-03 --wait
    moksha replay daily_close --param day=2026-10-01..2026-10-16

             load_catalog ─┐
    fx_rates (flaky) ──────┤
    extract_region × 4 ────┼─► clean_region × 4 ─► check_anomalies ─► [approve?] ─► daily_summary ─► publish_report
      (latam may be late) ─┘     (queue: compute)      ▲ last 7 days' summaries
                                                       (ctx.latest_version)

What it shows, beyond revenue/:

- Fan-out: ctx.step_async starts the four extracts at once, and each
  region's clean once its extract is in; the pipeline's max_parallel caps how
  many of its steps run at once.
- Failing well: extract_region retries a late file (Retry, exponential);
  when it still isn't there the run catches StepFailed and closes the day
  without that region, marked partial. fx_rates retries a flaky API, and
  raises NonRetryable when retrying can't help.
- A person in the loop: when a day looks off, the run waits (durably: a
  restart or a deploy doesn't lose it) for a signal:

      moksha signal <run> approve --data '{"by": "ana"}'
      moksha signal <run> approve --data '{"by": "ana", "reject": "duplicated feed"}'

  No answer in 24 hours, and the day stays open (the run ends "held").
- History: the gate compares today with the summaries of the seven days
  before, from their data versions' meta (no data read).
"""

import datetime as dt
from datetime import timedelta

import moksha as mk

from .steps import (REGIONS, check_anomalies, clean_region, daily_summary, extract_region, fx_rates, load_catalog,
                    publish_report, shard)

HISTORY_DAYS = 7


class Rejected(Exception):
    """Someone looked at the day and said no."""


@mk.pipeline(title="Close the day", max_parallel=6)
def daily_close(ctx, day: str, auto_approve: bool = False):
    """One day's books from four regions, held for approval when they look off."""
    # Start everything that doesn't depend on anything, at once.
    catalog_h = ctx.step_async(load_catalog)
    fx_h = ctx.step_async(fx_rates, day)
    extracts = {r: ctx.step_async(extract_region, shard(day, r)) for r in REGIONS}
    catalog, fx = ctx.gather([catalog_h, fx_h])

    # Clean each region as soon as its extract is in; a region that never
    # delivered is left out (gathered one by one, so one failure doesn't
    # fail the rest).
    cleans, missing = {}, []
    for r, h in extracts.items():
        try:
            (orders,) = ctx.gather([h])
        except mk.StepFailed as e:
            missing.append(r)
            ctx.state.setdefault("why_missing", {})[r] = e.message
            continue
        cleans[r] = ctx.step_async(clean_region, orders, fx, catalog, shard(day, r))
    if not cleans:
        raise RuntimeError(f"no region delivered orders for {day}: {ctx.state.get('why_missing')}")
    parts = [v.result for v in ctx.gather(list(cleans.values()))]

    # The gate: today against the week before.
    d = dt.date.fromisoformat(day)
    history = []
    for i in range(HISTORY_DAYS, 0, -1):
        v = ctx.latest_version("mart.shop_daily", partition=(d - timedelta(days=i)).isoformat())
        if v is not None:
            history.append(v.meta)
    flags = ctx.step(check_anomalies, parts, history)

    approved_by = "auto" if not flags else ""
    if flags and auto_approve:
        approved_by = "auto (auto_approve)"
    elif flags:
        try:
            answer = ctx.wait_signal("approve", timeout=timedelta(hours=24))
        except mk.SignalTimeout:
            return {"day": day, "status": "held", "flags": flags}
        answer = answer or {}
        if answer.get("reject"):
            raise Rejected(f"{answer.get('by', 'someone')} rejected {day}: {answer['reject']}")
        approved_by = answer.get("by", "someone")

    summary = ctx.step(daily_summary, parts, day, missing, flags, approved_by)
    report = ctx.step(publish_report, summary, day)
    return {"day": day, "status": "partial" if missing else "closed", "revenue": report,
            "missing": missing, "flags": flags, "approved_by": approved_by}
