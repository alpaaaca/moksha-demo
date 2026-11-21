"""The dashboard: rebuilt by itself whenever days are closed.

No one starts this pipeline: it subscribes to mart.shop_daily, so every
new daily summary starts it. A backfill of thirty days doesn't start it
thirty times: new versions are collected until none came for 20 seconds
(debounce), or for two minutes at most (max_wait), and one run takes
them all.

It draws the 14 days up to the newest one in the batch, from their
summaries' meta. The result, reports.shop_dashboard, keeps its last ten
versions (Keep): open dashboard.html from the web UI's datasets page.
"""

import datetime as dt
from datetime import timedelta

import moksha as mk

from .steps import render_dashboard

WINDOW = 14


@mk.pipeline(title="Refresh the dashboard",
             subscribe=mk.Subscription("mart.shop_daily", debounce=20, max_wait=120))
def refresh_dashboard(ctx, versions: list):
    """The last two weeks of revenue, redrawn when new days are closed."""
    newest = dt.date.fromisoformat(max(v.partition for v in versions))
    days = []
    for i in range(WINDOW - 1, -1, -1):
        v = ctx.latest_version("mart.shop_daily", partition=(newest - timedelta(days=i)).isoformat())
        if v is not None:
            days.append(v.meta)
    shown = ctx.step(render_dashboard, days)
    return {"new_days": sorted(v.partition for v in versions), "shown": len(days), "through": shown.result}
