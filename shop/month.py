"""Month end: close every day of the month that isn't closed yet, then roll up.

    moksha start month_end --param month=2026-10 --wait

It looks each day up in mart.shop_daily (ctx.latest_version). A day with
no summary yet is closed by a run of daily_close of its own (ctx.run, a
sub-run you can open from this run's page), with auto_approve: at month
end nobody waits for a person per day. A day that fails (say, rejected)
is reported, not fatal, unless more than `max_failed` do.

Run it twice: the second time every day is already closed, so it starts
no sub-runs, and monthly_rollup is reused.
"""

import calendar
import datetime as dt

import moksha as mk

from .close import daily_close
from .steps import monthly_rollup


@mk.pipeline(title="Month end")
def month_end(ctx, month: str, max_failed: int = 3):
    """Closes the month's open days (sub-runs of Close the day) and rolls the month up."""
    year, mon = map(int, month.split("-"))
    days = [dt.date(year, mon, i).isoformat() for i in range(1, calendar.monthrange(year, mon)[1] + 1)]

    closed, failed, started = [], {}, 0
    for day in days:
        v = ctx.latest_version("mart.shop_daily", partition=day)
        if v is None:
            started += 1
            try:
                ctx.run(daily_close, day=day, auto_approve=True)
            except mk.RunFailed as e:
                failed[day] = f"{e.reason}: {e.message}"
                if len(failed) > max_failed:
                    raise RuntimeError(f"{len(failed)} days failed to close, more than {max_failed}: {failed}")
                continue
            v = ctx.latest_version("mart.shop_daily", partition=day)
            if v is None:  # the run ended without closing the day (it was held)
                failed[day] = "not closed"
                continue
        closed.append(v.meta)

    rollup = ctx.step(monthly_rollup, closed, month)
    return {"month": month, "closed": len(closed), "closed_now": started - len(failed), "failed": failed,
            "revenue_eur": rollup.result["revenue_eur"], "partial_days": rollup.result["partial_days"]}
