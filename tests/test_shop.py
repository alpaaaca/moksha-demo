"""The shop's pipelines against scripted results, with moksha.testing: no
server, no worker. Run with: python -m pytest tests (or python tests/test_shop.py).

The steps are plain functions, so the last tests call them directly on
the real source data.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import moksha as mk  # noqa: E402
from moksha import testing  # noqa: E402

from shop import steps  # noqa: E402
from shop.close import daily_close  # noqa: E402
from shop.month import month_end  # noqa: E402
from shop.returns import watch_returns  # noqa: E402


def part(region, revenue="1000.00", orders=40, rejected=0):
    return {"region": region, "orders": orders, "rejected": rejected, "revenue_eur": revenue, "cost_eur": "600.00",
            "customers": 30, "by_category": {"audio": revenue}, "top_customer": "c0001"}


def clean(orders, fx, catalog, shard):
    # What the step returns; the run gets a data version holding it as .result.
    return part(steps.split(shard)[1])


def scripted(**over):
    s = {"load_catalog": {"products": 10}, "fx_rates": {"EUR": "1"}, "extract_region": {"lines": 40},
         "clean_region": clean, "check_anomalies": [], "daily_summary": {"revenue_eur": "4000.00"},
         "publish_report": "€4,000.00"}
    return {**s, **over}


def test_clean_day_closes_without_asking():
    r = testing.run(daily_close, {"day": "2026-10-03"}, steps=scripted())
    assert r.status == "completed", r.message
    assert r.result["status"] == "closed" and r.result["approved_by"] == "auto"
    names = [c.name for c in r.steps]
    assert names.count("extract_region") == 4 and names.count("clean_region") == 4
    assert names[-2:] == ["daily_summary", "publish_report"]


def test_missing_region_closes_partial():
    def extract(shard):
        if shard.endswith(".latam"):
            raise steps.SourceLate("latam hasn't delivered yet")
        return {"lines": 40}

    r = testing.run(daily_close, {"day": "2026-10-09"}, steps=scripted(extract_region=extract))
    assert r.status == "completed", r.message
    assert r.result["status"] == "partial" and r.result["missing"] == ["latam"]
    assert sum(c.name == "clean_region" for c in r.steps) == 3


def test_anomaly_waits_for_approval():
    flags = ["apac: revenue is 4.6× its 7-day mean"]
    r = testing.run(daily_close, {"day": "2026-10-17"}, steps=scripted(check_anomalies=flags),
                    signals={"approve": {"by": "ana"}})
    assert r.status == "completed" and r.result["approved_by"] == "ana"
    summary = next(c for c in r.steps if c.name == "daily_summary")
    assert summary.args["approved_by"] == "ana" and summary.args["flags"] == flags
    assert [p["region"] for p in summary.args["parts"]] == list(steps.REGIONS)


def test_anomaly_rejected_fails_the_run():
    r = testing.run(daily_close, {"day": "2026-10-17"}, steps=scripted(check_anomalies=["x"]),
                    signals={"approve": {"by": "ana", "reject": "duplicated feed"}})
    assert r.status == "failed" and r.reason == "Rejected"
    assert "daily_summary" not in [c.name for c in r.steps]


def test_anomaly_unanswered_is_held():
    r = testing.run(daily_close, {"day": "2026-10-17"}, steps=scripted(check_anomalies=["x"]))
    assert r.status == "completed" and r.result["status"] == "held"


def test_month_end_rolls_up_closed_days_without_sub_runs():
    meta = {"day": "2026-02-01", "orders": 1, "revenue_eur": "1", "margin_eur": "1", "partial": False,
            "flags": [], "regions": {}}
    r = testing.run(month_end, {"month": "2026-02"}, versions={"mart.shop_daily": mk.DataVersion(id="d", meta=meta)},
                    steps={"monthly_rollup": {"revenue_eur": "28", "partial_days": []}})
    assert r.status == "completed", r.message
    assert r.children == [] and r.result["closed"] == 28
    assert len(next(c for c in r.steps if c.name == "monthly_rollup").args["days"]) == 28


def test_month_end_closes_open_days_as_sub_runs_and_gives_up_after_too_many_failures():
    r = testing.run(month_end, {"month": "2026-10", "max_failed": 2},
                    children={"daily_close": RuntimeError("rejected")})
    assert r.status == "failed" and "3 days failed" in r.message
    assert [inputs for _, inputs in r.children] == [{"day": f"2026-10-0{i}", "auto_approve": True} for i in (1, 2, 3)]


def test_watch_returns_continues_as_new_with_its_cursor():
    batches = iter([[{"file": "a.json", "reason": "defective"}], [], [{"file": "b.json"}, {"file": "c.json"}]])
    r = testing.run(watch_returns, {"polls_per_run": 3},
                    steps={"poll_returns": lambda cursor: next(batches),
                           "record_returns": lambda returns, batch: {"recorded": len(returns)}})
    assert r.status == "continued"
    assert r.params == {"cursor": "c.json", "recorded": 3, "polls_per_run": 3}


def test_watch_returns_stops_on_signal():
    r = testing.run(watch_returns, {}, steps={"poll_returns": []}, signals={"stop": None})
    assert r.status == "completed" and r.result["stopped"]


# --- the steps themselves, on the real data -----------------------------------


def test_anomaly_check_flags_the_flash_sale():
    history = [{"regions": {"apac": {"revenue_eur": v}}} for v in ("2100", "2300", "1900", "2250", "2050")]
    flags = steps.check_anomalies([part("apac", revenue="9800.00")], history)
    assert len(flags) == 1 and flags[0].startswith("apac: revenue")
    assert steps.check_anomalies([part("apac", revenue="2200.00")], history) == []


def test_invalid_orders():
    products = {"MS-02": {}}
    rates = {"EUR": "1"}
    assert steps._invalid({"currency": "EUR", "lines": [{"sku": "MS-02", "qty": 1}]}, products, rates) is None
    assert steps._invalid({"lines": [{"sku": "MS-02", "qty": 1}]}, products, rates) == "unknown or missing currency"
    assert steps._invalid({"currency": "EUR", "lines": [{"sku": "ZZ-99", "qty": 1}]}, products, rates) == "unknown sku ZZ-99"
    assert steps._invalid({"currency": "EUR", "lines": [{"sku": "MS-02", "qty": 0}]}, products, rates) == "quantity not positive"


def test_source_data_has_its_surprises():
    orders = steps.DATA / "orders"
    assert not (orders / "latam" / "2026-10-09.jsonl").exists()
    n = lambda day: len((orders / "apac" / f"{day}.jsonl").read_text().splitlines())  # noqa: E731
    assert n("2026-10-17") > 4 * n("2026-10-16")
    lines = [json.loads(x) for p in orders.glob("*/*.jsonl") for x in p.read_text().splitlines()]
    assert any("currency" not in o for o in lines) and any(o["lines"][0]["sku"] == "ZZ-99" for o in lines)


if __name__ == "__main__":
    for name, f in list(globals().items()):
        if name.startswith("test_"):
            f()
            print("ok", name)
