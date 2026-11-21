"""The shop's steps. Each is plain Python: callable as is in tests.

Which steps say what about reuse, and why:

- The extracts (load_catalog, extract_region, fx_rates, poll_returns) read
  sources moksha can't fingerprint (files, an API), so they run every time:
  reuse=False, or reuse="6h" for the FX API, whose past rates don't move.
- Everything after them reads only its inputs and arguments: reuse=True.
  When an extract comes out byte-identical, all of it is reused.
"""

import csv
import html
import json
import logging
import os
import random
import statistics
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

import moksha as mk

DATA = Path(__file__).resolve().parent.parent / "data" / "shop"
REGIONS = ("eu", "us", "apac", "latam")
CENTS = Decimal("0.01")
log = logging.getLogger(__name__)


def shard(day: str, region: str) -> str:
    """One region's slice of one day, as a partition: "2026-10-17.apac"."""
    return f"{day}.{region}"


def split(shard_: str) -> tuple[str, str]:
    day, _, region = shard_.rpartition(".")
    return day, region


# --- extracts: they read the outside world -----------------------------------


@mk.step(reuse=False, output=mk.Dataset("ref.products", keep=mk.Keep(versions=5)))
def load_catalog() -> dict:
    """The product catalog (sku, name, category, cost)."""
    src = DATA / "products.csv"
    text = src.read_text()
    (mk.output().path / "products.csv").write_text(text)
    n = len(text.splitlines()) - 1
    print(f"{n} products")
    return {"products": n}


class SourceLate(Exception):
    """A region's file isn't there (yet): worth retrying."""


@mk.step(reuse=False, retry=mk.Retry(limit=3, backoff="exponential", initial=2, max=10),
         output=mk.Dataset("raw.shop_orders", partition="shard", keep=mk.Keep(versions=3)))
def extract_region(shard: str) -> dict:
    """One region's orders for one day, as delivered (not checked yet)."""
    day, region = split(shard)
    src = DATA / "orders" / region / f"{day}.jsonl"
    if not src.exists():
        # Retried (Retry above): a late file often arrives within minutes.
        raise SourceLate(f"{region} hasn't delivered {src.name} yet")
    text = src.read_text()
    (mk.output().path / "orders.jsonl").write_text(text)
    n = text.count("\n")
    mk.output().stats["lines"] = n
    print(f"{region}: {n} orders for {day}")
    return {"region": region, "lines": n}


@mk.step(reuse="6h", retry=mk.Retry(limit=6, backoff="exponential", initial=0.5, max=8), timeout=30,
         output=mk.Dataset("ref.fx", partition="day"))
def fx_rates(day: str) -> dict:
    """The day's exchange rates, from a (simulated, flaky) rates API.

    It fails about one call in three (FX_FAILURE_RATE): watch the
    attempts and their backoff on the step's page in the web UI.
    """
    if random.random() < float(os.environ.get("FX_FAILURE_RATE", "0.3")):
        raise ConnectionError("rates API: 503 Service Unavailable")
    rates = {"EUR": "1"}
    with (DATA / "fx.csv").open() as f:
        for r in csv.DictReader(f):
            if r["day"] == day:
                rates[r["currency"]] = r["per_eur"]
    if len(rates) == 1:
        # A day the API has no rates for will never have them: don't retry.
        raise mk.NonRetryable(f"rates API has no rates for {day}")
    (mk.output().path / "rates.json").write_text(json.dumps(rates, sort_keys=True, indent=1))
    print("1 EUR = " + ", ".join(f"{v} {k}" for k, v in sorted(rates.items()) if k != "EUR"))
    return rates


# --- transforms: reused while their inputs don't change ----------------------


@mk.step(reuse=True, queue="compute",
         output=mk.Dataset("staging.shop_orders", partition="shard", keep=mk.Keep(versions=3)))
def clean_region(orders: mk.DataVersion, fx: mk.DataVersion, catalog: mk.DataVersion, shard: str) -> dict:
    """Checks each order, converts it to EUR and prices its cost.

    Writes the good lines to lines.csv and the bad orders, with why, to
    rejects.jsonl; returns the region's totals (small: they go in the
    run's history and on to daily_summary).
    """
    day, region = split(shard)
    rates = json.loads(fx.path("rates.json").read_text())
    with catalog.path("products.csv").open() as f:
        products = {r["sku"]: r for r in csv.DictReader(f)}

    out = mk.output()
    good, rejects = [], []
    for raw in orders.path("orders.jsonl").read_text().splitlines():
        o = json.loads(raw)
        why = _invalid(o, products, rates)
        if why:
            rejects.append({"order_id": o.get("order_id"), "why": why, "order": o})
            continue
        per_eur = Decimal(rates[o["currency"]])
        for line in o["lines"]:
            p = products[line["sku"]]
            revenue = (Decimal(str(line["unit_price"])) * line["qty"] / per_eur).quantize(CENTS)
            cost = (Decimal(p["unit_cost_eur"]) * line["qty"]).quantize(CENTS)
            good.append({"order_id": o["order_id"], "customer": o["customer"], "sku": line["sku"],
                         "category": p["category"], "qty": line["qty"], "revenue_eur": revenue, "cost_eur": cost})

    with (out.path / "lines.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["order_id", "customer", "sku", "category", "qty", "revenue_eur", "cost_eur"])
        w.writeheader()
        w.writerows(good)
    (out.path / "rejects.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rejects))

    revenue = sum((g["revenue_eur"] for g in good), Decimal(0))
    cost = sum((g["cost_eur"] for g in good), Decimal(0))
    by_category: dict[str, Decimal] = defaultdict(Decimal)
    by_customer: dict[str, Decimal] = defaultdict(Decimal)
    for g in good:
        by_category[g["category"]] += g["revenue_eur"]
        by_customer[g["customer"]] += g["revenue_eur"]
    orders_ok = len({g["order_id"] for g in good})
    out.stats.update(lines=len(good), rejected=len(rejects))
    if rejects:
        reasons = Counter(r["why"] for r in rejects)
        log.warning("%s: rejected %d of %d orders: %s", shard, len(rejects), orders_ok + len(rejects),
                    ", ".join(f"{n}× {why}" for why, n in reasons.most_common()))
    print(f"{region}: {orders_ok} orders, €{revenue:,} revenue, €{revenue - cost:,} margin")
    return {
        "region": region,
        "orders": orders_ok,
        "rejected": len(rejects),
        "revenue_eur": str(revenue),
        "cost_eur": str(cost),
        "customers": len(by_customer),
        "by_category": {k: str(v) for k, v in sorted(by_category.items())},
        "top_customer": max(by_customer, key=by_customer.__getitem__) if by_customer else None,
    }


def _invalid(o: dict, products: dict, rates: dict) -> str | None:
    if o.get("currency") not in rates:
        return "unknown or missing currency"
    if not o.get("lines"):
        return "no lines"
    for line in o["lines"]:
        if line.get("sku") not in products:
            return f"unknown sku {line.get('sku')}"
        if not isinstance(line.get("qty"), int) or line["qty"] <= 0:
            return "quantity not positive"
    return None


@mk.step(reuse=True)
def check_anomalies(parts: list[dict], history: list[dict]) -> list[str]:
    """What looks off today, per region, against the days before
    (history: their daily summaries' meta, oldest first). An empty list is
    a clean day."""
    flags = []
    for p in parts:
        r = p["region"]
        past = [Decimal(h["regions"][r]["revenue_eur"]) for h in history if r in h.get("regions", {})]
        today = Decimal(p["revenue_eur"])
        if len(past) >= 3:
            mean = statistics.fmean(float(x) for x in past)
            sd = statistics.pstdev(float(x) for x in past) or 1.0
            z = (float(today) - mean) / sd
            if abs(z) > 4 and abs(float(today) - mean) > 0.5 * mean:
                flags.append(f"{r}: revenue €{today:,} is {float(today) / mean:.1f}× its {len(past)}-day mean "
                             f"(€{mean:,.2f})")
        total = p["orders"] + p["rejected"]
        if total and p["rejected"] / total > 0.10:
            flags.append(f"{r}: {p['rejected']} of {total} orders rejected")
    for f in flags:
        log.warning("anomaly: %s", f)
    if not flags:
        print(f"no anomalies against {len(history)} earlier days")
    return flags


@mk.step(reuse=True, output=mk.Dataset("mart.shop_daily", partition="day"))
def daily_summary(parts: list[dict], day: str, missing: list[str], flags: list[str], approved_by: str) -> dict:
    """The day's books: per region and category, in EUR. Partial when a
    region is missing. The totals go in the version's meta too, where
    later runs read them without opening the data (ctx.latest_version)."""
    revenue = sum((Decimal(p["revenue_eur"]) for p in parts), Decimal(0))
    cost = sum((Decimal(p["cost_eur"]) for p in parts), Decimal(0))
    categories: dict[str, Decimal] = defaultdict(Decimal)
    for p in parts:
        for c, v in p["by_category"].items():
            categories[c] += Decimal(v)
    summary = {
        "day": day,
        "partial": bool(missing),
        "missing": missing,
        "flags": flags,
        "approved_by": approved_by,
        "orders": sum(p["orders"] for p in parts),
        "rejected": sum(p["rejected"] for p in parts),
        "revenue_eur": str(revenue),
        "margin_eur": str(revenue - cost),
        "regions": {p["region"]: {k: p[k] for k in ("orders", "revenue_eur", "cost_eur", "customers")} for p in parts},
        "categories": {c: str(v) for c, v in sorted(categories.items())},
    }
    out = mk.output()
    (out.path / "summary.json").write_text(json.dumps(summary, indent=1))
    out.meta.update(summary)
    return {"revenue_eur": summary["revenue_eur"], "margin_eur": summary["margin_eur"], "partial": summary["partial"]}


@mk.step(reuse=True, output=mk.Dataset("reports.shop_daily", partition="day"))
def publish_report(summary: mk.DataVersion, day: str) -> str:
    """The day's report, as Markdown."""
    s = json.loads(summary.path("summary.json").read_text())
    rev = Decimal(s["revenue_eur"])
    lines = [f"# Shop, {day}", ""]
    if s["partial"]:
        lines += [f"> **Partial**: no data from {', '.join(s['missing'])}.", ""]
    if s["flags"]:
        lines += [f"> Approved by {s['approved_by']} despite:"] + [f"> - {f}" for f in s["flags"]] + [""]
    lines += [f"{s['orders']} orders, €{rev:,} revenue, €{Decimal(s['margin_eur']):,} margin "
              f"({Decimal(s['margin_eur']) / rev * 100 if rev else 0:.1f}%); "
              f"{s['rejected']} order{'' if s['rejected'] == 1 else 's'} rejected.", "",
              "| Region | Orders | Revenue | Customers |", "|---|---:|---:|---:|"]
    for r, v in sorted(s["regions"].items(), key=lambda kv: -Decimal(kv[1]["revenue_eur"])):
        lines.append(f"| {r} | {v['orders']} | €{Decimal(v['revenue_eur']):,} | {v['customers']} |")
    lines += ["", "| Category | Revenue |", "|---|---:|"]
    lines += [f"| {c} | €{Decimal(v):,} |" for c, v in sorted(s["categories"].items(), key=lambda kv: -Decimal(kv[1]))]
    text = "\n".join(lines) + "\n"
    (mk.output().path / "report.md").write_text(text)
    print(text, end="")
    return f"€{rev:,}"


@mk.step(reuse=True, output=mk.Dataset("reports.shop_monthly", partition="month"))
def monthly_rollup(days: list[dict], month: str) -> dict:
    """The month from its days' summaries: a CSV of days and a short Markdown report."""
    days = sorted(days, key=lambda d: d["day"])
    out = mk.output()
    with (out.path / "days.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["day", "orders", "revenue_eur", "margin_eur", "partial", "flags"])
        for d in days:
            w.writerow([d["day"], d["orders"], d["revenue_eur"], d["margin_eur"], d["partial"], len(d["flags"])])
    revenue = sum((Decimal(d["revenue_eur"]) for d in days), Decimal(0))
    margin = sum((Decimal(d["margin_eur"]) for d in days), Decimal(0))
    regions: dict[str, Decimal] = defaultdict(Decimal)
    for d in days:
        for r, v in d["regions"].items():
            regions[r] += Decimal(v["revenue_eur"])
    best = max(days, key=lambda d: Decimal(d["revenue_eur"]))
    partial = [d["day"] for d in days if d["partial"]]
    lines = [f"# Shop, {month}", "",
             f"{len(days)} days, {sum(d['orders'] for d in days):,} orders, €{revenue:,} revenue, €{margin:,} margin.",
             f"Best day: {best['day']} (€{Decimal(best['revenue_eur']):,}).",
             f"Partial days: {', '.join(partial) or 'none'}.", "", "| Region | Revenue | Share |", "|---|---:|---:|"]
    lines += [f"| {r} | €{v:,} | {v / revenue * 100:.1f}% |" for r, v in sorted(regions.items(), key=lambda kv: -kv[1])]
    text = "\n".join(lines) + "\n"
    (out.path / "report.md").write_text(text)
    print(text, end="")
    out.meta.update(revenue_eur=str(revenue), days=len(days))
    return {"days": len(days), "revenue_eur": str(revenue), "partial_days": partial}


@mk.step(reuse=True, output=mk.Dataset("reports.shop_dashboard", keep=mk.Keep(versions=10)))
def render_dashboard(days: list[dict]) -> str:
    """A small HTML page: revenue per day, stacked by region, an SVG chart."""
    days = sorted(days, key=lambda d: d["day"])
    colors = {"eu": "#4C78A8", "us": "#F58518", "apac": "#54A24B", "latam": "#B279A2"}
    top = max((float(d["revenue_eur"]) for d in days), default=1.0) or 1.0
    w, h, bar = 40 * len(days) + 60, 260, 28
    svg = [f'<svg width="{w}" height="{h + 40}" font-family="sans-serif" font-size="10">']
    for i, d in enumerate(days):
        x, y = 50 + 40 * i, h
        for r in REGIONS:
            v = float(d["regions"].get(r, {}).get("revenue_eur", 0))
            bh = v / top * (h - 20)
            y -= bh
            svg.append(f'<rect x="{x}" y="{y:.1f}" width="{bar}" height="{bh:.1f}" fill="{colors[r]}">'
                       f'<title>{r} {d["day"]}: €{v:,.2f}</title></rect>')
        mark = "*" if d["partial"] else ("!" if d["flags"] else "")
        svg.append(f'<text x="{x + bar / 2}" y="{h + 14}" text-anchor="middle">{d["day"][5:]}{mark}</text>')
    svg.append("</svg>")
    legend = " ".join(f'<span style="color:{c}">■</span> {r}' for r, c in colors.items())
    page = (f"<!doctype html><meta charset=utf-8><title>Shop dashboard</title>"
            f"<h1>Revenue per day (EUR)</h1><p>{legend} · * partial · ! approved despite anomalies</p>"
            + "".join(svg) + "<table border=1 cellpadding=4><tr><th>Day<th>Orders<th>Revenue<th>Margin</tr>"
            + "".join(f"<tr><td>{html.escape(d['day'])}<td>{d['orders']}<td>€{Decimal(d['revenue_eur']):,}"
                      f"<td>€{Decimal(d['margin_eur']):,}</tr>" for d in days) + "</table>")
    (mk.output().path / "dashboard.html").write_text(page)
    print(f"dashboard of {len(days)} days, {days[0]['day']} to {days[-1]['day']}" if days else "empty dashboard")
    return days[-1]["day"] if days else ""


# --- returns: polled from an inbox -------------------------------------------

INBOX = DATA / "returns" / "inbox"


@mk.step(reuse=False)
def poll_returns(cursor: str) -> list[dict]:
    """Returns that landed in the inbox after cursor (file names sort by
    arrival: name them <day>-<n>.json), oldest first, at most 50."""
    names = sorted(p.name for p in INBOX.glob("*.json") if p.name > cursor)[:50]
    found = []
    for n in names:
        try:
            found.append({"file": n, **json.loads((INBOX / n).read_text())})
        except json.JSONDecodeError as e:
            found.append({"file": n, "error": str(e)})
    if found:
        print(f"{len(found)} new: {names[0]} … {names[-1]}")
    return found


@mk.step(reuse=True, output=mk.Dataset("staging.shop_returns", partition="batch", keep=mk.Keep(versions=1)))
def record_returns(returns: list[dict], batch: str) -> dict:
    """A batch of returns as returns.csv; unreadable ones logged and left out."""
    ok = [r for r in returns if "error" not in r]
    for r in returns:
        if "error" in r:
            log.error("%s: not JSON (%s), skipped", r["file"], r["error"])
    with (mk.output().path / "returns.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["file", "order_id", "sku", "qty", "reason"], extrasaction="ignore")
        w.writeheader()
        w.writerows(ok)
    reasons = Counter(r.get("reason", "?") for r in ok)
    print(", ".join(f"{n}× {why}" for why, n in reasons.most_common()) or "nothing readable")
    return {"recorded": len(ok), "skipped": len(returns) - len(ok), "reasons": dict(reasons)}
