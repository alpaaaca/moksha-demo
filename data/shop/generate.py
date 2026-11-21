"""Writes the shop's source data: python3 data/shop/generate.py

Deterministic (seeded), so the files in git are what it writes. October
2026, four regions, each delivering one file of orders per day:

    orders/<region>/<day>.jsonl   one order per line: customer, currency, lines
    products.csv                  the catalog: sku, name, category, unit cost (EUR)
    fx.csv                        a rate per day and currency (1 EUR in it)
    returns/inbox/                where returns land (watch_returns picks them up)

Things built in, for the pipelines to find:

- 2026-10-17: a flash sale in apac, about five times the usual orders
  (daily_close's anomaly gate holds it for approval).
- latam's file for 2026-10-09 never arrived (its extract fails after its
  retries; the day closes without latam, marked partial).
- A few bad lines here and there: an unknown sku, a zero quantity, a
  missing currency (quarantined, not counted).
"""

import csv
import datetime as dt
import json
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent
REGIONS = {"eu": ("EUR", 34), "us": ("USD", 28), "apac": ("JPY", 22), "latam": ("BRL", 12)}
PRODUCTS = [
    ("KB-01", "Mechanical keyboard", "peripherals", "41.00"),
    ("MS-02", "Wireless mouse", "peripherals", "9.50"),
    ("MN-27", "27\" monitor", "displays", "142.00"),
    ("MN-32", "32\" monitor", "displays", "238.00"),
    ("HD-01", "USB-C hub", "accessories", "12.25"),
    ("CB-10", "Cable kit", "accessories", "3.10"),
    ("HP-05", "Headphones", "audio", "38.00"),
    ("SP-02", "Desk speakers", "audio", "29.90"),
    ("LT-14", "14\" laptop", "computers", "612.00"),
    ("DK-01", "Docking station", "accessories", "71.00"),
]
LIST_PRICE_EUR = {"KB-01": 89, "MS-02": 24, "MN-27": 279, "MN-32": 449, "HD-01": 35,
                  "CB-10": 12, "HP-05": 99, "SP-02": 69, "LT-14": 1099, "DK-01": 159}
FX_BASE = {"EUR": 1.0, "USD": 1.08, "JPY": 162.0, "BRL": 5.9}
DAYS = [dt.date(2026, 10, 1) + dt.timedelta(days=i) for i in range(31)]


def main() -> None:
    rnd = random.Random(2026)
    with (HERE / "products.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sku", "name", "category", "unit_cost_eur"])
        w.writerows(PRODUCTS)

    with (HERE / "fx.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["day", "currency", "per_eur"])
        drift = dict(FX_BASE)
        for day in DAYS:
            for cur in sorted(FX_BASE):
                if cur != "EUR":
                    drift[cur] *= 1 + rnd.uniform(-0.004, 0.004)
                w.writerow([day.isoformat(), cur, f"{drift[cur]:.4f}"])

    order_id = 50_000
    for region, (currency, per_day) in REGIONS.items():
        d = HERE / "orders" / region
        d.mkdir(parents=True, exist_ok=True)
        for day in DAYS:
            if region == "latam" and day == dt.date(2026, 10, 9):
                continue  # never delivered
            n = per_day + rnd.randint(-6, 6) + (6 if day.weekday() >= 5 else 0)
            if region == "apac" and day == dt.date(2026, 10, 17):
                n *= 5  # the flash sale
            lines_out = []
            for _ in range(n):
                order_id += 1
                lines = []
                for sku in rnd.sample(list(LIST_PRICE_EUR), k=rnd.choice([1, 1, 1, 2, 2, 3])):
                    price = LIST_PRICE_EUR[sku] * FX_BASE[currency] * rnd.uniform(0.9, 1.05)
                    if region == "apac" and day == dt.date(2026, 10, 17):
                        price *= 0.8  # 20% off
                    lines.append({"sku": sku, "qty": rnd.choice([1, 1, 1, 1, 2, 3]),
                                  "unit_price": round(price, 0 if currency == "JPY" else 2)})
                order = {"order_id": order_id, "customer": f"c{rnd.randint(1, 900):04d}",
                         "currency": currency, "lines": lines}
                bad = rnd.random()
                if bad < 0.003:
                    order["lines"][0]["sku"] = "ZZ-99"
                elif bad < 0.006:
                    order["lines"][0]["qty"] = 0
                elif bad < 0.008:
                    del order["currency"]
                lines_out.append(json.dumps(order, separators=(",", ":")))
            (d / f"{day.isoformat()}.jsonl").write_text("\n".join(lines_out) + "\n")

    inbox = HERE / "returns" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    (inbox / ".gitkeep").touch()


if __name__ == "__main__":
    main()
