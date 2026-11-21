"""The shop: a bigger moksha example than revenue/.

    steps.py      the steps: extract, clean, check, summarize, render
    close.py      daily_close: one day's books, four regions in parallel,
                  an anomaly gate that waits for a person's approval
    month.py      month_end: closes the days still open (sub-runs), then rolls up
    dashboard.py  refresh_dashboard: starts by itself when a day is closed
    returns.py    watch_returns: a long-running poll of an inbox, continue_as_new

The source data is in data/shop/ (data/shop/generate.py writes it).
"""
