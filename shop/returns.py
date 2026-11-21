"""Watch the returns inbox: a run that runs for as long as you let it.

    moksha start watch_returns
    echo '{"order_id": 50012, "sku": "MS-02", "qty": 1, "reason": "defective"}' \
      > data/shop/returns/inbox/2026-10-21-0001.json
    moksha signal <run> stop

Every 15 seconds it polls the inbox (poll_returns) and records what's new
(record_returns), one data version per batch. Between polls it waits for
a "stop" signal with a timeout: an interruptible sleep, held by the
server, so a restart doesn't lose its place.

A run that loops forever would keep a history that grows forever, and
replay all of it on each step. After `polls_per_run` polls it ends with
ctx.continue_as_new: the same pipeline starts again, fresh, from the
cursor (the last file it saw). The web UI links the runs of the chain.
"""

from datetime import timedelta

import moksha as mk

from .steps import poll_returns, record_returns

POLL_EVERY = timedelta(seconds=15)


@mk.pipeline(title="Watch returns")
def watch_returns(ctx, cursor: str = "", recorded: int = 0, polls_per_run: int = 20):
    """Polls the returns inbox and records each batch, until it gets a stop signal."""
    for _ in range(polls_per_run):
        batch = ctx.step(poll_returns, cursor)
        if batch:
            r = ctx.step(record_returns, batch, batch[0]["file"])
            recorded += r.result["recorded"]
            cursor = batch[-1]["file"]
        try:
            ctx.wait_signal("stop", timeout=POLL_EVERY)
            return {"stopped": True, "cursor": cursor, "recorded": recorded}
        except mk.SignalTimeout:
            pass
    ctx.continue_as_new(cursor=cursor, recorded=recorded, polls_per_run=polls_per_run)
