"""Runs the three lab scenarios: replication, stale read, recovery/convergence."""

import sys

from replication import Client, Primary, Replica, Replicator, log


def banner(title):
    print(f"\n{'=' * 64}\n{title}\n{'=' * 64}", flush=True)


def check(condition, message):
    log("CHECK", ("PASS: " if condition else "FAIL: ") + message)
    return condition


def main():
    # Both replicas start empty (stock=0, v0).
    primary = Primary(stock=0)
    secondary = Replica("SECONDARY", stock=0)
    replicator = Replicator(primary, secondary, delay=0.3)
    client = Client(primary, secondary)
    replicator.start()
    ok = True

    # ---- Scenario 1: normal replication -------------------------------------
    banner("SCENARIO 1: Replication (Primary -> Secondary)")
    primary.write(10)
    replicator.wait_until_synced()
    p_stock, _ = client.read(primary)
    s_stock, _ = client.read(secondary)
    ok &= check(p_stock == s_stock == 10, "both replicas return stock=10 after replication")

    # ---- Scenario 2: stale read ---------------------------------------------
    banner("SCENARIO 2: Stale read (replication delayed)")
    replicator.pause()
    primary.write(9)
    p_stock, _ = client.read(primary)
    s_stock, _ = client.read(secondary)
    ok &= check(p_stock == 9, "Primary returns the new value 9")
    ok &= check(s_stock == 10, "Secondary still returns the old value 10 (stale read)")

    # ---- Scenario 3: recovery and convergence -------------------------------
    banner("SCENARIO 3: Recovery and convergence (replication resumed)")
    replicator.resume()
    synced = replicator.wait_until_synced()
    p_stock, _ = client.read(primary)
    s_stock, _ = client.read(secondary)
    ok &= check(synced and s_stock == 9, "Secondary caught up to 9")
    ok &= check(p_stock == s_stock == 9, "CONVERGED: both replicas return stock=9")

    replicator.stop()
    print()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
