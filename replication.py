"""Simple replicated inventory: 1 Primary, 1 Secondary, asynchronous replication.

Writes go to the Primary, which queues them for a Replicator thread that
applies them to the Secondary. The Replicator can be paused/resumed to
simulate delayed replication (and therefore stale reads on the Secondary).
"""

import queue
import threading
import time

_START = time.monotonic()
_log_lock = threading.Lock()


def log(tag, message):
    """Print a timestamped, tagged log line (thread-safe)."""
    with _log_lock:
        print(f"[{time.monotonic() - _START:7.3f}s] [{tag:<10}] {message}", flush=True)


class Replica:
    """A single copy of the inventory value, with a version number."""

    def __init__(self, name, stock=0):
        self.name = name
        self._stock = stock
        self._version = 0
        self._lock = threading.Lock()

    def read(self):
        """Return (stock, version) atomically."""
        with self._lock:
            return self._stock, self._version

    def apply(self, stock, version):
        """Apply an update unless it is older than what we already have.

        Returns True if the update was applied.
        """
        with self._lock:
            if version <= self._version:
                return False
            self._stock = stock
            self._version = version
            return True


class Primary(Replica):
    """Accepts writes and hands them to the replication queue."""

    def __init__(self, stock=0):
        super().__init__("PRIMARY", stock)
        self.replication_queue = queue.Queue()

    def write(self, stock):
        with self._lock:
            self._stock = stock
            self._version += 1
            version = self._version
        log(self.name, f"write stock={stock} (v{version})")
        self.replication_queue.put((stock, version))
        return version


class Replicator(threading.Thread):
    """Background thread that ships queued updates from Primary to Secondary.

    pause() holds updates back (they stay queued, nothing is lost);
    resume() lets them flow again so the Secondary catches up in order.
    """

    def __init__(self, primary, secondary, delay=0.2):
        super().__init__(name="replicator", daemon=True)
        self.primary = primary
        self.secondary = secondary
        self.delay = delay
        self._running = threading.Event()
        self._running.set()
        self._stopped = threading.Event()

    def pause(self):
        self._running.clear()
        log("REPLICATOR", "replication PAUSED (updates will queue up)")

    def resume(self):
        log("REPLICATOR", "replication RESUMED")
        self._running.set()

    def wait_until_synced(self, timeout=5.0):
        """Block until every queued update has been applied. True on success."""
        q = self.primary.replication_queue
        done = threading.Event()

        def _join():
            q.join()
            done.set()

        threading.Thread(target=_join, daemon=True).start()
        return done.wait(timeout)

    def stop(self):
        self._stopped.set()
        self._running.set()  # unblock if paused
        self.primary.replication_queue.put(None)  # wake the queue.get()

    def run(self):
        q = self.primary.replication_queue
        while not self._stopped.is_set():
            item = q.get()
            if item is None:
                q.task_done()
                break
            stock, version = item
            self._running.wait()  # blocks while paused
            time.sleep(self.delay)  # simulated network latency
            applied = self.secondary.apply(stock, version)
            if applied:
                log(self.secondary.name, f"apply stock={stock} (v{version})")
            q.task_done()


class Client:
    """Reads inventory from either replica and flags stale results."""

    def __init__(self, primary, secondary):
        self.primary = primary
        self.secondary = secondary

    def read(self, replica):
        stock, version = replica.read()
        _, latest = self.primary.read()
        stale = version < latest
        note = f"  <-- STALE (latest is v{latest})" if stale else ""
        log("CLIENT", f"read {replica.name.lower():<9} -> stock={stock} (v{version}){note}")
        return stock, version
