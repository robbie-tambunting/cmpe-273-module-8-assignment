"""Automated checks for the three lab requirements. Run: python3 -m unittest -v"""

import unittest

from replication import Client, Primary, Replica, Replicator


class ReplicationTests(unittest.TestCase):
    def setUp(self):
        # Short delay keeps the tests fast; both replicas start at stock=10.
        self.primary = Primary(stock=10)
        self.secondary = Replica("SECONDARY", stock=10)
        self.replicator = Replicator(self.primary, self.secondary, delay=0.05)
        self.client = Client(self.primary, self.secondary)
        self.replicator.start()

    def tearDown(self):
        self.replicator.stop()

    def test_replication_propagates_to_secondary(self):
        """Requirement 1: Primary gets the value, Secondary eventually matches."""
        self.primary.write(9)
        self.assertEqual(self.primary.read()[0], 9)
        self.assertTrue(self.replicator.wait_until_synced())
        self.assertEqual(self.secondary.read(), self.primary.read())

    def test_stale_read_while_replication_paused(self):
        """Requirement 2: Primary=9 while Secondary still returns old 10."""
        self.replicator.pause()
        self.primary.write(9)
        p_stock, p_version = self.client.read(self.primary)
        s_stock, s_version = self.client.read(self.secondary)
        self.assertEqual(p_stock, 9)
        self.assertEqual(s_stock, 10)  # stale read
        self.assertLess(s_version, p_version)

    def test_convergence_after_resume(self):
        """Requirement 3: after resume, Secondary catches up and matches."""
        self.replicator.pause()
        self.primary.write(9)
        self.assertEqual(self.secondary.read()[0], 10)
        self.replicator.resume()
        self.assertTrue(self.replicator.wait_until_synced())
        self.assertEqual(self.secondary.read()[0], 9)
        self.assertEqual(self.secondary.read(), self.primary.read())

    def test_multiple_writes_while_paused_apply_in_order(self):
        """Edge case: queued writes replay in order; final value wins."""
        self.replicator.pause()
        self.primary.write(9)
        self.primary.write(8)
        self.primary.write(7)
        self.assertEqual(self.secondary.read()[0], 10)
        self.replicator.resume()
        self.assertTrue(self.replicator.wait_until_synced())
        self.assertEqual(self.secondary.read(), (7, 3))
        self.assertEqual(self.secondary.read(), self.primary.read())

    def test_secondary_ignores_older_versions(self):
        """Edge case: an out-of-order/old update never overwrites newer data."""
        self.assertTrue(self.secondary.apply(5, 2))
        self.assertFalse(self.secondary.apply(99, 1))
        self.assertEqual(self.secondary.read(), (5, 2))


if __name__ == "__main__":
    unittest.main()
