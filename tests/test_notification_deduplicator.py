import os
import unittest
from tempfile import TemporaryDirectory


class TestNotificationDeduplicator(unittest.TestCase):
    def test_timeout_dedup_persists(self):
        with TemporaryDirectory() as tmpdir:
            os.environ["NOTIFICATION_DEDUP_DIR"] = tmpdir
            from src.utils.notification_deduplicator import NotificationDeduplicator

            d1 = NotificationDeduplicator()
            self.assertFalse(d1.has_timeout_sent("Seq_1"))
            d1.mark_timeout_sent("Seq_1", ts="2026-07-09T00:00:00+08:00")
            self.assertTrue(d1.has_timeout_sent("Seq_1"))

            d2 = NotificationDeduplicator()
            self.assertTrue(d2.has_timeout_sent("Seq_1"))

    def test_recovered_channel_state(self):
        with TemporaryDirectory() as tmpdir:
            os.environ["NOTIFICATION_DEDUP_DIR"] = tmpdir
            from src.utils.notification_deduplicator import NotificationDeduplicator

            d = NotificationDeduplicator()
            d.mark_timeout_sent("Seq_2", ts="2026-07-09T00:00:00+08:00")
            self.assertFalse(d.is_recovered_complete("Seq_2"))

            d.mark_recovered_channel_success("Seq_2", "yunzhijia", ts="2026-07-09T00:01:00+08:00")
            self.assertFalse(d.is_recovered_complete("Seq_2"))
            self.assertEqual(d.get_recovered_success_channels("Seq_2"), {"yunzhijia"})

            d.mark_recovered_channel_success("Seq_2", "feishu", ts="2026-07-09T00:02:00+08:00")
            self.assertTrue(d.is_recovered_complete("Seq_2"))


if __name__ == "__main__":
    unittest.main()

