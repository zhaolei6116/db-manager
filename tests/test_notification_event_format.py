import json
import unittest

from src.notifications.events import build_notification_event, build_event_id
from src.notifications.formatters import format_feishu_text, format_yunzhijia_text


class TestNotificationEventFormat(unittest.TestCase):
    def test_event_id_is_stable(self):
        event_id1 = build_event_id("NEW_SAMPLE", "P001", "S001")
        event_id2 = build_event_id("NEW_SAMPLE", "P001", "S001")
        self.assertEqual(event_id1, event_id2)
        self.assertEqual(event_id1, "P001|S001|NEW_SAMPLE")

    def test_feishu_text_contains_json(self):
        event = build_notification_event(
            event="NEW_SAMPLE",
            project_type="细菌完成图（标准分析）",
            project_id="P001",
            sample_id="S001",
            batch_id="B001",
            lab_sequencer_id="WSequenator01",
            barcode="barcode001",
        )

        text = format_feishu_text(event)
        self.assertIn("事件: NEW_SAMPLE", text)
        self.assertIn("项目类型: 细菌完成图（标准分析）", text)
        self.assertIn("项目编号: P001", text)
        self.assertIn("样本编号: S001", text)
        self.assertIn("JSON: ", text)

        json_part = text.split("JSON: ", 1)[1]
        parsed = json.loads(json_part)
        self.assertEqual(parsed["event"], "NEW_SAMPLE")
        self.assertEqual(parsed["project_id"], "P001")
        self.assertEqual(parsed["sample_id"], "S001")
        self.assertEqual(parsed["event_id"], "P001|S001|NEW_SAMPLE")

    def test_yunzhijia_text_contains_human_readable_fields(self):
        event = build_notification_event(
            event="READY_TO_RUN",
            project_type="细菌完成图（标准分析）",
            project_id="P001",
            sample_id="S001",
            raw_data_path="/nas04/sequencing/run1",
            analysis_dir="/nas02/project/bacass/P001",
        )

        text = format_yunzhijia_text(event)
        self.assertIn("事件: READY_TO_RUN", text)
        self.assertIn("项目编号: P001", text)
        self.assertIn("样本编号: S001", text)
        self.assertIn("下机路径: /nas04/sequencing/run1", text)
        self.assertIn("分析目录: /nas02/project/bacass/P001", text)
        self.assertNotIn("JSON: ", text)


if __name__ == "__main__":
    unittest.main()
