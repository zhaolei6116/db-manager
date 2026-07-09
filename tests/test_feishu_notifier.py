import unittest
from unittest.mock import Mock, patch

from src.notifications.feishu_notifier import FeishuWebhookNotifier


class TestFeishuWebhookNotifier(unittest.TestCase):
    def setUp(self):
        self.notifier = FeishuWebhookNotifier()

    @patch("src.notifications.feishu_notifier.requests.post")
    def test_send_text_success(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_post.return_value = mock_response

        result = self.notifier.send_text(
            webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
            text="EVENT: NEW_SAMPLE\nJSON: {}",
            timeout=5,
        )

        self.assertTrue(result)
        mock_post.assert_called_once()

    @patch("src.notifications.feishu_notifier.requests.post")
    def test_send_text_exception_returns_false(self, mock_post):
        mock_post.side_effect = Exception("network error")

        result = self.notifier.send_text(
            webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
            text="EVENT: NEW_SAMPLE\nJSON: {}",
            timeout=5,
        )

        self.assertFalse(result)

    def test_send_text_without_webhook_returns_false(self):
        result = self.notifier.send_text(webhook_url="", text="EVENT: NEW_SAMPLE")
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()

