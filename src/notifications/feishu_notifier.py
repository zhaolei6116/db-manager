"""飞书 webhook 发送器。"""

from __future__ import annotations

import logging
from typing import Optional

import requests


logger = logging.getLogger(__name__)


class FeishuWebhookNotifier:
    """飞书机器人 webhook 发送器。"""

    def send_text(self, webhook_url: str, text: str, timeout: int = 10) -> bool:
        """发送飞书文本消息。"""
        if not webhook_url:
            logger.warning("飞书 webhook_url 未配置，跳过发送")
            return False

        payload = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }

        try:
            response = requests.post(webhook_url, json=payload, timeout=timeout)
            if response.status_code == 200:
                logger.info("飞书通知发送成功")
                return True

            logger.error(
                "飞书通知发送失败，status_code=%s, response=%s",
                response.status_code,
                response.text[:500],
            )
            return False
        except Exception as exc:
            logger.error("飞书通知发送异常: %s", str(exc), exc_info=True)
            return False

