"""通知分发入口。"""

from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, Optional, Sequence

from src.notifications.events import NotificationEvent
from src.notifications.feishu_notifier import FeishuWebhookNotifier
from src.notifications.formatters import format_feishu_text, format_yunzhijia_text
from src.utils.notification_manager import notification_manager
from src.utils.yaml_config import YAMLConfig


logger = logging.getLogger(__name__)


class NotificationDispatcher:
    """统一通知分发层。"""

    def __init__(self, yaml_config: Optional[YAMLConfig] = None):
        self.yaml_config = yaml_config or YAMLConfig()
        self.feishu_notifier = FeishuWebhookNotifier()

    def dispatch(
        self,
        event: NotificationEvent,
        channels: Optional[Sequence[str]] = None,
    ) -> Dict[str, bool]:
        """按配置分发事件到不同渠道。"""
        targets = tuple(channels) if channels else ("yunzhijia", "feishu")
        result = {"yunzhijia": False, "feishu": False}

        if "yunzhijia" in targets and self._should_send("yunzhijia", event):
            try:
                content = format_yunzhijia_text(event)
                result["yunzhijia"] = notification_manager.send_yunzhijia_text(
                    content=content,
                    project_type=event.project_type,
                )
            except Exception as exc:
                logger.error("云之家通知分发失败: %s", str(exc), exc_info=True)

        if "feishu" in targets and self._should_send("feishu", event):
            try:
                content = format_feishu_text(event)
                timeout = self._get_channel_config("feishu").get("timeout", 10)
                webhook_url = self._get_channel_webhook_url("feishu", event.project_type)
                result["feishu"] = self.feishu_notifier.send_text(
                    webhook_url=webhook_url,
                    text=content,
                    timeout=timeout,
                )
            except Exception as exc:
                logger.error("飞书通知分发失败: %s", str(exc), exc_info=True)

        return result

    def _should_send(self, channel: str, event: NotificationEvent) -> bool:
        channel_config = self._get_channel_config(channel)
        enabled = channel_config.get("enabled")
        if enabled is False:
            return False

        allowed_events = channel_config.get("send_event_types", [])
        if allowed_events and event.event not in allowed_events:
            return False

        project_type_filters = channel_config.get("project_type_filters", [])
        if project_type_filters and event.project_type not in project_type_filters:
            return False

        if channel == "feishu":
            webhook_url = self._get_channel_webhook_url(channel, event.project_type)
            return bool(webhook_url)

        if channel == "yunzhijia":
            return bool(self._get_channel_webhook_url(channel, event.project_type))

        return False

    def _get_channel_config(self, channel: str) -> Dict:
        notification_config = self.yaml_config.get("notification", default={})
        return notification_config.get(channel, {})

    def _get_channel_webhook_url(self, channel: str, project_type: str) -> str:
        channel_config = self._get_channel_config(channel)

        if channel == "feishu":
            env_webhook_url = os.environ.get("FEISHU_WEBHOOK_URL", "").strip()
            if env_webhook_url:
                return env_webhook_url

            project_webhooks = channel_config.get("webhooks", {})
            if project_type in project_webhooks:
                return project_webhooks[project_type]
            return channel_config.get("webhook_url", "")

        if channel == "yunzhijia":
            project_webhooks = channel_config.get("webhooks", {})
            if project_type in project_webhooks:
                return project_webhooks[project_type]
            legacy_webhooks = self.yaml_config.get("notification.webhooks", default={})
            if project_type in legacy_webhooks:
                return legacy_webhooks[project_type]
            return channel_config.get("webhook_url", "")

        return ""


notification_dispatcher = NotificationDispatcher()
