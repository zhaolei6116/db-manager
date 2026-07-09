"""通知消息格式化器。"""

from __future__ import annotations

import json
from typing import List

from src.notifications.events import NotificationEvent


def _build_lines(event: NotificationEvent) -> List[str]:
    """构造通用正文行。"""
    lines = [f"事件: {event.event}"]

    if event.project_type:
        lines.append(f"项目类型: {event.project_type}")
    if event.project_id:
        lines.append(f"项目编号: {event.project_id}")
    if event.sample_id:
        lines.append(f"样本编号: {event.sample_id}")
    if event.batch_id:
        lines.append(f"批次编号: {event.batch_id}")
    if event.lab_sequencer_id:
        lines.append(f"测序仪编号: {event.lab_sequencer_id}")
    if event.barcode:
        lines.append(f"条码: {event.barcode}")
    if event.raw_data_path:
        lines.append(f"下机路径: {event.raw_data_path}")
    if event.analysis_dir:
        lines.append(f"分析目录: {event.analysis_dir}")
    if event.message:
        lines.append(f"消息: {event.message}")

    lines.append(f"时间: {event.ts}")
    return lines


def _build_json_line(event: NotificationEvent) -> str:
    """构造机器可读 JSON 行。"""
    payload = json.dumps(event.to_dict(), ensure_ascii=False, sort_keys=True)
    return f"JSON: {payload}"


def format_event_text_with_json(event: NotificationEvent) -> str:
    """输出正文 + JSON 的通用格式。"""
    return "\n".join(_build_lines(event) + [_build_json_line(event)])


def format_yunzhijia_text(event: NotificationEvent) -> str:
    """格式化云之家消息。"""
    return "\n".join(_build_lines(event))


def format_feishu_text(event: NotificationEvent) -> str:
    """格式化飞书消息。"""
    return format_event_text_with_json(event)
