"""统一事件对象定义与构造工具。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional


TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))


@dataclass(frozen=True)
class NotificationEvent:
    """通知事件对象。

    保持字段稳定，便于下游机器人、AI 与表格解析。
    """

    event: str
    project_type: str
    project_id: str
    sample_id: str
    batch_id: Optional[str] = None
    lab_sequencer_id: Optional[str] = None
    barcode: Optional[str] = None
    raw_data_path: Optional[str] = None
    analysis_dir: Optional[str] = None
    message: Optional[str] = None
    ts: str = ""
    event_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """转为字典，仅保留非空字段。"""
        data = asdict(self)
        return {key: value for key, value in data.items() if value is not None and value != ""}


def get_current_timestamp() -> str:
    """返回东八区 ISO8601 时间。"""
    return datetime.now(TZ_UTC_PLUS_8).isoformat()


def build_event_id(event: str, project_id: str, sample_id: str) -> str:
    """生成稳定事件 ID。"""
    return f"{project_id}|{sample_id}|{event}"


def build_notification_event(
    event: str,
    project_type: str,
    project_id: str,
    sample_id: str,
    batch_id: Optional[str] = None,
    lab_sequencer_id: Optional[str] = None,
    barcode: Optional[str] = None,
    raw_data_path: Optional[str] = None,
    analysis_dir: Optional[str] = None,
    message: Optional[str] = None,
) -> NotificationEvent:
    """构造统一事件对象。"""
    ts = get_current_timestamp()
    event_id = build_event_id(event=event, project_id=project_id, sample_id=sample_id)
    return NotificationEvent(
        event=event,
        project_type=project_type,
        project_id=project_id,
        sample_id=sample_id,
        batch_id=batch_id,
        lab_sequencer_id=lab_sequencer_id,
        barcode=barcode,
        raw_data_path=raw_data_path,
        analysis_dir=analysis_dir,
        message=message,
        ts=ts,
        event_id=event_id,
    )

