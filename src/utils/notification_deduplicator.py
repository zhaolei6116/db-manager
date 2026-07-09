import json
import logging
import os
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional, Set

from src.utils.yaml_config import get_yaml_config

try:
    import fcntl
except Exception:
    fcntl = None


@dataclass(frozen=True)
class DedupRecord:
    key: str
    ts: str
    channel: Optional[str] = None


class NotificationDeduplicator:
    def __init__(self) -> None:
        config = get_yaml_config()
        log_config = config.get_log_config()

        dedup_dir = os.environ.get("NOTIFICATION_DEDUP_DIR")
        if dedup_dir:
            self._dir = Path(dedup_dir)
        else:
            self._dir = Path(log_config["log_dir"])

        try:
            self._dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            self._dir = Path("./logs")
            self._dir.mkdir(parents=True, exist_ok=True)

        self._path = self._dir / "notification_dedup.log"
        self._lock_path = self._dir / "notification_dedup.lock"
        self._logger = self._build_logger(
            max_bytes=log_config["max_bytes"],
            backup_count=log_config["backup_count"],
        )

        self._timeout_keys: Set[str] = set()
        self._recovered_success_channels: Dict[str, Set[str]] = {}
        self._load()

    def has_timeout_sent(self, sequence_id: str) -> bool:
        return self._timeout_key(sequence_id) in self._timeout_keys

    def mark_timeout_sent(self, sequence_id: str, ts: str) -> None:
        key = self._timeout_key(sequence_id)
        if key in self._timeout_keys:
            return
        self._write_record(DedupRecord(key=key, ts=ts))
        self._timeout_keys.add(key)

    def get_recovered_success_channels(self, sequence_id: str) -> Set[str]:
        return set(self._recovered_success_channels.get(self._recovered_key(sequence_id), set()))

    def mark_recovered_channel_success(self, sequence_id: str, channel: str, ts: str) -> None:
        key = self._recovered_key(sequence_id)
        channels = self._recovered_success_channels.setdefault(key, set())
        if channel in channels:
            return
        self._write_record(DedupRecord(key=key, ts=ts, channel=channel))
        channels.add(channel)

    def is_recovered_complete(self, sequence_id: str) -> bool:
        channels = self._recovered_success_channels.get(self._recovered_key(sequence_id), set())
        return "yunzhijia" in channels and "feishu" in channels

    def _timeout_key(self, sequence_id: str) -> str:
        return f"{sequence_id}|DATA_PATH_TIMEOUT"

    def _recovered_key(self, sequence_id: str) -> str:
        return f"{sequence_id}|DATA_PATH_RECOVERED"

    def _build_logger(self, max_bytes: int, backup_count: int) -> logging.Logger:
        logger = logging.getLogger("notification_dedup")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

        handler = RotatingFileHandler(
            self._path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        return logger

    def _load(self) -> None:
        for path in self._iter_log_files():
            self._load_single_file(path)

    def _iter_log_files(self):
        yield self._path
        for i in range(1, 100):
            rotated = Path(f"{self._path}.{i}")
            if rotated.exists():
                yield rotated
            else:
                break

    def _load_single_file(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    key = obj.get("key")
                    if not key:
                        continue
                    if key.endswith("|DATA_PATH_TIMEOUT"):
                        self._timeout_keys.add(key)
                    if key.endswith("|DATA_PATH_RECOVERED"):
                        channel = obj.get("channel")
                        if channel:
                            channels = self._recovered_success_channels.setdefault(key, set())
                            channels.add(channel)
        except Exception:
            return

    def _write_record(self, record: DedupRecord) -> None:
        payload = {"key": record.key, "ts": record.ts}
        if record.channel:
            payload["channel"] = record.channel

        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        lock_fh = None
        try:
            lock_fh = self._lock_path.open("a+", encoding="utf-8")
            if fcntl:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            self._logger.info(line)
        finally:
            if lock_fh and fcntl:
                try:
                    fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
            if lock_fh:
                try:
                    lock_fh.close()
                except Exception:
                    pass


notification_deduplicator = NotificationDeduplicator()
