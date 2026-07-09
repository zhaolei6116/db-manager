import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from src.notifications.dispatcher import NotificationDispatcher
from src.notifications.events import build_notification_event
from src.processing.json_data_processor import JSONDataProcessor
from src.utils.yaml_config import YAMLConfig


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def _get_latest_subdirectory(parent_dir: Path) -> Tuple[Optional[Path], str]:
    try:
        subdirs = [d for d in parent_dir.iterdir() if d.is_dir()]
        if not subdirs:
            return None, f"目录{parent_dir}下没有子目录"
        subdirs.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        return subdirs[0], ""
    except Exception as e:
        return None, f"获取子目录过程中发生错误: {str(e)}"


def validate_sequence_path(raw_data_path: str, barcode: str, dir2: str) -> Tuple[bool, str]:
    if not raw_data_path:
        return False, "raw_data_path字段为空"

    scan_dir = Path(raw_data_path)
    if not scan_dir.exists() or not scan_dir.is_dir():
        return False, f"目录{scan_dir}不存在或不是目录"

    latest_first_level_dir, first_level_error = _get_latest_subdirectory(scan_dir)
    if not latest_first_level_dir:
        return False, first_level_error

    latest_second_level_dir, second_level_error = _get_latest_subdirectory(latest_first_level_dir)
    if not latest_second_level_dir:
        return False, second_level_error

    updated_done_file = latest_second_level_dir / "updated.done"
    if not updated_done_file.exists() or not updated_done_file.is_file():
        return False, f"文件{updated_done_file}不存在，数据可能传输不完整"

    full_barcode_path = latest_second_level_dir / dir2 / barcode
    if not full_barcode_path.exists() or not full_barcode_path.is_dir():
        return False, f"路径{full_barcode_path}不存在或不是目录"

    try:
        has_contents = any(True for _ in full_barcode_path.iterdir())
        if not has_contents:
            return False, f"路径{full_barcode_path}存在但为空文件夹，下机数据不存在"
    except Exception as e:
        return False, f"检查路径{full_barcode_path}内容时发生错误: {str(e)}"

    return True, str(full_barcode_path)


def create_fake_raw_data_tree(raw_data_root: Path, barcode: str, dir2: str) -> None:
    first = raw_data_root / "run_0001"
    second = first / "basecall_0001"
    second.mkdir(parents=True, exist_ok=True)

    _touch(second / "updated.done")

    barcode_dir = second / dir2 / barcode
    barcode_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = barcode_dir / "dummy.fastq.gz"
    dummy_file.write_text("dummy", encoding="utf-8")

    now = datetime.now().timestamp()
    os.utime(first, (now, now))
    os.utime(second, (now, now))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("json_files", nargs="+")
    parser.add_argument("--raw-root", default=str(Path.cwd() / ".tmp_sequencing"))
    parser.add_argument("--analysis-root", default=str(Path.cwd() / ".tmp_analysis"))
    parser.add_argument("--simulate-raw", action="store_true")
    parser.add_argument("--send-ready", action="store_true")
    args = parser.parse_args()

    yaml_config = YAMLConfig()
    dispatcher = NotificationDispatcher(yaml_config=yaml_config)
    processor = JSONDataProcessor()

    sequence_info = yaml_config.get_sequence_info_config()
    dir2 = sequence_info.get("dir2", "fastq_pass")

    raw_root = Path(args.raw_root)
    analysis_root = Path(args.analysis_root)

    for json_file in args.json_files:
        json_path = Path(json_file)
        result = processor.parse_json_file(json_path)
        if not result:
            print(f"解析失败或被过滤: {json_path}")
            continue

        seq = result.get("sequence", {})
        project = result.get("project", {})
        sample = result.get("sample", {})
        batch = result.get("batch", {})

        project_id = project.get("project_id", "未知")
        sample_id = sample.get("sample_id", "未知")
        project_type = seq.get("project_type", "")
        batch_id = batch.get("batch_id", "")
        lab_sequencer_id = seq.get("lab_sequencer_id", "")
        barcode = seq.get("barcode", "")

        raw_data_root = raw_root / lab_sequencer_id / batch_id
        raw_data_path = str(raw_data_root) + "/"

        if args.simulate_raw:
            create_fake_raw_data_tree(raw_data_root, barcode=barcode, dir2=dir2)

        is_valid, final_path_or_reason = validate_sequence_path(
            raw_data_path=raw_data_path,
            barcode=barcode,
            dir2=dir2,
        )

        print(f"{json_path.name} raw_data_check: {is_valid}, {final_path_or_reason}")

        if not args.send_ready:
            continue

        if not is_valid:
            continue

        analysis_dir = analysis_root / project_id
        analysis_dir.mkdir(parents=True, exist_ok=True)
        input_tsv = analysis_dir / "input.tsv"
        if not input_tsv.exists():
            input_tsv.write_text("", encoding="utf-8")

        event = build_notification_event(
            event="READY_TO_RUN",
            project_type=project_type,
            project_id=project_id,
            sample_id=sample_id,
            batch_id=batch_id,
            raw_data_path=str(final_path_or_reason),
            analysis_dir=str(analysis_dir),
            message="分析目录和输入文件已准备完成",
        )

        dispatch_result = dispatcher.dispatch(event)
        print(f"{json_path.name} READY_TO_RUN dispatch: {dispatch_result}")


if __name__ == "__main__":
    main()
