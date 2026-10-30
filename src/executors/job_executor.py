"""SGE 和 Slurm 作业提交执行器。"""

import os
import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from src.utils.yaml_config import YAMLConfig


@dataclass(frozen=True)
class SubmissionResult:
    """一次作业提交的标准化结果。"""

    success: bool
    return_code: Optional[int]
    stdout: str = ""
    stderr: str = ""
    job_id: Optional[str] = None


class JobExecutor(ABC):
    """集群作业提交执行器基类。"""

    name: str

    def __init__(self, command_path: str, timeout_seconds: int = 60):
        self.command_path = self._validate_command_path(command_path)
        if timeout_seconds <= 0:
            raise ValueError("job_submission.submit_timeout_seconds 必须大于 0")
        self.timeout_seconds = timeout_seconds

    def submit(self, script_path: str, working_directory: str) -> SubmissionResult:
        """在指定目录中提交脚本，并返回标准化结果。"""
        work_dir = Path(working_directory)
        script = Path(script_path)

        if not work_dir.is_dir():
            return SubmissionResult(
                success=False,
                return_code=None,
                stderr=f"作业工作目录不存在或不是目录: {work_dir}",
            )
        if not script.is_file():
            return SubmissionResult(
                success=False,
                return_code=None,
                stderr=f"作业脚本不存在或不是文件: {script}",
            )

        try:
            completed = subprocess.run(
                [self.command_path, script.name],
                cwd=str(work_dir),
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            return SubmissionResult(
                success=False,
                return_code=None,
                stdout=self._normalize_output(exc.stdout),
                stderr=(
                    f"{self.name} 提交命令超过 "
                    f"{self.timeout_seconds} 秒未返回"
                ),
            )
        except OSError as exc:
            return SubmissionResult(
                success=False,
                return_code=None,
                stderr=f"执行 {self.command_path} 失败: {exc}",
            )

        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        success = completed.returncode == 0
        job_id = self.parse_job_id(stdout) if success else None

        return SubmissionResult(
            success=success,
            return_code=completed.returncode,
            stdout=stdout,
            stderr=stderr,
            job_id=job_id,
        )

    @abstractmethod
    def parse_job_id(self, stdout: str) -> Optional[str]:
        """从提交命令的标准输出中解析集群 job ID。"""

    @staticmethod
    def _validate_command_path(command_path: str) -> str:
        if not command_path:
            raise ValueError("作业提交命令路径未配置")

        path = Path(command_path).expanduser()
        if not path.is_file():
            raise ValueError(f"作业提交命令不存在或不是文件: {path}")
        if not os.access(path, os.X_OK):
            raise ValueError(f"作业提交命令不可执行: {path}")
        return str(path)

    @staticmethod
    def _normalize_output(output) -> str:
        if output is None:
            return ""
        if isinstance(output, bytes):
            return output.decode(errors="replace").strip()
        return str(output).strip()


class SGEExecutor(JobExecutor):
    """通过 qsub 向 SGE 提交作业。"""

    name = "sge"
    _JOB_ID_PATTERN = re.compile(r"\bYour job(?:-array)?\s+(\d+)\b")

    def parse_job_id(self, stdout: str) -> Optional[str]:
        match = self._JOB_ID_PATTERN.search(stdout)
        return match.group(1) if match else None


class SlurmExecutor(JobExecutor):
    """通过 sbatch 向 Slurm 提交作业。"""

    name = "slurm"
    _JOB_ID_PATTERN = re.compile(r"\bSubmitted batch job\s+(\d+)\b")

    def parse_job_id(self, stdout: str) -> Optional[str]:
        match = self._JOB_ID_PATTERN.search(stdout)
        return match.group(1) if match else None


def create_job_executor(
    config: YAMLConfig,
    test_mode: bool = False,
) -> JobExecutor:
    """根据配置创建 SGE 或 Slurm 执行器。"""
    timeout_value = config.get(
        "job_submission.submit_timeout_seconds",
        default=60,
    )
    try:
        timeout_seconds = int(timeout_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "job_submission.submit_timeout_seconds 必须是整数"
        ) from exc

    if test_mode:
        simulated_qsub_path = str(
            Path(__file__).resolve().parents[2] / "tests" / "simulated_qsub.sh"
        )
        return SGEExecutor(simulated_qsub_path, timeout_seconds)

    executor_name = str(
        config.get("job_submission.executor", default="sge")
    ).strip().lower()

    if executor_name == "sge":
        command_path = config.get(
            "job_submission.qsub_path",
            required=True,
        )
        return SGEExecutor(command_path, timeout_seconds)

    if executor_name == "slurm":
        command_path = config.get(
            "job_submission.sbatch_path",
            required=True,
        )
        return SlurmExecutor(command_path, timeout_seconds)

    raise ValueError(
        "不支持的 job_submission.executor: "
        f"{executor_name!r}，可选值为 sge、slurm"
    )
