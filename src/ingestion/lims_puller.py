# src/ingestion/lims_puller.py
import yaml
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass  # 用内置模块，无需额外安装

# 项目内依赖：替换为新的YAML配置工具类 + 数据库会话
from src.models.database import get_session  # 仅保留数据库会话获取，
from src.models.models import InputFileMetadata  
from src.repositories.input_file_repository import InputFileRepository  
from src.utils.yaml_config import get_yaml_config  # 新YAML配置工具类

# 初始化统一日志（按拉取任务维度记录）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/lims_puller.log"), logging.StreamHandler()]
)
logger = logging.getLogger("LIMS_Puller")

# 外部LIMS下载器导入（兼容原有调用，报错时明确提示）
try:
    from lims_python.cwbio_lims_downloader import CwbioLimsDownloader
    logger.info("成功导入 lims_python.cwbio_lims_downloader")
except ImportError as e:
    logger.error(f"导入LIMS下载器失败：{str(e)}，请确认lims_python包已安装或路径正确")
    raise


@dataclass
class PullResult:
    """
    拉取结果数据类（结构化返回拉取信息，便于后续脚本调用）
    - lab: 实验室标识（如T、W）
    - success: 拉取是否成功（True/False）
    - new_json_count: 本次拉取新增的JSON文件数（非累计，仅本次）
    - new_json_paths: 本次拉取新增的JSON文件完整路径（空列表表示无新增）
    - error_msg: 错误信息（success=False时非空）
    """
    lab: str
    success: bool
    new_json_count: int
    new_json_paths: List[str]
    error_msg: str = ""


def dict_to_object(d: Dict) -> object:
    """将字典转为对象（适配CwbioLimsDownloader的参数格式）"""
    class ConfigObject:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
    return ConfigObject(** d)


def validate_pull_config(pull_config: Dict) -> None:
    """校验pull_request配置完整性（避免拉取参数缺失）"""
    required_keys = ["labs", "START_OFFSET", "path"]
    missing_keys = [k for k in required_keys if k not in pull_config]
    if missing_keys:
        raise ValueError(
            f"config.yaml的pull_request节点缺失必填配置：{missing_keys}，"
            f"请补充（例：pull_request: {{labs: [T,W], START_OFFSET:24, path:/data/LimsData/}}）"
        )
    # 校验目录（不存在则自动创建）
    pull_path = Path(pull_config["path"])
    if not pull_path.exists():
        pull_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"拉取根目录{pull_path.absolute()}不存在，已自动创建")
    elif not pull_path.is_dir():
        raise ValueError(f"pull_request.path={pull_path.absolute()}不是目录，请检查")


def get_time_range(pull_config: Dict) -> Tuple[str, str]:
    """计算拉取时间范围（当前时间 - START_OFFSET 到当前时间）"""
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=pull_config["START_OFFSET"])
    time_format = "%Y-%m-%d %H:%M:%S"
    return start_time.strftime(time_format), end_time.strftime(time_format)


def get_precise_time_range(pull_config: Dict) -> Tuple[str, str]:
    """精确时间范围：基于上次拉取记录，避免重复"""
    pull_path = Path(pull_config["path"])
    last_pull_file = pull_path / "last_pull_time.txt"
    end_time = datetime.now()
    # 读取上次拉取结束时间（无则用当前时间-30分钟）
    if last_pull_file.exists():
        with open(last_pull_file, "r") as f:
            last_end_time = datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
        start_time = last_end_time
    else:
        start_time = end_time - timedelta(hours=pull_config["START_OFFSET"])
    # 记录本次拉取结束时间
    with open(last_pull_file, "w") as f:
        f.write(end_time.strftime("%Y-%m-%d %H:%M:%S"))
    return start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S")


def get_existing_json_paths(pull_root_dir: Path) -> List[Path]:
    """获取拉取根目录下已存在的所有JSON文件（递归扫描，用于对比新增）"""
    if not pull_root_dir.exists():
        return []
    # 递归扫描所有子目录的.json文件（含batchID子目录）
    return list(pull_root_dir.glob("**/*.json"))


def get_new_json_paths(
    pull_root_dir: Path, pre_pull_paths: List[Path]
) -> Tuple[List[Path], List[str]]:
    """
    对比拉取前后的JSON文件，获取本次拉取新增的文件
    :param pull_root_dir: 拉取根目录
    :param pre_pull_paths: 拉取前已存在的JSON路径列表
    :return: (新增文件Path列表, 新增文件绝对路径字符串列表)
    """
    post_pull_paths = get_existing_json_paths(pull_root_dir)
    # 找出拉取后有但拉取前没有的文件（基于绝对路径对比）
    pre_pull_abs = {p.absolute() for p in pre_pull_paths}
    new_paths = [p for p in post_pull_paths if p.absolute() not in pre_pull_abs]
    new_paths_str = [str(p.absolute()) for p in new_paths]
    return new_paths, new_paths_str


def pull_lab_data(
    lab: str, pull_path: str, start_time: str, end_time: str
) -> PullResult:
    """
    单实验室拉取任务（核心拉取逻辑，返回结构化结果）
    :param lab: 实验室标识（如T）
    :param pull_path: 拉取根目录
    :param start_time: 拉取开始时间
    :param end_time: 拉取结束时间
    :return: 该实验室的拉取结果（PullResult对象）
    """
    pull_root_dir = Path(pull_path)
    logger.info(f"=== 开始拉取实验室[{lab}]数据（{start_time} ~ {end_time}）===")

    # 1. 记录拉取前已存在的JSON文件（用于后续对比新增）
    pre_pull_paths = get_existing_json_paths(pull_root_dir)
    logger.debug(f"拉取前，{pull_root_dir.absolute()}下已存在{len(pre_pull_paths)}个JSON文件")

    try:
        # 2. 组装拉取参数（适配LIMS下载器）
        pull_args = {
            "path": pull_path,
            "startTime": start_time,
            "endTime": end_time,
            "lab": lab
        }
        args_obj = dict_to_object(pull_args)
        logger.debug(f"实验室[{lab}]拉取参数：{pull_args}")

        # 3. 调用LIMS下载器拉取
        downloader = CwbioLimsDownloader()
        returncode = downloader.run(args_obj)

        # 4. 检查拉取返回码（假设0为成功，需与下载器定义对齐）
        if returncode != 0:
            error_msg = f"实验室[{lab}]拉取返回码非0（returncode={returncode}），可能拉取失败"
            logger.error(error_msg)
            return PullResult(
                lab=lab, success=False, new_json_count=0,
                new_json_paths=[], error_msg=error_msg
            )

        # 5. 对比拉取前后，获取新增文件
        _, new_json_paths = get_new_json_paths(pull_root_dir, pre_pull_paths)
        new_count = len(new_json_paths)

        # 6. 日志记录新增结果
        if new_count > 0:
            logger.info(f"实验室[{lab}]拉取成功，新增{new_count}个JSON文件：{new_json_paths}")
        else:
            logger.info(f"实验室[{lab}]拉取成功，但未新增JSON文件（可能无新数据）")

        # 7. 返回成功结果
        return PullResult(
            lab=lab, success=True, new_json_count=new_count,
            new_json_paths=new_json_paths, error_msg=""
        )

    except Exception as e:
        error_msg = f"实验室[{lab}]拉取异常：{str(e)}"
        logger.error(error_msg, exc_info=True)
        return PullResult(
            lab=lab, success=False, new_json_count=0,
            new_json_paths=[], error_msg=error_msg
        )


def run_lims_puller(config_file: Optional[str] = None) -> Dict[str, PullResult]:
    """
    LIMS数据拉取主函数（对外接口，30分钟定时任务调用）
    :param config_file: 配置文件路径（默认config/config.yaml）
    :return: 所有实验室的拉取结果字典（key=实验室标识，value=PullResult对象）
    """
    # 1. 加载并校验配置
    try:
        config = get_yaml_config(config_file)
        pull_config = config.get("pull_request")
        if not pull_config:
            raise ValueError("config.yaml中未找到pull_request节点")
        validate_pull_config(pull_config)
        logger.info("LIMS拉取配置校验通过，启动多实验室拉取任务")
    except Exception as e:
        logger.error(f"配置初始化失败：{str(e)}，终止拉取任务")
        raise

    # 2. 准备拉取基础参数
    pull_path = pull_config["path"]
    labs = pull_config["labs"]
    start_time, end_time = get_time_range(pull_config)
    pull_results = {}  # 存储所有实验室的拉取结果

    # 新增：拉取前先执行安全清理（保留24小时内已录入文件）
    clean_result = clean_lims_data_dir(config_file, retain_hours=24, dry_run=False)
    logger.info(f"拉取前清理结果：{clean_result}")

    # 3. 循环拉取每个实验室（单实验室失败不影响其他）
    for lab in labs:
        lab_result = pull_lab_data(lab, pull_path, start_time, end_time)
        pull_results[lab] = lab_result
        logger.info(f"=== 实验室[{lab}]拉取任务结束（成功：{lab_result.success}，新增文件数：{lab_result.new_json_count}）===\n")

    # 4. 整体拉取结果统计
    total_success_labs = sum(1 for res in pull_results.values() if res.success)
    total_new_files = sum(res.new_json_count for res in pull_results.values())
    logger.info("=" * 50)
    logger.info("所有实验室拉取任务完成，整体统计：")
    logger.info(f"- 总实验室数：{len(labs)}")
    logger.info(f"- 拉取成功实验室数：{total_success_labs}")
    logger.info(f"- 拉取失败实验室数：{len(labs) - total_success_labs}")
    logger.info(f"- 本次拉取新增JSON文件总数：{total_new_files}")
    logger.info("=" * 50)

    return pull_results


def get_all_json_in_lims_dir(config_file: Optional[str] = None) -> List[str]:
    """
    对外提供的接口：获取拉取根目录下所有JSON文件（供后续录入脚本调用）
    :param config_file: 配置文件路径
    :return: 所有JSON文件的绝对路径列表（递归扫描，含batchID子目录）
    """
    # 加载拉取根目录配置
    yaml_config = get_yaml_config(config_file)
    pull_path = yaml_config.get("pull_request.path", required=True)
    if not pull_path:
        raise ValueError("config.yaml的pull_request.path未配置")
    
    # 递归扫描所有JSON文件
    pull_root_dir = Path(pull_path)
    all_json_paths = get_existing_json_paths(pull_root_dir)
    all_json_paths_str = [str(p.absolute()) for p in all_json_paths]
    
    logger.info(f"从{pull_root_dir.absolute()}中扫描到{len(all_json_paths_str)}个JSON文件（供录入脚本使用）")
    return all_json_paths_str


def clean_lims_data_dir(
    config_file: Optional[str] = None,
    retain_hours: int = 24,  # 已录入文件的保留时间（默认24小时）
    dry_run: bool = False     # 测试模式：只打印要删除的文件，不实际删除
) -> Dict[str, int]:
    """
    安全清理LimsData目录：仅删除“已录入数据库”且“超过retain_hours”的JSON文件
    :param config_file: 配置文件路径（默认config/config.yaml）
    :param retain_hours: 已录入文件的保留时间（小时），默认24小时
    :param dry_run: 测试模式（True：不删除，仅日志；False：实际删除）
    :return: 清理结果字典（total_scanned: 扫描文件数, total_deleted: 实际删除数, skipped: 跳过数）
    """
    logger.info(f"=== 开始清理LimsData目录（保留已录入文件{retain_hours}小时，测试模式：{dry_run}）===")
    result = {"total_scanned": 0, "total_deleted": 0, "skipped": 0}

    # 1. 加载配置和初始化资源
    try:
        # 获取LimsData目录路径
        yaml_config = get_yaml_config(config_file)
        pull_path = yaml_config.get("pull_request.path", required=True)
        
        if not pull_path:
            raise ValueError("config.yaml的pull_request.path未配置，无法清理")
        lims_dir = Path(pull_path)
        if not lims_dir.exists():
            logger.warning(f"LimsData目录{lims_dir.absolute()}不存在，无需清理")
            return result

        # 初始化数据库会话（查询input_file_metadata）
        db_session = get_session()
        file_repo = InputFileRepository(db_session)   # 操作input_file_metadata表
    except Exception as e:
        logger.error(f"清理初始化失败：{str(e)}", exc_info=True)
        return result

    # 2. 递归扫描所有JSON文件
    all_json_files = get_existing_json_paths(lims_dir)  # 复用已有的递归扫描函数
    result["total_scanned"] = len(all_json_files)
    if len(all_json_files) == 0:
        logger.info("LimsData目录中无JSON文件，无需清理")
        db_session.close()
        return result

    # 3. 计算“清理阈值时间”（当前时间 - retain_hours）
    delete_threshold = datetime.now() - timedelta(hours=retain_hours)
    logger.info(f"清理阈值时间：{delete_threshold.strftime('%Y-%m-%d %H:%M:%S')}（早于该时间的已录入文件将被删除）")

    # 4. 逐个文件判断是否需要清理
    for json_file in all_json_files:
        file_path = json_file.absolute()
        file_name = json_file.name  # 文件名（与input_file_metadata的file_name字段一致）
        file_ctime = datetime.fromtimestamp(json_file.stat().st_ctime)  # 文件创建时间（系统时间）

        try:
            # 4.1 检查文件是否已录入数据库（input_file_metadata表）
            existing_file = file_repo.get_by_pk(file_name)  # 按主键查询
            if not existing_file:
                # 未录入的文件：跳过（避免删除待录入数据）
                logger.debug(f"文件[{file_path}]未录入input_file_metadata，跳过清理")
                result["skipped"] += 1
                continue

            # 4.2 检查已录入文件是否超过保留时间
            if file_ctime > delete_threshold:
                # 未超过保留时间：跳过（防止刚录入就被删除）
                logger.debug(
                    f"文件[{file_path}]已录入，但创建时间（{file_ctime.strftime('%Y-%m-%d %H:%M:%S')}）"
                    f"晚于清理阈值，跳过清理"
                )
                result["skipped"] += 1
                continue

            # 4.3 满足清理条件：执行删除（或测试模式打印）
            logger.info(f"文件[{file_path}]满足清理条件（已录入+创建时间超{retain_hours}小时）")
            if not dry_run:
                os.remove(file_path)  # 实际删除文件
                logger.info(f"文件[{file_path}]已删除")
                result["total_deleted"] += 1
            else:
                logger.info(f"【测试模式】文件[{file_path}]将被删除（未实际执行）")

        except Exception as e:
            logger.error(f"处理文件[{file_path}]时异常：{str(e)}", exc_info=True)
            result["skipped"] += 1
            continue

    # 5. 清理完成，释放资源
    db_session.close()
    logger.info(f"=== 清理完成 ===")
    logger.info(f"扫描文件数：{result['total_scanned']}，实际删除数：{result['total_deleted']}，跳过数：{result['skipped']}")
    return result


def force_clear_lims_data_dir(
    config_file: Optional[str] = None,
    confirm_key: str = ""  # 强制校验密钥，防止误调用
) -> bool:
    """
    强制清空LimsData目录（谨慎使用！生产环境不推荐）
    :param config_file: 配置文件路径
    :param confirm_key: 校验密钥（需与预设值一致才允许清空）
    :return: 清空成功返回True，失败返回False
    """
    # 1. 安全校验：必须提供正确的密钥（避免误调用）
    PRESET_CONFIRM_KEY = "FORCE_CLEAR_2024"  # 预设密钥，可修改为更复杂的值
    if confirm_key != PRESET_CONFIRM_KEY:
        logger.error("强制清空失败：校验密钥错误，禁止执行（防止误调用）")
        return False

    # 2. 加载LimsData目录路径
    try:
        
        yaml_config = get_yaml_config(config_file)
        pull_path = yaml_config.get("pull_request.path", required=True)
        if not pull_path:
            raise ValueError("config.yaml的pull_request.path未配置")
        lims_dir = Path(pull_path)
        if not lims_dir.exists():
            logger.warning(f"LimsData目录{lims_dir.absolute()}不存在，无需清空")
            return True
    except Exception as e:
        logger.error(f"强制清空初始化失败：{str(e)}", exc_info=True)
        return False

    # 3. 递归删除所有JSON文件
    all_json_files = get_existing_json_paths(lims_dir)
    if len(all_json_files) == 0:
        logger.info("LimsData目录中无JSON文件，无需清空")
        return True

    logger.warning(f"=== 执行强制清空！将删除LimsData目录下{len(all_json_files)}个JSON文件 ===")
    delete_count = 0
    for json_file in all_json_files:
        try:
            os.remove(json_file.absolute())
            delete_count += 1
            logger.warning(f"已强制删除：{json_file.absolute()}")
        except Exception as e:
            logger.error(f"强制删除文件{json_file.absolute()}失败：{str(e)}", exc_info=True)

    logger.warning(f"=== 强制清空完成，共删除{delete_count}个JSON文件 ===")
    return True


# 脚本直接运行入口（用于测试拉取逻辑，30分钟定时任务可调用run_lims_puller）
if __name__ == "__main__":
    try:
        # 测试拉取（使用默认配置）
        # run_lims_puller(config_file="config/config.yaml")
        # run_lims_puller()
        # 测试获取所有JSON接口（后续录入脚本会调用类似逻辑）
        # all_json = get_all_json_in_lims_dir(config_file="config/config.yaml")
        all_json = get_all_json_in_lims_dir()
        print(f"所有JSON文件：{all_json}")
    except Exception as e:
        logger.critical(f"LIMS拉取脚本运行失败：{str(e)}", exc_info=True)
        exit(1)