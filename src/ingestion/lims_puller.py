# src/ingestion/lims_puller.py
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass  # 用内置模块，无需额外安装

# 项目内依赖：替换为新的YAML配置工具类 + 数据库会话
from src.models.database import get_session  # 仅保留数据库会话获取
from src.repositories.input_file_repository import InputFileRepository  
from src.utils.yaml_config import get_yaml_config  # 新YAML配置工具类

# 初始化统一日志（按拉取任务维度记录）
from src.utils.logging_config import get_lims_puller_logger
logger = get_lims_puller_logger()

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
    """精确时间范围：基于上次拉取记录，避免重复，包含5分钟重叠区间"""
    pull_path = Path(pull_config["path"])
    last_pull_file = pull_path / "last_pull_time.txt"
    end_time = datetime.now()
    # 读取上次拉取结束时间（无则用当前时间-START_OFFSET）
    if last_pull_file.exists():
        with open(last_pull_file, "r") as f:
            last_end_time = datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
        # 开始时间比上次结束时间提前5分钟，确保重叠区间
        start_time = last_end_time - timedelta(minutes=5)
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
    start_time, end_time = get_precise_time_range(pull_config)  # 可以改为精确拉取时间 get_precise_time_range
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


def get_all_json_in_lims_dir(temp_path:Optional[str] = None ,config_file: Optional[str] = None) -> List[str]:
    """
    对外提供的接口：获取拉取根目录下所有JSON文件（供后续录入脚本调用）
    :param config_file: 配置文件路径
    :return: 所有JSON文件的绝对路径列表（递归扫描，含batchID子目录）
    """
    if temp_path:
        pull_path = temp_path
        pull_root_dir = Path(pull_path)
    else:
        # 加载拉取根目录配置
        yaml_config = get_yaml_config(config_file)
        pull_path = yaml_config.get("pull_request.path", required=True)
        if not pull_path:
            raise ValueError("config.yaml的pull_request.path未配置")
        
        # 递归扫描所有JSON文件
        pull_root_dir = Path(pull_path)
    # 递归扫描所有JSON文件
    all_json_paths = get_existing_json_paths(pull_root_dir)
    all_json_paths_str = [str(p.absolute()) for p in all_json_paths]
    
    logger.info(f"从{pull_root_dir.absolute()}中扫描到{len(all_json_paths_str)}个JSON文件（供录入脚本使用）")
    return all_json_paths_str


def clean_lims_data_dir(
    config_file: Optional[str] = None,
    retain_hours: int = 24,  # 已录入文件的保留时间（默认24小时）
    dry_run: bool = False,    # 测试模式：只打印要删除的文件，不实际删除
    temp_path: Optional[str] = None  # 临时指定清理目录，用于测试
) -> Dict[str, int]:
    """
    安全清理LimsData目录：仅删除“已录入数据库”且“超过retain_hours”的JSON文件
    :param config_file: 配置文件路径（默认config/config.yaml）
    :param retain_hours: 已录入文件的保留时间（小时），默认24小时
    :param dry_run: 测试模式（True：不删除，仅日志；False：实际删除）
    :param temp_path: 临时指定清理目录，用于测试，优先级高于配置文件
    :return: 清理结果字典（total_scanned: 扫描文件数, total_deleted: 实际删除数, skipped: 跳过数）
    """
    logger.info(f"=== 开始清理LimsData目录（保留已录入文件{retain_hours}小时，测试模式：{dry_run}）===")
    result = {"total_scanned": 0, "total_deleted": 0, "skipped": 0, "details": {"not_in_db": 0, "recent_file": 0, "error_processing": 0}}  # 添加更详细的统计
    file_repo = None

    try:
        # 1. 加载配置和初始化资源
        if temp_path:
            lims_dir = Path(temp_path)
            logger.info(f"使用临时指定的清理目录：{lims_dir.absolute()}")
        else:
            # 获取LimsData目录路径
            yaml_config = get_yaml_config(config_file)
            pull_path = yaml_config.get("pull_request.path", required=True)
            
            if not pull_path:
                raise ValueError("config.yaml的pull_request.path未配置，无法清理")
            lims_dir = Path(pull_path)
            logger.info(f"从配置文件加载清理目录：{lims_dir.absolute()}")

        if not lims_dir.exists():
            logger.warning(f"LimsData目录{lims_dir.absolute()}不存在，无需清理")
            return result

        # 初始化数据库会话（查询input_file_metadata）使用上下文管理器
        with get_session() as db_session:
            file_repo = InputFileRepository(db_session)   # 操作input_file_metadata表

            # 2. 递归扫描所有JSON文件
            all_json_files = get_existing_json_paths(lims_dir)  # 复用已有的递归扫描函数
            result["total_scanned"] = len(all_json_files)
            
            if len(all_json_files) == 0:
                logger.info("LimsData目录中无JSON文件，无需清理")
                return result

            # 3. 计算“清理阈值时间”（当前时间 - retain_hours）
            delete_threshold = datetime.now() - timedelta(hours=retain_hours)
            logger.info(f"清理阈值时间：{delete_threshold.strftime('%Y-%m-%d %H:%M:%S')}（早于该时间的已录入文件将被删除）")

            # 4. 逐个文件判断是否需要清理
            for json_file in all_json_files:
                file_path = json_file.absolute()
                file_name = json_file.name  # 文件名（与input_file_metadata的file_name字段一致）
                
                try:
                    # 获取文件创建时间（系统时间）
                    file_ctime = datetime.fromtimestamp(json_file.stat().st_ctime)

                    # 4.1 检查文件是否已录入数据库（input_file_metadata表）
                    existing_file = file_repo.get_by_pk(file_name)  # 按主键查询
                    if not existing_file:
                        # 未录入的文件：跳过（避免删除待录入数据）
                        logger.debug(f"文件[{file_path}]未录入input_file_metadata，跳过清理")
                        result["skipped"] += 1
                        result["details"]["not_in_db"] += 1
                        continue

                    # 4.2 检查已录入文件是否超过保留时间
                    if file_ctime > delete_threshold:
                        # 未超过保留时间：跳过（防止刚录入就被删除）
                        logger.debug(
                            f"文件[{file_path}]已录入，但创建时间（{file_ctime.strftime('%Y-%m-%d %H:%M:%S')}）"
                            f"晚于清理阈值，跳过清理"
                        )
                        result["skipped"] += 1
                        result["details"]["recent_file"] += 1
                        continue

                    # 4.3 满足清理条件：执行删除（或测试模式打印）
                    logger.info(f"文件[{file_path}]满足清理条件（已录入+创建时间超{retain_hours}小时）")
                    if not dry_run:
                        os.remove(file_path)  # 实际删除文件
                        logger.info(f"文件[{file_path}]已删除")
                        result["total_deleted"] += 1
                    else:
                        logger.info(f"【测试模式】文件[{file_path}]将被删除（未实际执行）")
                        result["total_deleted"] += 1  # 即使在测试模式下也计数，便于验证

                except FileNotFoundError:
                    logger.warning(f"文件[{file_path}]在处理过程中被其他程序删除")
                    result["skipped"] += 1
                    continue
                except Exception as e:
                    logger.error(f"处理文件[{file_path}]时异常：{str(e)}", exc_info=True)
                    result["skipped"] += 1
                    result["details"]["error_processing"] += 1
                    continue

    except Exception as e:
        logger.error(f"清理过程发生严重错误：{str(e)}", exc_info=True)
    finally:
        # 5. 清理完成，确保释放资源
        # 由于使用了上下文管理器，会话会自动关闭，这里无需额外处理
        pass

    logger.info(f"=== 清理完成 ===")
    logger.info(f"扫描文件数：{result['total_scanned']}，实际删除数：{result['total_deleted']}，跳过数：{result['skipped']}")
    if result['details']:
        logger.info(f"跳过详情：未录入数据库{result['details']['not_in_db']}个，创建时间较新{result['details']['recent_file']}个，处理异常{result['details']['error_processing']}个")
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
    import argparse
    
    try:
        # 1. 解析命令行参数
        parser = argparse.ArgumentParser(description='LIMS数据拉取脚本测试工具')
        parser.add_argument('--mode', '-m', 
                          choices=['get_files', 'pull', 'clean', 'all'], 
                          default='get_files',
                          help='测试模式：get_files(获取文件列表), pull(拉取数据), clean(清理目录), all(全部测试)')
        parser.add_argument('--config', '-c', 
                          type=str, 
                          default=None,
                          help='配置文件路径（默认使用config/config.yaml）')
        parser.add_argument('--temp-dir', 
                          type=str, 
                          default=None,
                          help='临时指定测试目录（仅用于get_files和clean模式的测试）')
        parser.add_argument('--retain-hours', 
                          type=int, 
                          default=24,
                          help='清理模式下保留已录入文件的小时数（默认24小时）')
        parser.add_argument('--no-dry-run', 
                          action='store_true',
                          help='清理模式下是否实际删除文件（默认使用dry_run=True测试模式）')
        
        args = parser.parse_args()
        test_mode = args.mode
        config_file = args.config
        temp_dir = args.temp_dir
        retain_hours = args.retain_hours
        dry_run = not args.no_dry_run
        
        logger.info(f"开始执行LIMS拉取脚本测试，模式：{test_mode}")
        
        # 2. 根据测试模式执行对应功能
        if test_mode in ["get_files", "all"]:
            print("\n===== 测试：获取所有JSON文件 =====")
            if temp_dir:
                print(f"使用临时目录进行测试：{temp_dir}")
                all_json = get_all_json_in_lims_dir(temp_path=temp_dir, config_file=config_file)
            else:
                all_json = get_all_json_in_lims_dir(config_file=config_file)
            print(f"扫描到的JSON文件数量：{len(all_json)}")
            if len(all_json) > 0:
                print(f"前5个文件路径示例：{all_json[:5]}")
            else:
                print("未找到任何JSON文件")
                
        # 3. 测试拉取数据功能（实际从LIMS系统拉取数据）
        if test_mode in ["pull", "all"]:
            print("\n===== 测试：拉取LIMS数据 =====")
            pull_results = run_lims_puller(config_file=config_file)
            print("拉取结果汇总：")
            for lab, result in pull_results.items():
                print(f"实验室 {lab}: 成功={result.success}, 新增文件数={result.new_json_count}")
                if not result.success:
                    print(f"  错误信息: {result.error_msg}")
                elif result.new_json_count > 0:
                    print(f"  新增文件示例: {result.new_json_paths[:3]}")
        
        # 4. 测试安全清理功能
        if test_mode in ["clean", "all"]:
            print(f"\n===== 测试：安全清理LimsData目录（{'实际删除' if not dry_run else '测试模式'}）=====")
            if dry_run:
                print(f"【注意】当前为测试模式，不会实际删除文件")
            clean_result = clean_lims_data_dir(
                config_file=config_file, 
                retain_hours=retain_hours, 
                dry_run=dry_run,
                temp_path=temp_dir
            )
        
        logger.info(f"LIMS拉取脚本测试完成，模式：{test_mode}")
        exit(0)
        
    except Exception as e:
        logger.critical(f"LIMS拉取脚本运行失败：{str(e)}", exc_info=True)
        print(f"错误：{str(e)}")
        exit(1)