"""数据验证服务
负责调用 sequence_validation.py 中的 SequenceValidation 类进行路径验证
"""

import logging
from typing import Dict, Any
from datetime import datetime

from src.models.database import get_session
from src.utils.yaml_config import get_yaml_config
from src.processing.sequence_validation import SequenceValidation

# 在模块级别配置日志
from src.utils.logging_config import setup_logger
logger = setup_logger("validation_service")


class ValidationService:
    """数据验证服务，用于调用序列数据验证方法"""
    
    def __init__(self, config_file: str = None):
        """
        初始化数据验证服务
        
        Args:
            config_file: 配置文件路径
        """
        self.config = get_yaml_config(config_file)
    
    def validate_sequence_data(self) -> Dict[str, int]:
        """
        调用 sequence_validation.py 中的 SequenceValidation 类进行路径验证，分两个步骤：
        1. 先调用 validate_sequence_data_status 方法进行检查，获取验证通过的记录主键列表（使用独立session）
        2. 再调用 update_validated_sequences 方法根据验证通过的记录主键列表执行修改操作（使用独立session）
        
        Returns:
            Dict[str, int]: 验证结果统计
                - total: 总验证记录数
                - valid: 验证通过数
                - update_success: 更新成功数
                - update_failure: 更新失败数
        """
        logger.info("开始执行序列数据路径验证")
        
        valid_sequence_ids = []
        total_sequences = 0
        update_stats = {'total': 0, 'success': 0, 'failure': 0}
        
        try:
            # 步骤1：使用独立session进行验证检查
            with get_session() as check_session:
                # 创建 SequenceValidation 实例
                sequence_validation_check = SequenceValidation(check_session)
                
                # 获取验证通过的记录主键列表和总验证记录数
                valid_sequence_ids, total_sequences = sequence_validation_check.validate_sequence_data_status()
                
            logger.info(f"验证检查完成：总记录数{total_sequences}，验证通过{len(valid_sequence_ids)}条")
            
            # 步骤2：使用独立session执行修改操作
            if valid_sequence_ids:
                success_count = 0
                failure_count = 0
                
                # 对每个验证通过的记录使用独立session进行处理
                for sequence_id in valid_sequence_ids:
                    try:
                        with get_session() as update_session:
                            # 创建新的 SequenceValidation 实例用于更新
                            sequence_validation_update = SequenceValidation(update_session)
                            
                            # 执行单个记录的更新操作
                            update_success = sequence_validation_update.update_validated_sequence(sequence_id)
                            
                            if update_success:
                                success_count += 1
                            else:
                                failure_count += 1
                    except Exception as e:
                        logger.error(f"处理sequence_id={sequence_id}时发生异常: {str(e)}", exc_info=True)
                        failure_count += 1
                        continue
                
                # 更新统计信息
                update_stats = {
                    'total': len(valid_sequence_ids),
                    'success': success_count,
                    'failure': failure_count
                }
                
            # 构建返回结果
            result_stats = {
                'total': total_sequences,
                'valid': len(valid_sequence_ids),
                'update_success': update_stats['success'],
                'update_failure': update_stats['failure']
            }
            
            logger.info(f"序列数据路径验证完成，结果：{result_stats}")
            return result_stats
        except Exception as e:
            logger.error(f"序列数据路径验证过程中发生异常: {str(e)}", exc_info=True)
            # 返回空的统计结果
            return {
                'total': 0,
                'valid': 0,
                'update_success': 0,
                'update_failure': 0
            }


def run_validation_process() -> Dict[str, Any]:
    """
    数据验证流程入口函数，供调度器调用
    
    Returns:
        Dict[str, Any]: 验证结果统计信息和处理时间
    """
    logger.info("开始执行数据验证流程")
    try:
        service = ValidationService()
        result_stats = service.validate_sequence_data()
        
        # 添加处理时间
        result = {
            **result_stats,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return result
    except Exception as e:
        logger.error(f"数据验证流程执行失败: {str(e)}", exc_info=True)
        return {
            "error": str(e),
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "timeout": 0,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


if __name__ == "__main__":
    # 使用项目统一的日志配置，确保同时输出到文件和控制台
    from src.utils.logging_config import setup_logger
    logger = setup_logger("validation_service")
    
    # 测试数据验证流程
    result = run_validation_process()
    print(f"数据验证流程执行结果: {result}")