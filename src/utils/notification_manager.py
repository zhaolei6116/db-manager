# File: notification_manager.py
import smtplib
import requests
import logging
from email.mime.text import MIMEText
from typing import Dict, Optional, Union
import time
from pathlib import Path

from src.utils.yaml_config import YAMLConfig

logger = logging.getLogger(__name__)

class NotificationManager:
    """通知管理模块，提供灵活的通知接口"""
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化通知管理器
        
        Args:
            config: 通知配置字典，如果为None则尝试从默认配置文件加载
        """
        # 默认配置
        self.default_config = {
            "email": {
                "sender": "huyifan895518851@163.com",
                "password": "DBPGXV4673DyJ6pK",
                "smtp_server": "smtp.163.com",
                "smtp_port": 465,
                "receivers": ["huyifan@cwbio.com.cn"],
                "cc": ["zhaolei@cwbio.com.cn"]
            },
            "webhook_url": "https://www.yunzhijia.com/gateway/robot/webhook/send?yzjtype=0&yzjtoken=814cb2294b5d4b009b7c678f51e55f32",
            "start": True
        }
        
        # 使用传入的配置覆盖默认配置
        self.config = config if config else self.default_config
        self.email_config = self.config.get('email')
        self.webhook_url = self.config.get('webhook_url')
        self.start = self.config.get('start', True)  # 默认启用通知
        
        # 加载YAML配置，获取项目类型对应的webhook URL
        self.yaml_config = YAMLConfig()
        # 获取webhook配置（如果配置文件中有）
        try:
            # 假设配置文件中有notification.webhooks节点
            self.project_webhooks = self.yaml_config.get("notification.webhooks", default={})
            logger.info("成功加载项目类型webhook配置")
        except Exception as e:
            logger.warning(f"加载项目类型webhook配置失败: {str(e)}")
            self.project_webhooks = {}

    def send_notification(self, message: str, status: str, module: str = "General", 
                         send_email: bool = False, send_webhook: bool = True, 
                         job_id: Optional[str] = None) -> Dict[str, bool]:
        """
        发送综合通知
        
        Args:
            message: 通知消息内容
            status: 通知状态（如"success", "error", "warning"等）
            module: 发送通知的模块名称，默认为"General"
            send_email: 是否发送邮件通知，默认为False
            send_webhook: 是否发送Webhook通知（云之家），默认为True
            job_id: 可选的任务ID，用于邮件主题
        
        Returns:
            Dict[str, bool]: 通知发送结果，包含email和webhook的发送状态
        """
        result = {'email': False, 'webhook': False}
        
        if not self.start:
            logger.debug("Notification is disabled")
            return result
        
        # 状态表情映射
        status_emoji = {
            'success': '✅',
            'error': '❌',
            'warning': '⚠️',
            'info': 'ℹ️',
            'timeout': '⏰'
        }
        
        # 获取状态对应的表情，如果没有则使用默认
        status_icon = status_emoji.get(status.lower(), '')
        full_status = f"{status_icon} {status}" if status_icon else status
        
        # 构建完整消息
        formatted_message = f"[{module}] {full_status}: {message}"
        logger.info(formatted_message)
        
        # 发送邮件
        if send_email and self.email_config:
            if job_id is None:
                job_id = f"{module}_{int(time.time())}"
            result['email'] = self._send_email(job_id, status, message)
        
        # 发送Webhook（云之家）
        if send_webhook and self.webhook_url:
            result['webhook'] = self._send_webhook(message, status, module)
        
        return result
    
    def _send_email(self, job_id: str, status: str, message: str) -> bool:
        """发送邮件通知（内部方法）"""
        if not self.email_config:
            return False
        
        subject = f"[Pipeline] {status.upper()} - {job_id}"
        content = f"""
Pipeline Job Status Update:
- Job ID: {job_id}
- Status: {status.upper()}
- Message: {message}
- Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        msg = MIMEText(content.strip())
        msg['Subject'] = subject
        msg['From'] = self.email_config['sender']
        msg['To'] = ', '.join(self.email_config['receivers'])
        
        if 'cc' in self.email_config and self.email_config['cc']:
            msg['Cc'] = ', '.join(self.email_config['cc'])

        try:
            with smtplib.SMTP_SSL(
                self.email_config['smtp_server'],
                self.email_config['smtp_port']
            ) as server:
                server.login(
                self.email_config['sender'],
                self.email_config['password']  # 从配置中获取密码
                )
                server.send_message(msg)
            logger.info(f"Email notification sent for {job_id}")
            return True
        except Exception as e:
            logger.error(f"Email notification failed: {str(e)}")
            return False
    
    def get_webhook_url_for_project(self, project_type: Optional[str] = None) -> str:
        """
        根据项目类型获取对应的webhook URL
        
        Args:
            project_type: 项目类型
        
        Returns:
            str: 对应项目类型的webhook URL，如果不存在则返回默认URL
        """
        if project_type and project_type in self.project_webhooks:
            webhook_url = self.project_webhooks[project_type]
            logger.debug(f"使用项目类型 {project_type} 对应的webhook URL")
            return webhook_url
        else:
            logger.debug(f"未找到项目类型 {project_type} 对应的webhook URL，使用默认URL")
            return self.webhook_url
    
    def _send_webhook(self, message: str, status: str, module: str = "General", 
                      project_type: Optional[str] = None) -> bool:
        """发送Webhook通知（云之家）"""
        # 根据项目类型获取webhook URL
        webhook_url = self.get_webhook_url_for_project(project_type)
        if not webhook_url:
            return False
        
        # 状态表情映射
        status_emoji = {
            'success': '✅',
            'error': '❌',
            'warning': '⚠️',
            'info': 'ℹ️',
            'timeout': '⏰'
        }
        
        status_icon = status_emoji.get(status.lower(), '')
        full_status = f"{status_icon} {status}" if status_icon else status
        
        # 构建云之家消息格式
        payload = {
            "content": f"""模块: {module}
状态: {full_status}
消息: {message}
时间: {time.strftime('%Y-%m-%d %H:%M:%S')}
"""
        }
        
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=10
            )
            if response.status_code == 200:
                logger.info(f"Webhook notification sent for {module}")
                return True
            else:
                logger.error(f"Webhook notification failed with code {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Webhook notification error: {str(e)}")
            return False
    
    def send_yunzhijia_alert(self, message: str, module: str = "General", 
                           status: str = "warning", project_type: Optional[str] = None) -> bool:
        """
        便捷方法：只发送云之家提醒
        
        Args:
            message: 提醒消息
            module: 模块名称
            status: 状态类型
            project_type: 项目类型，用于选择对应的webhook URL
        
        Returns:
            bool: 发送是否成功
        """
        result = self.send_notification(
            message=message,
            status=status,
            module=module,
            send_email=False,
            send_webhook=True
        )
        
        # 如果提供了project_type，还需要发送到项目类型对应的webhook
        if project_type and self.get_webhook_url_for_project(project_type) != self.webhook_url:
            try:
                project_result = self._send_webhook(
                    message=message,
                    status=status,
                    module=module,
                    project_type=project_type
                )
                # 如果项目特定的webhook发送成功，则整体返回成功
                if project_result:
                    return True
            except Exception as e:
                logger.error(f"发送项目类型特定的webhook通知失败: {str(e)}")
        
        return result['webhook']


# 创建全局实例，方便其他模块直接导入使用
notification_manager = NotificationManager()

if __name__ == "__main__":
    print("Notification Manager Test")
    
    # 测试便捷方法
    notification_manager.send_yunzhijia_alert(
        message="测试云之家提醒功能", 
        module="测试模块", 
        status="info"
    )
    
    # # 测试完整通知功能
    # result = notification_manager.send_notification(
    #     message="这是一个测试通知",
    #     status="warning",
    #     module="测试模块",
    #     send_email=False,
    #     send_webhook=True,
    #     job_id="test_job_001"
    # )
    
    # print(f"通知发送结果: {result}")