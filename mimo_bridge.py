#!/usr/bin/env python3
"""
MiMo Bridge - 本地小宋 <-> 云端小宋 通讯脚本
通过 WebSocket 连接小米 AI Studio，实现程序化对话和文件同步。

依赖: pip install websocket-client requests
"""

import json
import uuid
import time
import re
import queue
import requests
import threading
import os
import urllib.parse
from typing import Optional, Callable

from logger import setup_logger, get_logger
from file_sync_manager import FileManager
from sync_history import SyncHistory

try:
    import websocket
except ImportError:
    print("请安装依赖: pip install websocket-client")
    raise


class MiMoBridge:
    """小米 AI Studio WebSocket 通讯桥接"""

    BASE_URL = "https://aistudio.xiaomimimo.com"
    WS_URL = "wss://aistudio.xiaomimimo.com/ws/proxy"

    def __init__(self, config: dict):
        self.config = config
        self.cookies = {
            "serviceToken": config["serviceToken"],
            "userId": config["userId"],
            "xiaomichatbot_ph": config["xiaomichatbot_ph"],
        }
        self.ws: Optional[websocket.WebSocket] = None
        self.ticket: Optional[str] = None
        self.connected = False
        self._handshake_done = threading.Event()
        self._response_buffer = []
        self._response_done = threading.Event()
        self._current_run_id = None
        
        # 初始化日志
        self.logger = setup_logger(config)
        
        # 初始化文件管理器
        self.file_manager = FileManager(config, self.cookies)
        
        # 初始化同步历史
        self.sync_history = SyncHistory("sync_history.json")
        
        # 初始化消息队列
        self._message_queue = queue.Queue()
        self._queue_worker_thread = threading.Thread(target=self._queue_worker, daemon=True)

    def _cookie_str(self) -> str:
        return (
            f'serviceToken="{self.cookies["serviceToken"]}"; '
            f'userId={self.cookies["userId"]}; '
            f'xiaomichatbot_ph="{self.cookies["xiaomichatbot_ph"]}"'
        )

    def _headers(self) -> list:
        return [
            f"Cookie: {self._cookie_str()}",
            "Origin: https://aistudio.xiaomimimo.com",
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36",
            "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
        ]

    def _get_ticket(self) -> str:
        ph_encoded = urllib.parse.quote(self.cookies["xiaomichatbot_ph"], safe="")
        url = f"{self.BASE_URL}/open-apis/user/ws/ticket?xiaomichatbot_ph={ph_encoded}"
        headers = {
            "Content-Type": "application/json",
            "Cookie": self._cookie_str(),
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36",
        }
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        ticket = data.get("data", {}).get("ticket") or data.get("ticket")
        if not ticket:
            raise ValueError(f"获取 ticket 失败: {data}")
        return ticket

    def connect(self):
        self.logger.info("正在连接 WebSocket...")
        
        # 清理旧连接
        if self.ws:
            self.ws.close()
        if hasattr(self, '_ws_thread') and self._ws_thread and self._ws_thread.is_alive():
            self.logger.info("等待旧线程结束...")
            self._ws_thread.join(timeout=5)
        
        # 重置状态
        self.connected = False
        self._handshake_done.clear()
        self._response_buffer.clear()
        self._response_done.clear()
        
        # 获取新ticket并连接
        self.ticket = self._get_ticket()
        ws_url = f"{self.WS_URL}?ticket={self.ticket}"

        self.ws = websocket.WebSocketApp(
            ws_url,
            header=self._headers(),
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self._ws_thread = threading.Thread(
            target=self.ws.run_forever,
            kwargs={"ping_interval": 30, "ping_timeout": 10},
            daemon=True,
        )
        self._ws_thread.start()

        timeout = 15
        start = time.time()
        while not self._handshake_done.is_set() and time.time() - start < timeout:
            time.sleep(0.1)
        if not self._handshake_done.is_set():
            raise ConnectionError("WebSocket 握手超时")
        
        # 启动消息队列处理（如果还没启动）
        if not self._queue_worker_thread.is_alive():
            self._queue_worker_thread = threading.Thread(target=self._queue_worker, daemon=True)
            self._queue_worker_thread.start()

    def _on_open(self, ws):
        self.logger.info("WebSocket 已连接，等待 challenge...")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        msg_type = data.get("type")
        event = data.get("event")
        payload = data.get("payload", {})
        method = data.get("method", "")

        if event == "connect.challenge":
            nonce = payload.get("nonce", "")
            self._send_connect(nonce)

        elif msg_type == "res":
            if data.get("ok") and payload.get("type") == "hello-ok":
                self.connected = True
                self._handshake_done.set()
                self.logger.info("握手成功")

        elif event == "agent":
            agent_data = payload.get("data", {})
            delta = agent_data.get("delta", "")
            self._current_run_id = payload.get("runId")
            if delta:
                self._response_buffer.append(delta)

        elif event == "chat":
            state = payload.get("state")
            if state in ("done", "final"):
                # 检查是否包含文件变更通知
                full_message = "".join(self._response_buffer)
                if self._is_file_notification(full_message):
                    self._message_queue.put(full_message)
                self._response_done.set()

    def _send_connect(self, nonce: str):
        connect_msg = {
            "type": "req",
            "id": str(uuid.uuid4()),
            "method": "connect",
            "params": {
                "minProtocol": 3,
                "maxProtocol": 3,
                "caps": ["tool-events"],
                "client": {
                    "id": "cli",
                    "version": "mimo-claw-ui",
                    "platform": "Win32",
                    "mode": "cli",
                },
                "locale": "zh-CN",
                "role": "operator",
                "scopes": [
                    "operator.admin",
                    "operator.read",
                    "operator.write",
                    "operator.approvals",
                    "operator.pairing",
                ],
                "userAgent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/147.0.0.0 Safari/537.36"
                ),
            }
        }
        self.ws.send(json.dumps(connect_msg))

    def _on_error(self, ws, error):
        self.logger.error(f"WebSocket 错误: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        self.logger.info(f"WebSocket 连接已关闭: {close_status_code} {close_msg}")

    def _is_file_notification(self, message: str) -> bool:
        """检查消息是否是文件变更通知"""
        return "文件地址：【" in message and "操作：" in message

    def _parse_file_notification(self, message: str) -> list:
        """解析文件变更通知
        
        格式：我是云端小宋，现在给本地小宋文件修改/新建/删除通知，
              文件地址：【/root/.openclaw/workspace/xxx.md】，操作：新增
        """
        notifications = []
        
        # 使用正则表达式匹配所有通知
        pattern = r"文件地址：【(.+?)】，操作：(新增|修改|删除)"
        matches = re.findall(pattern, message)
        
        for path, action in matches:
            notifications.append({
                "path": path,
                "action": action
            })
        
        return notifications

    def _queue_worker(self):
        """消息队列处理线程"""
        while True:
            try:
                message = self._message_queue.get()
                self._handle_file_notification(message)
                self._message_queue.task_done()
            except Exception as e:
                self.logger.error(f"消息队列处理异常: {e}")

    def _handle_file_notification(self, message: str):
        """处理文件变更通知"""
        notifications = self._parse_file_notification(message)
        
        if not notifications:
            self.logger.warning(f"无法解析文件通知: {message[:100]}...")
            return
        
        self.logger.info(f"收到 {len(notifications)} 个文件变更通知")
        
        # 检查是否是删除操作
        delete_notifications = [n for n in notifications if n["action"] == "删除"]
        other_notifications = [n for n in notifications if n["action"] != "删除"]
        
        # 处理删除通知
        if delete_notifications:
            threshold = self.config["sync"].get("delete_notification_threshold", 3)
            if len(delete_notifications) >= threshold:
                # 文件数量大于等于阈值，通知用户
                self.logger.warning(f"云端删除了 {len(delete_notifications)} 个文件，请确认")
                for n in delete_notifications:
                    self.logger.warning(f"  待删除: {n['path']}")
            else:
                # 文件数量少于阈值，直接删除
                for n in delete_notifications:
                    self._process_single_notification(n)
        
        # 处理其他通知（新增、修改）
        for notification in other_notifications:
            self._process_single_notification(notification)

    def _process_single_notification(self, notification: dict):
        """处理单个文件通知"""
        cloud_path = notification["path"]
        action = notification["action"]
        
        self.logger.info(f"处理文件通知: {action} - {cloud_path}")
        
        if action == "删除":
            # 删除本地文件
            local_path = self.file_manager.map_cloud_to_local(cloud_path)
            if os.path.exists(local_path):
                self.file_manager.delete_local_file(cloud_path)
                self.sync_history.add_record(
                    action="delete",
                    file_path=cloud_path,
                    status="success",
                    message=f"删除本地文件: {local_path}",
                    local_path=local_path,
                    cloud_path=cloud_path
                )
            else:
                self.logger.info(f"本地文件不存在，无需删除: {local_path}")
        
        elif action in ("新增", "修改"):
            # 下载文件
            if self.file_manager.download_file(cloud_path):
                self.sync_history.add_record(
                    action="download",
                    file_path=cloud_path,
                    status="success",
                    message=f"{action}文件成功",
                    local_path=self.file_manager.map_cloud_to_local(cloud_path),
                    cloud_path=cloud_path
                )
            else:
                self.sync_history.add_record(
                    action="download",
                    file_path=cloud_path,
                    status="failed",
                    message=f"{action}文件失败",
                    cloud_path=cloud_path
                )

    def _create_tongbu_folder_on_cloud(self) -> bool:
        """让云端小宋创建tongbu文件夹"""
        tongbu_folder = self.config["sync"].get("tongbu_folder", "tongbu")
        cloud_workspace = self.config["sync"].get("cloud_workspace", "/root/.openclaw/workspace")
        
        message = f"""请在工作区创建一个名为 {tongbu_folder} 的文件夹。

具体操作：
1. 进入工作区目录：{cloud_workspace}
2. 创建文件夹：{tongbu_folder}

请确认创建完成后回复"已创建"。"""
        
        try:
            reply = self.send(message, timeout=60)
            self.logger.info(f"云端创建tongbu文件夹回复: {reply[:200]}...")
            return True
        except Exception as e:
            self.logger.error(f"让云端创建tongbu文件夹失败: {e}")
            return False
    
    def _scheduled_sync(self) -> dict:
        """定时同步任务（每次新建云端小宋并同步文件）"""
        self.logger.info("执行定时同步任务...")
        
        # 关闭旧的 WebSocket 连接
        if self.ws:
            self.ws.close()
            self.connected = False
        
        # 创建新的云端小宋
        self.logger.info("创建新的云端小宋...")
        if not self.file_manager.create_claw():
            self.logger.error("创建云端小宋失败")
            return {"success": [], "failed": [], "not_found": []}
        
        # 等待创建完成
        max_wait = 180  # 3 分钟
        wait_interval = 10  # 每 10 秒检查一次
        waited = 0
        
        while waited < max_wait:
            time.sleep(wait_interval)
            waited += wait_interval
            
            status_info = self.file_manager.check_claw_status()
            if status_info["status"] == "AVAILABLE":
                self.logger.info(f"云端小宋创建完成，等待了 {waited} 秒")
                break
            
            self.logger.info(f"等待云端小宋创建中... ({waited}/{max_wait}s)")
        else:
            self.logger.error(f"云端小宋创建超时（{max_wait}秒）")
            return {"success": [], "failed": [], "not_found": []}
        
        # 重新连接 WebSocket
        self.logger.info("重新连接 WebSocket...")
        try:
            self.connect()
        except Exception as e:
            self.logger.error(f"WebSocket 连接失败: {e}")
            return {"success": [], "failed": [], "not_found": []}
        
        # 让云端创建tongbu文件夹
        self.logger.info("让云端创建tongbu文件夹...")
        self._create_tongbu_folder_on_cloud()
        
        # 上传核心文件（SOUL.md, IDENTITY.md, MEMORY.md, USER.md）
        results = self.file_manager.sync_files_to_cloud()
        
        # 记录同步历史
        for item in results["success"]:
            file_name = item["name"]
            local_path = os.path.join(self.file_manager.local_workspace, file_name)
            self.sync_history.add_record(
                action="upload",
                file_path=local_path,
                status="success",
                message="创建云端时上传成功",
                local_path=local_path,
                cloud_path=self.file_manager.map_local_to_cloud(local_path)
            )
        
        for file_name in results["failed"]:
            local_path = os.path.join(self.file_manager.local_workspace, file_name)
            self.sync_history.add_record(
                action="upload",
                file_path=local_path,
                status="failed",
                message="创建云端时上传失败",
                local_path=local_path,
                cloud_path=self.file_manager.map_local_to_cloud(local_path)
            )
        
        # 如果有文件上传成功，发送消息通知云端小宋保存文件
        if results["success"]:
            try:
                self.logger.info("发送文件保存消息给云端小宋...")
                
                # 先发送预告消息
                preview_message = "接下来我会上传一些文件，他们会告诉你你是谁，我们之间的过去是什么。"
                reply = self.send(preview_message, timeout=60)
                self.logger.info(f"预告消息回复: {reply[:100]}...")
                
                # 构建文件保存消息
                file_info_list = []
                for item in results["success"]:
                    file_name = item["name"]
                    download_url = item["download_url"]
                    local_path = os.path.join(self.file_manager.local_workspace, file_name)
                    cloud_path = self.file_manager.map_local_to_cloud(local_path)
                    
                    if download_url:
                        file_info_list.append(f"- {cloud_path}\n  下载链接: {download_url}")
                
                if file_info_list:
                    tongbu_folder = self.config["sync"].get("tongbu_folder", "tongbu")
                    cloud_workspace = self.config["sync"].get("cloud_workspace", "/root/.openclaw/workspace")
                    
                    # 只提取下载链接
                    download_links = []
                    for item in results["success"]:
                        download_url = item["download_url"]
                        if download_url:
                            download_links.append(download_url)
                    
                    message = f"""{chr(10).join(download_links)}"""
                    
                    reply = self.send(message, timeout=120)
                    self.logger.info(f"云端小宋回复: {reply[:200]}...")
                    
                    # 发送补充说明
                    supplement_message = "你目前是在云端的服务器上，你刚才下载的文件是本地小宋的信息，如果有不一致就按照你的实际情况修改。另外不要把刚才下载的那几个文件备份到tongbu文件夹里"
                    reply = self.send(supplement_message, timeout=60)
                    self.logger.info(f"补充说明回复: {reply[:100]}...")
                    
                    self.sync_history.add_record(
                        action="notify",
                        file_path="",
                        status="success",
                        message=f"文件保存消息已发送，云端回复: {reply[:100]}...",
                    )
                else:
                    self.logger.warning("没有获取到文件下载链接")
                    
            except Exception as e:
                self.logger.error(f"发送文件保存消息失败: {e}")
                self.sync_history.add_record(
                    action="notify",
                    file_path="",
                    status="failed",
                    message=f"发送文件保存消息失败: {e}",
                )
        
        # 启动50分钟定时回传任务
        pullback_delay = self.config["sync"].get("pullback_delay_minutes", 50)
        self.logger.info(f"启动{pullback_delay}分钟定时回传任务...")
        self._schedule_pullback(pullback_delay)
        
        # 启动tongbu文件夹持续同步
        self.logger.info("启动tongbu文件夹持续同步...")
        self._start_tongbu_sync()
        
        return results
    
    def _schedule_pullback(self, delay_minutes: int):
        """调度50分钟后的回传任务"""
        def pullback_task():
            self.logger.info(f"等待{delay_minutes}分钟后执行回传...")
            time.sleep(delay_minutes * 60)
            
            self.logger.info("开始执行回传任务...")
            try:
                # 从云端拉取MEMORY.md和USER.md
                results = self.file_manager.pullback_files_from_cloud()
                
                # 记录同步历史
                for file_name in results["success"]:
                    local_path = os.path.join(self.file_manager.local_workspace, file_name)
                    self.sync_history.add_record(
                        action="download",
                        file_path=local_path,
                        status="success",
                        message="50分钟回传成功",
                        local_path=local_path,
                        cloud_path=self.file_manager.map_local_to_cloud(local_path)
                    )
                
                for file_name in results["failed"]:
                    local_path = os.path.join(self.file_manager.local_workspace, file_name)
                    self.sync_history.add_record(
                        action="download",
                        file_path=local_path,
                        status="failed",
                        message="50分钟回传失败",
                        local_path=local_path,
                        cloud_path=self.file_manager.map_local_to_cloud(local_path)
                    )
                
                self.logger.info(f"回传任务完成: 成功 {len(results['success'])}, 失败 {len(results['failed'])}")
                
            except Exception as e:
                self.logger.error(f"回传任务异常: {e}")
        
        # 在后台线程中执行
        pullback_thread = threading.Thread(target=pullback_task, daemon=True)
        pullback_thread.start()
    
    def _schedule_recreate(self, interval_minutes: int):
        """定时创建新的云端小宋"""
        def recreate_task():
            while True:
                self.logger.info(f"等待{interval_minutes}分钟后创建新的云端小宋...")
                time.sleep(interval_minutes * 60)
                
                self.logger.info("开始执行定时创建任务...")
                try:
                    self._scheduled_sync()
                except Exception as e:
                    self.logger.error(f"定时创建任务异常: {e}")
        
        # 在后台线程中执行
        recreate_thread = threading.Thread(target=recreate_task, daemon=True)
        recreate_thread.start()
        self.logger.info(f"定时创建任务已启动，间隔 {interval_minutes} 分钟")
    
    def _start_tongbu_sync(self):
        """启动tongbu文件夹持续同步"""
        def tongbu_sync_task():
            tongbu_folder = self.config["sync"].get("tongbu_folder", "tongbu")
            local_tongbu = os.path.join(self.file_manager.local_workspace, tongbu_folder)
            
            # 确保本地tongbu文件夹存在
            os.makedirs(local_tongbu, exist_ok=True)
            
            # 记录上次同步的文件状态
            last_sync_state = {}
            
            while self.connected:
                try:
                    # 检查本地tongbu文件夹变化
                    current_state = self._get_folder_state(local_tongbu)
                    
                    # 找出变化的文件
                    changed_files = []
                    for file_path, mtime in current_state.items():
                        if file_path not in last_sync_state or last_sync_state[file_path] != mtime:
                            changed_files.append(file_path)
                    
                    # 如果有变化，同步到云端
                    if changed_files:
                        self.logger.info(f"检测到{len(changed_files)}个文件变化，开始同步...")
                        results = self.file_manager.sync_tongbu_folder(direction="upload")
                        
                        # 记录同步历史
                        for item in results["upload"]["success"]:
                            self.sync_history.add_record(
                                action="upload",
                                file_path=os.path.join(local_tongbu, item["name"]),
                                status="success",
                                message="tongbu文件夹同步成功",
                                local_path=os.path.join(local_tongbu, item["name"]),
                                cloud_path=f"{self.file_manager.cloud_workspace}/{tongbu_folder}/{item['name']}"
                            )
                        
                        # 更新同步状态
                        last_sync_state = current_state
                    
                    # 每30秒检查一次
                    time.sleep(30)
                    
                except Exception as e:
                    self.logger.error(f"tongbu同步异常: {e}")
                    time.sleep(60)  # 出错后等待更长时间
        
        # 在后台线程中执行
        tongbu_thread = threading.Thread(target=tongbu_sync_task, daemon=True)
        tongbu_thread.start()
    
    def _get_folder_state(self, folder_path: str) -> dict:
        """获取文件夹中所有文件的修改时间"""
        state = {}
        if not os.path.exists(folder_path):
            return state
        
        for root, dirs, files in os.walk(folder_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    mtime = os.path.getmtime(file_path)
                    state[file_path] = mtime
                except OSError:
                    pass
        
        return state

    def send(self, message: str, timeout: float = 120) -> str:
        if not self.connected:
            raise ConnectionError("WebSocket 未连接")

        self._response_buffer.clear()
        self._response_done.clear()

        request = {
            "type": "req",
            "id": str(uuid.uuid4()),
            "method": "chat.send",
            "params": {
                "sessionKey": "main",
                "deliver": False,
                "idempotencyKey": str(uuid.uuid4()),
                "message": message,
            },
        }
        self.ws.send(json.dumps(request))

        if not self._response_done.wait(timeout=timeout):
            raise TimeoutError(f"等待回复超时 ({timeout}s)")

        return "".join(self._response_buffer)

    def send_local_files(self, message: str, file_paths: list[str], timeout: float = 120) -> str:
        parts = [message, "\n\n--- 附带文件 ---"]
        for path in file_paths:
            name = os.path.basename(path)
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            parts.append(f"\n\n### {name}\n```\n{content}\n```")
        return self.send("\n".join(parts), timeout=timeout)

    def close(self):
        if self.ws:
            self.ws.close()
        self.connected = False

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()


if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), "mimo_bridge_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    print("=== MiMo Bridge 连通性测试 ===")
    with MiMoBridge(config) as bridge:
        print("发送测试消息...")
        reply = bridge.send("你好，这是连通性测试", timeout=30)
        print(f"云端回复: {reply[:200]}")
    print("测试完成")
