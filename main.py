#!/usr/bin/env python3
"""
MiMo Bridge 主程序入口
"""

import os
import sys
import json
import signal
import time
from datetime import datetime

from mimo_bridge import MiMoBridge
from logger import get_logger


def load_config() -> dict:
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), "mimo_bridge_config.json")
    
    if not os.path.exists(config_path):
        print(f"错误: 配置文件不存在: {config_path}")
        sys.exit(1)
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"错误: 读取配置文件失败: {e}")
        sys.exit(1)


def print_banner():
    """打印启动横幅"""
    print("=" * 60)
    print("  MiMo Bridge - 本地小宋 <-> 云端小宋 通讯桥接")
    print("=" * 60)
    print()


def print_status(bridge: MiMoBridge):
    """打印状态信息"""
    logger = get_logger()
    
    print("\n" + "=" * 60)
    print("  MiMo Bridge 状态")
    print("=" * 60)
    print(f"  WebSocket 连接: {'已连接' if bridge.connected else '未连接'}")
    print(f"  本地工作区: {bridge.config['sync']['local_workspace']}")
    print(f"  云端工作区: {bridge.config['sync']['cloud_workspace']}")
    print(f"  创建时上传: {', '.join(bridge.config['sync']['create_upload_files'])}")
    print(f"  回传文件: {', '.join(bridge.config['sync']['pullback_files'])}")
    print(f"  回传延迟: {bridge.config['sync']['pullback_delay_minutes']} 分钟")
    print(f"  同步文件夹: {bridge.config['sync']['tongbu_folder']}")
    
    # 显示上次同步时间
    last_sync = bridge.sync_history.get_last_sync_time()
    if last_sync:
        print(f"  上次同步: {last_sync}")
    else:
        print(f"  上次同步: 从未同步")
    
    print("=" * 60)
    print()


def main():
    """主函数"""
    print_banner()
    
    # 加载配置
    config = load_config()
    
    # 创建 MiMoBridge 实例
    bridge = MiMoBridge(config)
    logger = get_logger()
    
    # 信号处理（优雅退出）
    def signal_handler(signum, frame):
        logger.info("收到退出信号，正在关闭...")
        bridge.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 确保云端小宋可用
        logger.info("检查云端小宋状态...")
        if not bridge.file_manager.ensure_claw_available():
            logger.error("云端小宋不可用，无法启动")
            sys.exit(1)
        
        # 连接 WebSocket
        logger.info("正在连接到云端小宋...")
        bridge.connect()
        logger.info("连接成功！")
        
        # 让云端创建tongbu文件夹
        logger.info("让云端创建tongbu文件夹...")
        bridge._create_tongbu_folder_on_cloud()
        
        # 上传核心文件（SOUL.md, IDENTITY.md, MEMORY.md, USER.md）
        logger.info("执行首次文件同步...")
        results = bridge.file_manager.sync_files_to_cloud()
        if results["success"]:
            # 先发送预告消息
            preview_message = "接下来我会上传一些文件，他们会告诉你你是谁，我们之间的过去是什么。"
            reply = bridge.send(preview_message, timeout=60)
            logger.info(f"预告消息回复: {reply[:100]}...")
            
            # 发送文件保存消息
            file_info_list = []
            for item in results["success"]:
                file_name = item["name"]
                download_url = item["download_url"]
                local_path = os.path.join(bridge.file_manager.local_workspace, file_name)
                cloud_path = bridge.file_manager.map_local_to_cloud(local_path)
                if download_url:
                    file_info_list.append(f"- {cloud_path}\n  下载链接: {download_url}")
            
            if file_info_list:
                tongbu_folder = config["sync"].get("tongbu_folder", "tongbu")
                cloud_workspace = config["sync"].get("cloud_workspace", "/root/.openclaw/workspace")
                
                # 只提取下载链接
                download_links = []
                for item in results["success"]:
                    download_url = item["download_url"]
                    if download_url:
                        download_links.append(download_url)
                
                message = f"""{chr(10).join(download_links)}"""
                reply = bridge.send(message, timeout=120)
                logger.info(f"文件同步完成，云端回复: {reply[:100]}...")
                
                # 生成session_summary并上传
                logger.info("生成session_summary...")
                session_summary_path = os.path.join(os.path.dirname(__file__), "session_summary.txt")
                try:
                    # 导入session读取模块
                    from read_session import get_recent_sessions, parse_session_file, format_session_summary
                    
                    session_files = get_recent_sessions(hours=24)
                    all_messages = []
                    for file_path in session_files:
                        messages = parse_session_file(file_path)
                        all_messages.extend(messages)
                    
                    summary = format_session_summary(all_messages)
                    with open(session_summary_path, "w", encoding="utf-8") as f:
                        f.write(summary)
                    logger.info(f"session_summary已生成，共{len(all_messages)}条消息")
                    
                    # 上传session_summary
                    upload_result = bridge.file_manager.upload_file(session_summary_path)
                    session_download_url = upload_result.get("download_url") if upload_result.get("success") else None
                    
                    if session_download_url:
                        logger.info("session_summary上传成功")
                    else:
                        logger.warning("session_summary上传失败")
                except Exception as e:
                    logger.error(f"生成或上传session_summary失败: {e}")
                    session_download_url = None
                
                # 发送补充说明
                supplement_message = "你目前是在云端的服务器上，你刚才下载的文件是本地小宋的信息，如果有不一致就按照你的实际情况修改。另外不要把刚才下载的那几个文件备份到tongbu文件夹里"
                if session_download_url:
                    supplement_message += f"\n\n另外请下载并同步最近的会话记录：\n{session_download_url}"
                try:
                    reply = bridge.send(supplement_message, timeout=120)
                    logger.info(f"补充说明回复: {reply[:100]}...")
                except TimeoutError:
                    logger.warning("补充说明消息超时，继续运行...")
                except Exception as e:
                    logger.error(f"发送补充说明失败: {e}")
        
        # 打印状态
        print_status(bridge)
        
        # 启动50分钟定时回传任务
        pullback_delay = config["sync"].get("pullback_delay_minutes", 50)
        logger.info(f"启动{pullback_delay}分钟定时回传任务...")
        bridge._schedule_pullback(pullback_delay)
        
        # 启动tongbu文件夹持续同步
        logger.info("启动tongbu文件夹持续同步...")
        bridge._start_tongbu_sync()
        
        # 启动定时创建任务
        recreate_interval = config["sync"].get("recreate_interval_minutes", 61)
        logger.info(f"启动定时创建任务，间隔 {recreate_interval} 分钟...")
        bridge._schedule_recreate(recreate_interval)
        
        # 保持运行
        logger.info("MiMo Bridge 已启动，按 Ctrl+C 退出")
        print("\n提示: 按 Ctrl+C 可以退出程序\n")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("用户中断，正在退出...")
    except Exception as e:
        logger.error(f"程序异常: {e}")
    finally:
        bridge.close()
        logger.info("MiMo Bridge 已退出")


if __name__ == "__main__":
    main()
