#!/usr/bin/env python3
"""
Session 信息读取工具
读取24小时内的session记录，整理成简短格式
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

SESSIONS_DIR = r"C:\Users\卿颜\.openclaw\agents\main\sessions"


def get_recent_sessions(hours=24):
    """获取指定小时数内的session文件"""
    # 使用带时区的当前时间
    from datetime import timezone
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    recent_files = []
    sessions_path = Path(SESSIONS_DIR)
    
    for file_path in sessions_path.glob("*.jsonl"):
        # 跳过已删除和已重置的文件
        if ".deleted." in file_path.name or ".reset." in file_path.name:
            continue
        
        # 检查文件内容中的时间戳
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                first_line = f.readline().strip()
                if first_line:
                    data = json.loads(first_line)
                    file_timestamp = data.get("timestamp", "")
                    if file_timestamp:
                        # 解析 ISO 格式时间戳
                        file_time = datetime.fromisoformat(file_timestamp.replace("Z", "+00:00"))
                        # 直接比较UTC时间
                        if file_time >= cutoff_time:
                            recent_files.append(file_path)
        except Exception:
            # 如果解析失败，跳过该文件
            continue
    
    return sorted(recent_files, key=lambda f: f.stat().st_mtime)


def parse_session_file(file_path):
    """解析session文件，提取消息"""
    messages = []
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            try:
                data = json.loads(line)
                
                # 只处理消息类型
                if data.get("type") != "message":
                    continue
                
                message = data.get("message", {})
                role = message.get("role", "")
                timestamp = data.get("timestamp", "")
                content = message.get("content", [])
                
                # 提取文本内容
                text_parts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        text_parts.append(item.get("text", ""))
                    elif isinstance(item, str):
                        text_parts.append(item)
                
                if text_parts:
                    text = "\n".join(text_parts)
                    # 截断过长的内容
                    if len(text) > 200:
                        text = text[:200] + "..."
                    
                    messages.append({
                        "role": role,
                        "text": text,
                        "timestamp": timestamp
                    })
            
            except json.JSONDecodeError:
                continue
    
    return messages


def format_session_summary(messages):
    """将消息整理成简短格式"""
    if not messages:
        return "无对话记录"
    
    lines = []
    current_date = None
    
    for msg in messages:
        # 解析时间戳
        ts_str = msg.get("timestamp", "")
        if ts_str:
            try:
                # 处理 ISO 格式时间戳
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                # 转换为本地时间
                local_ts = ts + timedelta(hours=8)  # 假设是 UTC+8
                time_str = local_ts.strftime("%H:%M")
                date_str = local_ts.strftime("%Y-%m-%d")
                
                # 如果日期变化，添加日期标题
                if date_str != current_date:
                    current_date = date_str
                    lines.append(f"\n📜 {date_str} 的对话记录")
                    lines.append("")
            except:
                time_str = "??:??"
        else:
            time_str = "??:??"
        
        # 格式化消息
        role = msg.get("role", "")
        text = msg.get("text", "")
        
        # 简化角色名称
        if role == "user":
            role_name = "卿颜"
        elif role == "assistant":
            role_name = "小宋"
        else:
            role_name = role
        
        # 截断过长的文本
        if len(text) > 100:
            text = text[:100] + "..."
        
        # 移除换行符，保持简洁
        text = text.replace("\n", " ").strip()
        
        lines.append(f"{time_str} — {role_name}：{text}")
    
    return "\n".join(lines)


def main():
    """主函数"""
    print("=" * 50)
    print("  Session 信息读取工具")
    print("=" * 50)
    print()
    
    # 获取24小时内的session文件
    session_files = get_recent_sessions(hours=24)
    print(f"找到 {len(session_files)} 个24小时内的session文件")
    print()
    
    # 解析所有session文件
    all_messages = []
    for file_path in session_files:
        messages = parse_session_file(file_path)
        all_messages.extend(messages)
    
    print(f"共 {len(all_messages)} 条消息")
    print()
    
    # 整理成简短格式
    summary = format_session_summary(all_messages)
    
    # 保存到文件
    output_path = os.path.join(os.path.dirname(__file__), "session_summary.txt")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(summary)
    print(f"已保存到: {output_path}")
    
    # 尝试打印到控制台（处理编码问题）
    try:
        print(summary)
    except UnicodeEncodeError:
        print("（控制台编码不支持，已保存到文件）")


if __name__ == "__main__":
    main()
