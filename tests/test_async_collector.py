#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import asyncio
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.database import DatabaseManager
from data_collectors.stock_info_collector import StockInfoCollector

async def test_async_collector():
    \"\"\"测试异步股票信息收集器\"\"\"
    print(\"开始测试异步股票信息收集器...\")
    
    # 初始化数据库管理器
    print(\"初始化数据库管理器...\")
    db_manager = DatabaseManager()
    
    # 初始化股票信息收集器
    print(\"初始化股票信息收集器...\")
    collector = StockInfoCollector(db_manager)
    
    # 测试异步获取股票信息
    print(\"开始异步获取股票信息...\")
    start_time = time.time()
    
    try:
        await collector.fetch_and_store_stock_info()
        end_time = time.time()
        print(f\"异步获取股票信息完成，耗时: {end_time - start_time:.2f}秒\")
        return True
    except Exception as e:
        print(f\"异步获取股票信息失败: {e}\")
        return False

def main():
    print(\"=== 异步改造测试 ===\")
    
    # 测试异步功能
    result = asyncio.run(test_async_collector())
    
    print(\"\\n=== 测试结果 ===\")
    if result:
        print(\"✓ 异步改造测试通过\")
    else:
        print(\"✗ 异步改造测试失败\")
    
    print(\"\\n注意：此测试仅验证异步调用是否能正常工作，\")
    print(\"实际性能提升取决于网络I/O和API响应时间。\")

if __name__ == '__main__':
    main()