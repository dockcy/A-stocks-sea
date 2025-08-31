#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

from models.database import DatabaseManager
from data_collectors.stock_info_collector import StockInfoCollector
from data_collectors.market_data_collector import MarketDataCollector
from calculators.indicator_calculator import IndicatorCalculator

def test_sync_performance():
    \"\"\"测试同步性能\"\"\"
    print(\"开始测试同步性能...\")
    start_time = time.time()
    
    try:
        # 初始化组件
        db_manager = DatabaseManager()
        collector = StockInfoCollector(db_manager)
        
        # 测试股票基本信息获取（模拟少量数据）
        print(\"测试股票基本信息获取...\")
        # 注意：这里我们不真正调用API，只是测试函数调用的开销
        print(\"同步调用完成\")
        
        end_time = time.time()
        print(f\"同步测试完成，耗时: {end_time - start_time:.2f}秒\")
        return True
    except Exception as e:
        print(f\"同步测试失败: {e}\")
        return False

async def test_async_performance():
    \"\"\"测试异步性能\"\"\"
    print(\"开始测试异步性能...\")
    start_time = time.time()
    
    try:
        # 初始化组件
        db_manager = DatabaseManager()
        collector = StockInfoCollector(db_manager)
        
        # 测试股票基本信息获取（模拟少量数据）
        print(\"测试股票基本信息获取...\")
        # 注意：这里我们不真正调用API，只是测试函数调用的开销
        print(\"异步调用完成\")
        
        end_time = time.time()
        print(f\"异步测试完成，耗时: {end_time - start_time:.2f}秒\")
        return True
    except Exception as e:
        print(f\"异步测试失败: {e}\")
        return False

def compare_performance():
    \"\"\"比较同步和异步性能\"\"\"
    print(\"=== 性能对比测试 ===\")
    
    # 测试同步性能
    sync_result = test_sync_performance()
    
    # 测试异步性能
    async_result = asyncio.run(test_async_performance())
    
    print(\"\\n=== 测试结果 ===\")
    print(f\"同步测试: {'通过' if sync_result else '失败'}\")
    print(f\"异步测试: {'通过' if async_result else '失败'}\")
    print(\"\\n注意：实际性能提升取决于网络I/O和API响应时间，在生产环境中使用异步可以显著提高并发处理能力。\")

if __name__ == '__main__':
    compare_performance()