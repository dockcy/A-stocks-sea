#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.database import DatabaseManager
from data_collectors.stock_info_collector import StockInfoCollector
from data_collectors.market_data_collector import MarketDataCollector
from calculators.indicator_calculator import IndicatorCalculator

def test_database():
    """测试数据库连接和初始化"""
    print("测试数据库连接...")
    try:
        db = DatabaseManager()
        print("数据库连接成功")
        return True
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return False

def test_create_directories():
    """测试创建必要的目录"""
    print("创建必要的目录...")
    directories = ['data', 'logs', 'config']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"创建目录: {directory}")
        else:
            print(f"目录已存在: {directory}")

if __name__ == '__main__':
    print("开始测试股票策略系统...")
    
    # 测试创建目录
    test_create_directories()
    
    # 测试数据库
    if test_database():
        print("所有测试通过!")
    else:
        print("测试失败!")
        sys.exit(1)