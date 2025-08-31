#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import logging

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """设置测试日志配置"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    logging.info("测试日志配置完成")

def test_imports():
    """测试所有模块导入"""
    logging.info("开始测试模块导入")
    
    try:
        from models.database import DatabaseManager
        logging.info("DatabaseManager 导入成功")
        print("✓ DatabaseManager 导入成功")
        
        from data_collectors.stock_info_collector import StockInfoCollector
        logging.info("StockInfoCollector 导入成功")
        print("✓ StockInfoCollector 导入成功")
        
        from data_collectors.market_data_collector import MarketDataCollector
        logging.info("MarketDataCollector 导入成功")
        print("✓ MarketDataCollector 导入成功")
        
        from calculators.indicator_calculator import IndicatorCalculator
        logging.info("IndicatorCalculator 导入成功")
        print("✓ IndicatorCalculator 导入成功")
        
        from scheduler.task_scheduler import TaskScheduler
        logging.info("TaskScheduler 导入成功")
        print("✓ TaskScheduler 导入成功")
        
        logging.info("所有模块导入测试通过!")
        return True
        
    except Exception as e:
        logging.error(f"导入测试失败: {e}", exc_info=True)
        print(f"✗ 导入测试失败: {e}")
        return False

def test_directory_structure():
    """测试目录结构"""
    logging.info("开始测试目录结构")
    
    required_dirs = ['models', 'data_collectors', 'calculators', 'scheduler', 'config', 'data', 'logs']
    required_files = [
        'models/database.py',
        'data_collectors/stock_info_collector.py',
        'data_collectors/market_data_collector.py',
        'calculators/indicator_calculator.py',
        'scheduler/task_scheduler.py',
        'config/config.ini',
        'main.py',
        'requirements.txt'
    ]
    
    logging.info("检查必要目录是否存在")
    for directory in required_dirs:
        if os.path.exists(directory) and os.path.isdir(directory):
            logging.info(f"目录 {directory} 存在")
            print(f"✓ 目录 {directory} 存在")
        else:
            logging.error(f"目录 {directory} 不存在")
            print(f"✗ 目录 {directory} 不存在")
            return False
    
    logging.info("检查必要文件是否存在")
    for file in required_files:
        if os.path.exists(file) and os.path.isfile(file):
            logging.info(f"文件 {file} 存在")
            print(f"✓ 文件 {file} 存在")
        else:
            logging.error(f"文件 {file} 不存在")
            print(f"✗ 文件 {file} 不存在")
            return False
    
    logging.info("目录结构检查通过!")
    return True

if __name__ == '__main__':
    # 设置日志
    setup_logging()
    
    logging.info("开始测试股票策略系统...")
    print("=" * 40)
    
    dir_test_passed = test_directory_structure()
    import_test_passed = test_imports()
    
    print("=" * 40)
    
    if dir_test_passed and import_test_passed:
        logging.info("所有测试通过! 项目已准备就绪。")
        print("所有测试通过! 项目已准备就绪。")
        sys.exit(0)
    else:
        logging.error("测试失败! 请检查错误信息。")
        print("测试失败! 请检查错误信息。")
        sys.exit(1)