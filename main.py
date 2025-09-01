#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import asyncio
from datetime import datetime

from utils.config import config
from scheduler.task_scheduler import TaskScheduler
from models.database import DatabaseManager
from data_collectors.stock_info_collector import StockInfoCollector
from data_collectors.market_data_collector import MarketDataCollector
from calculators.indicator_calculator import IndicatorCalculator


def setup_logging():
    """设置全局日志配置"""
    # 读取配置文件

    log_level = config.get('logging', {}).get('level', 'DEBUG')
    log_file = config.get('logging', {}).get('file', 'logs/stocks_strategy') + '_' + datetime.now().strftime(
        '%Y%m%d-%H%M%S') + '.log'

    # 创建日志目录（如果不存在）
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 配置日志格式
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logging.info("全局日志配置完成")


async def run_dev_mode(db_manager):
    """异步开发调试模式"""
    logging.info("开发调试模式启动，执行所有任务...")

    # 1. 更新股票基本信息
    logging.info("执行股票基本信息更新...")
    collector = StockInfoCollector(db_manager)
    await collector.fetch_and_store_stock_info()
    logging.info("股票基本信息更新完成")

    # 2. 更新行情数据
    start_date = config.get("stocks_info").get("start_date")
    logging.info("执行行情数据更新...")
    market_collector = MarketDataCollector(db_manager)
    await market_collector.fetch_and_store_kline_data(start_date)
    logging.info("行情数据更新完成")

    # 3. 计算指标
    logging.info("执行指标计算...")
    calculator = IndicatorCalculator(db_manager)
    await calculator.calculate_indicators()
    logging.info("指标计算完成")

    logging.info("开发调试模式所有任务执行完成")


async def run_update_stock_info(db_manager):
    """异步更新股票基本信息"""
    logging.info("开始更新股票基本信息")
    collector = StockInfoCollector(db_manager)
    await collector.fetch_and_store_stock_info()
    logging.info("股票基本信息更新完成")


async def run_update_market_data(db_manager, start_date):
    """异步更新行情数据"""
    logging.info("开始更新行情数据")
    collector = MarketDataCollector(db_manager)
    await collector.fetch_and_store_kline_data(start_date)
    logging.info("行情数据更新完成")


async def run_calculate_indicators(db_manager):
    """异步计算股票指标"""
    logging.info("开始计算股票指标")
    calculator = IndicatorCalculator(db_manager)
    await calculator.calculate_indicators()
    logging.info("股票指标计算完成")


def main():
    # 设置全局日志
    setup_logging()
    logging.info("程序启动")

    parser = argparse.ArgumentParser(description='股票策略系统')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库')
    parser.add_argument('--update-stock-info', action='store_true', help='更新股票基本信息')
    parser.add_argument('--update-market-data', action='store_true', help='更新行情数据')
    parser.add_argument('--calculate-indicators', action='store_true', help='计算股票指标')
    parser.add_argument('--run-scheduler', action='store_true', help='运行定时任务调度器')
    parser.add_argument('--dev-mode', action='store_true', help='开发调试模式，执行所有任务')

    args = parser.parse_args()
    logging.info(f"命令行参数: {args}")

    # 初始化数据库管理器
    logging.info("初始化数据库管理器")
    db_manager = DatabaseManager()

    start_date = config.get("stocks_info").get("start_date") or None

    # 开发调试模式 - 执行所有任务
    if args.dev_mode:
        asyncio.run(run_dev_mode(db_manager))
        return

    if args.init_db:
        # 数据库初始化已经在DatabaseManager的构造函数中完成
        logging.info("数据库初始化完成")
        print("数据库初始化完成")
        return

    if args.update_stock_info:
        asyncio.run(run_update_stock_info(db_manager))
        return

    if args.update_market_data:
        asyncio.run(run_update_market_data(db_manager, start_date))
        return

    if args.calculate_indicators:
        asyncio.run(run_calculate_indicators(db_manager))
        return

    if args.run_scheduler:
        logging.info("启动定时任务调度器")
        scheduler = TaskScheduler()
        try:
            scheduler.run()
        except KeyboardInterrupt:
            logging.info("调度器已停止")
            print("\n调度器已停止")
        return

    # 如果没有指定任何参数，显示帮助信息
    parser.print_help()


if __name__ == '__main__':
    main()
