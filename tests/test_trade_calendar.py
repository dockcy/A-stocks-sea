#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
from datetime import datetime
from adata.stock.info.trade_calendar import TradeCalendar
import pandas as pd

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

def test_trade_calendar():
    """测试交易日历功能"""
    logging.info("开始测试交易日历功能")
    
    try:
        # 创建交易日历实例
        trade_calendar = TradeCalendar()
        logging.info("创建交易日历实例成功")
        
        # 获取2025年的交易日历
        logging.info("获取2025年交易日历")
        df_2025 = trade_calendar.trade_calendar(year=2025)
        logging.info(f"2025年交易日历获取成功，共 {len(df_2025)} 条记录")
        logging.info(f"列名: {list(df_2025.columns)}")
        logging.info("前5条记录:")
        logging.info(f"\n{df_2025.head()}")
        
        # 获取当前年份的交易日历
        current_year = datetime.now().year
        logging.info(f"获取{current_year}年交易日历")
        df_current = trade_calendar.trade_calendar(year=current_year)
        logging.info(f"{current_year}年交易日历获取成功，共 {len(df_current)} 条记录")
        
        # 获取最近的交易日
        logging.info("获取最近的交易日")
        today = datetime.now().date()
        logging.info(f"今天日期: {today}")
        
        # 筛选交易日（trade_status=1）且日期在今天之前的记录
        trading_days = df_current[
            (df_current['trade_status'] == 1) & 
            (pd.to_datetime(df_current['trade_date']).dt.date < today)
        ].sort_values('trade_date', ascending=False)
        
        if not trading_days.empty:
            latest_trading_date = trading_days.iloc[0]['trade_date']
            logging.info(f"最近交易日: {latest_trading_date}")
        else:
            logging.warning("未找到最近的交易日")
            
        logging.info("交易日历功能测试完成")
        return True
        
    except Exception as e:
        logging.error(f"交易日历功能测试失败: {e}", exc_info=True)
        return False

if __name__ == '__main__':
    # 设置日志
    setup_logging()
    
    logging.info("开始测试adata交易日历功能...")
    print("=" * 50)
    
    if test_trade_calendar():
        logging.info("交易日历功能测试通过!")
        print("交易日历功能测试通过!")
        sys.exit(0)
    else:
        logging.error("交易日历功能测试失败!")
        print("交易日历功能测试失败!")
        sys.exit(1)