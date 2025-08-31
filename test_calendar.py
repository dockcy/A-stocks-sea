#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 简单测试交易日历功能
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from adata.stock.info.trade_calendar import TradeCalendar
    import pandas as pd
    from datetime import datetime
    
    print("开始测试交易日历功能...")
    
    # 创建交易日历实例
    trade_calendar = TradeCalendar()
    print("✓ 创建交易日历实例成功")
    
    # 获取2025年的交易日历
    print("获取2025年交易日历...")
    df_2025 = trade_calendar.trade_calendar(year=2025)
    print(f"✓ 2025年交易日历获取成功，共 {len(df_2025)} 条记录")
    print(f"列名: {list(df_2025.columns)}")
    
    # 显示前几条记录
    print("前5条记录:")
    print(df_2025.head())
    
    # 获取当前年份的交易日历
    current_year = datetime.now().year
    print(f"获取{current_year}年交易日历...")
    df_current = trade_calendar.trade_calendar(year=current_year)
    print(f"✓ {current_year}年交易日历获取成功，共 {len(df_current)} 条记录")
    
    # 获取最近的交易日
    print("获取最近的交易日...")
    today = datetime.now().date()
    print(f"今天日期: {today}")
    
    # 筛选交易日（trade_status=1）且日期在今天之前的记录
    trading_days = df_current[
        (df_current['trade_status'] == 1) & 
        (pd.to_datetime(df_current['trade_date']).dt.date < today)
    ].sort_values('trade_date', ascending=False)
    
    if not trading_days.empty:
        latest_trading_date = trading_days.iloc[0]['trade_date']
        print(f"✓ 最近交易日: {latest_trading_date}")
    else:
        print("⚠ 未找到最近的交易日")
        
    print("所有测试通过!")
    
except Exception as e:
    print(f"测试失败: {e}")
    import traceback
    traceback.print_exc()