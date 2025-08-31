import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
import concurrent.futures
from adata.stock.info.trade_calendar import TradeCalendar
from models.database import DatabaseManager

class IndicatorCalculator:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.trade_calendar = TradeCalendar()
        logging.info("初始化指标计算器")
    
    async def calculate_indicators(self):
        """异步计算所有股票的指标"""
        try:
            logging.info("开始计算所有股票的指标...")
            logging.info("从数据库获取所有股票代码")
            
            # 获取所有股票代码
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                stock_codes = await loop.run_in_executor(executor, self.db_manager.get_all_stocks)
            logging.info(f"从数据库获取到 {len(stock_codes)} 只股票代码")
            
            if not stock_codes:
                logging.warning("数据库中没有股票信息")
                return
            
            # 获取最近的交易日
            trade_date = self._get_latest_trading_date()
            if not trade_date:
                logging.error("无法获取有效的交易日")
                return
                
            logging.info(f"共需要计算 {len(stock_codes)} 只股票的指标，交易日: {trade_date}")
            
            # 使用异步并发处理多只股票
            tasks = []
            for i, stock_code in enumerate(stock_codes):
                task = self._calculate_single_stock_indicators_async(stock_code, trade_date, i, len(stock_codes))
                tasks.append(task)

            # 并发执行所有任务，限制并发数量以避免数据库连接过载
            semaphore = asyncio.Semaphore(20)  # 限制并发数为20
            
            async def safe_calculate(task):
                async with semaphore:
                    return await task
                    
            results = await asyncio.gather(*[safe_calculate(task) for task in tasks], return_exceptions=True)
            
            # 收集有效的指标数据
            indicators_data = []
            success_count = 0
            fail_count = 0
            
            for result in results:
                if isinstance(result, Exception):
                    fail_count += 1
                    logging.error(f"计算股票指标时出错: {result}")
                elif result is not None:
                    indicators_data.append(result)
                    success_count += 1
                else:
                    fail_count += 1
            
            # 将指标数据存储到数据库
            if indicators_data:
                logging.info(f"开始存储 {len(indicators_data)} 条指标数据到数据库")
                df = pd.DataFrame(indicators_data)
                # 数据库操作也是同步的，也需要在线程池中执行
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(executor, self.db_manager.insert_stock_indicators, df)
                logging.info("指标数据存储完成")
            else:
                logging.warning("没有指标数据需要存储")
            
            logging.info(f"指标计算完成，共处理了 {len(indicators_data)} 只股票: 成功 {success_count} 只，失败 {fail_count} 只")
            logging.info(f"成功率: {success_count/len(stock_codes)*100:.1f}%")
            
        except Exception as e:
            logging.error(f"计算指标时出错: {e}", exc_info=True)
            raise
    
    async def _calculate_single_stock_indicators_async(self, stock_code, trade_date, index, total_count):
        """异步计算单只股票的指标"""
        try:
            logging.info(f"[{index+1}/{total_count}] 开始计算股票 {stock_code} 的指标")
            
            # 从数据库获取该股票的历史行情数据（最近60天）
            logging.info(f"从数据库获取股票 {stock_code} 的历史行情数据")
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                history_data = await loop.run_in_executor(
                    executor, 
                    self.db_manager.get_stock_history_for_indicators, 
                    stock_code, 
                    60  # 获取最近60天的数据
                )
            
            if history_data is None or history_data.empty:
                logging.warning(f"股票 {stock_code} 没有历史行情数据")
                return None
            
            # 计算各种指标
            logging.info(f"开始计算股票 {stock_code} 的各项指标")
            
            # 计算移动平均线
            ma5 = self._calculate_ma(history_data['close'], 5)
            ma10 = self._calculate_ma(history_data['close'], 10)
            ma20 = self._calculate_ma(history_data['close'], 20)
            ma30 = self._calculate_ma(history_data['close'], 30)
            ma60 = self._calculate_ma(history_data['close'], 60)
            
            # 计算是否涨停跌停（基于最新一天的数据）
            latest_data = history_data.iloc[-1]  # 最新的一天
            pre_close = latest_data['close'] - latest_data['change']  # 前收盘价 = 收盘价 - 涨跌额
            
            # 确保数据类型一致，将decimal转换为float
            if hasattr(pre_close, 'item'):
                pre_close = float(pre_close.item())
            else:
                pre_close = float(pre_close)
            
            # 根据股票代码确定涨跌幅限制
            # 30****、68**** 开头的股票涨跌幅限制为20%
            # 8**** 开头的股票涨跌幅限制为30%
            # 其他股票涨跌幅限制为10%
            if stock_code.startswith('30') or stock_code.startswith('68'):
                limit_percent = 0.2  # 20%
            elif stock_code.startswith('8'):
                limit_percent = 0.3  # 30%
            else:
                limit_percent = 0.1  # 10%
            
            limit_up_threshold = round(pre_close * (1 + limit_percent), 2)
            limit_down_threshold = round(pre_close * (1 - limit_percent), 2)
            
            is_limit_up = latest_data['close'] >= limit_up_threshold
            is_limit_down = latest_data['close'] <= limit_down_threshold
            
            # 计算连续涨停天数（简化实现，实际需要更复杂的逻辑）
            consecutive_limit_up_days = 0
            if is_limit_up:
                # 简单检查最近几天是否都是涨停
                for i in range(len(history_data) - 1, -1, -1):
                    day_data = history_data.iloc[i]
                    day_pre_close = day_data['close'] - day_data['change']
                    # 确保数据类型一致，将decimal转换为float
                    if hasattr(day_pre_close, 'item'):
                        day_pre_close = float(day_pre_close.item())
                    else:
                        day_pre_close = float(day_pre_close)
                    
                    # 根据股票代码确定涨跌幅限制
                    # 30****、68**** 开头的股票涨跌幅限制为20%
                    # 8**** 开头的股票涨跌幅限制为30%
                    # 其他股票涨跌幅限制为10%
                    if stock_code.startswith('30') or stock_code.startswith('68'):
                        limit_percent = 0.2  # 20%
                    elif stock_code.startswith('8'):
                        limit_percent = 0.3  # 30%
                    else:
                        limit_percent = 0.1  # 10%
                    
                    day_limit_up_threshold = round(day_pre_close * (1 + limit_percent), 2)
                    if day_data['close'] >= day_limit_up_threshold:
                        consecutive_limit_up_days += 1
                    else:
                        break
            
            indicator = {
                'stock_code': stock_code,
                'trade_date': trade_date,
                'is_limit_up': is_limit_up,
                'is_limit_down': is_limit_down,
                'consecutive_limit_up_days': consecutive_limit_up_days,
                'ma5': ma5,
                'ma10': ma10,
                'ma20': ma20,
                'ma30': ma30,
                'ma60': ma60
            }
            
            logging.info(f"股票 {stock_code} 指标计算完成")
            return indicator
            
        except Exception as e:
            logging.error(f"计算股票 {stock_code} 指标时出错: {e}", exc_info=True)
            return None
    
    def _calculate_single_stock_indicators(self, stock_code, trade_date):
        """计算单只股票的指标（同步版本，供向后兼容）"""
        try:
            logging.info(f"开始计算股票 {stock_code} 的单只指标，交易日: {trade_date}")
            
            # 从数据库获取该股票的历史行情数据（最近60天）
            logging.info(f"从数据库获取股票 {stock_code} 的历史行情数据")
            history_data = self.db_manager.get_stock_history_for_indicators(stock_code, 60)
            
            if history_data is None or history_data.empty:
                logging.warning(f"股票 {stock_code} 没有历史行情数据")
                return None
            
            # 计算各种指标
            logging.info(f"开始计算股票 {stock_code} 的各项指标")
            
            # 计算移动平均线
            ma5 = self._calculate_ma(history_data['close'], 5)
            ma10 = self._calculate_ma(history_data['close'], 10)
            ma20 = self._calculate_ma(history_data['close'], 20)
            ma30 = self._calculate_ma(history_data['close'], 30)
            ma60 = self._calculate_ma(history_data['close'], 60)
            
            # 计算是否涨停跌停（基于最新一天的数据）
            latest_data = history_data.iloc[-1]  # 最新的一天
            pre_close = latest_data['close'] - latest_data['change']  # 前收盘价 = 收盘价 - 涨跌额
            
            # 确保数据类型一致，将decimal转换为float
            if hasattr(pre_close, 'item'):
                pre_close = float(pre_close.item())
            else:
                pre_close = float(pre_close)
            
            # 根据股票代码确定涨跌幅限制
            # 30****、68**** 开头的股票涨跌幅限制为20%
            # 8**** 开头的股票涨跌幅限制为30%
            # 其他股票涨跌幅限制为10%
            if stock_code.startswith('30') or stock_code.startswith('68'):
                limit_percent = 0.2  # 20%
            elif stock_code.startswith('8'):
                limit_percent = 0.3  # 30%
            else:
                limit_percent = 0.1  # 10%
            
            limit_up_threshold = round(pre_close * (1 + limit_percent), 2)
            limit_down_threshold = round(pre_close * (1 - limit_percent), 2)
            
            is_limit_up = latest_data['close'] >= limit_up_threshold
            is_limit_down = latest_data['close'] <= limit_down_threshold
            
            # 计算连续涨停天数（简化实现，实际需要更复杂的逻辑）
            consecutive_limit_up_days = 0
            if is_limit_up:
                # 简单检查最近几天是否都是涨停
                for i in range(len(history_data) - 1, -1, -1):
                    day_data = history_data.iloc[i]
                    day_pre_close = day_data['close'] - day_data['change']
                    # 确保数据类型一致，将decimal转换为float
                    if hasattr(day_pre_close, 'item'):
                        day_pre_close = float(day_pre_close.item())
                    else:
                        day_pre_close = float(day_pre_close)
                    
                    # 根据股票代码确定涨跌幅限制
                    # 30****、68**** 开头的股票涨跌幅限制为20%
                    # 8**** 开头的股票涨跌幅限制为30%
                    # 其他股票涨跌幅限制为10%
                    if stock_code.startswith('30') or stock_code.startswith('68'):
                        limit_percent = 0.2  # 20%
                    elif stock_code.startswith('8'):
                        limit_percent = 0.3  # 30%
                    else:
                        limit_percent = 0.1  # 10%
                    
                    day_limit_up_threshold = round(day_pre_close * (1 + limit_percent), 2)
                    if day_data['close'] >= day_limit_up_threshold:
                        consecutive_limit_up_days += 1
                    else:
                        break
            
            indicator = {
                'stock_code': stock_code,
                'trade_date': trade_date,
                'is_limit_up': is_limit_up,
                'is_limit_down': is_limit_down,
                'consecutive_limit_up_days': consecutive_limit_up_days,
                'ma5': ma5,
                'ma10': ma10,
                'ma20': ma20,
                'ma30': ma30,
                'ma60': ma60
            }
            
            logging.info(f"股票 {stock_code} 指标计算完成")
            return indicator
            
        except Exception as e:
            logging.error(f"计算股票 {stock_code} 指标时出错: {e}", exc_info=True)
            return None
    
    def _calculate_ma(self, prices, days):
        """计算移动平均线"""
        logging.info(f"计算 {days} 日移动平均线，数据点数: {len(prices)}")
        
        if len(prices) < days:
            logging.info(f"数据点数不足 {days} 个，无法计算 {days} 日移动平均线")
            return None
            
        ma_value = round(prices.tail(days).mean(), 2)
        logging.info(f"{days} 日移动平均线计算结果: {ma_value}")
        return ma_value
    
    def _get_latest_trading_date(self):
        """获取最近的交易日"""
        try:
            logging.info("开始获取最近的交易日")
            
            # 获取当前年份的交易日历
            current_year = datetime.now().year
            calendar_df = self.trade_calendar.trade_calendar(year=current_year)
            logging.info(f"获取到 {current_year} 年的交易日历，共 {len(calendar_df)} 条记录")
            
            # 获取今天之前的最近交易日
            today = datetime.now().date()
            logging.info(f"今天日期: {today}")
            
            # 筛选交易日（trade_status=1）且日期在今天之前的记录
            trading_days = calendar_df[
                (calendar_df['trade_status'] == 1) & 
                (pd.to_datetime(calendar_df['trade_date']).dt.date < today)
            ].sort_values('trade_date', ascending=False)
            
            if not trading_days.empty:
                latest_trading_date = trading_days.iloc[0]['trade_date']
                logging.info(f"最近交易日: {latest_trading_date}")
                return latest_trading_date
            else:
                # 如果当前年份没有找到交易日，尝试上一年
                logging.info("当前年份未找到交易日，尝试上一年")
                last_year = current_year - 1
                last_calendar_df = self.trade_calendar.trade_calendar(year=last_year)
                last_trading_days = last_calendar_df[
                    (last_calendar_df['trade_status'] == 1)
                ].sort_values('trade_date', ascending=False)
                
                if not last_trading_days.empty:
                    latest_trading_date = last_trading_days.iloc[0]['trade_date']
                    logging.info(f"最近交易日: {latest_trading_date}")
                    return latest_trading_date
                else:
                    logging.warning("无法找到最近的交易日")
                    return None
                    
        except Exception as e:
            logging.error(f"获取最近交易日时出错: {e}", exc_info=True)
            # 回退到使用昨天的日期
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            logging.info(f"回退到昨天的日期: {yesterday}")
            return yesterday