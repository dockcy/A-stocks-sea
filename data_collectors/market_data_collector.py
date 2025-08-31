import logging
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import concurrent.futures
from adata import stock
from adata.stock.info.trade_calendar import TradeCalendar
from models.database import DatabaseManager
from tqdm import tqdm


class MarketDataCollector:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.trade_calendar = TradeCalendar()
        logging.info("初始化行情数据收集器")
        # stock.market.wencai_hexin_v()

    async def fetch_and_store_kline_data(self, start_date: str = None, end_date: str = None):
        """异步获取并存储所有股票的日、周、月K线数据"""
        try:
            logging.info("开始获取所有股票的日、周、月K线数据...")
            logging.info("从数据库获取所有股票代码")

            # 获取所有股票代码
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                stock_codes = await loop.run_in_executor(executor, self.db_manager.get_all_stocks)
            
            # 过滤掉以900开头的股票代码
            stock_codes = [code for code in stock_codes if not code.startswith('900')]
            logging.info(f"从数据库获取到 {len(stock_codes)} 只股票代码（已过滤掉以900开头的股票）")

            if not stock_codes:
                logging.warning("数据库中没有股票信息")
                return

            # 获取最近的交易日
            trade_date = self._get_latest_trading_date()
            if not trade_date:
                logging.error("无法获取有效的交易日")
                return

            logging.info(f"共需要获取 {len(stock_codes)} 只股票的行情数据，交易日: {trade_date}")

            success_count = 0
            fail_count = 0
            empty_data_count = 0

            # 使用异步并发处理多只股票，添加进度条
            tasks = []
            for i, stock_code in enumerate(stock_codes):
                task = self._fetch_and_store_single_stock_data(
                    stock_code, start_date, end_date, i, len(stock_codes)
                )
                tasks.append(task)

            # 并发执行所有任务，限制并发数量以避免API过载
            semaphore = asyncio.Semaphore(10)  # 限制并发数为10
            
            async def safe_fetch(task):
                async with semaphore:
                    return await task
                    
            # 使用tqdm显示进度条
            results = []
            safe_tasks = [safe_fetch(task) for task in tasks]
            for task in tqdm(asyncio.as_completed(safe_tasks), 
                            total=len(safe_tasks), 
                            desc="获取股票K线数据"):
                try:
                    result = await task
                    results.append(result)
                except Exception as e:
                    results.append(e)
            
            # 统计结果
            for result in results:
                if isinstance(result, Exception):
                    fail_count += 1
                    logging.error(f"处理股票数据时出错: {result}")
                elif result == "success":
                    success_count += 1
                elif result == "empty":
                    empty_data_count += 1
                    fail_count += 1

            logging.info(f"行情数据获取完成: 成功 {success_count} 只，失败 {fail_count} 只，空数据 {empty_data_count} 只")
            logging.info(f"成功率: {success_count / len(stock_codes) * 100:.1f}%")

        except Exception as e:
            logging.error(f"获取行情数据时出错: {e}", exc_info=True)
            raise

    async def _fetch_and_store_single_stock_data(self, stock_code, start_date, end_date, index, total_count):
        """异步获取并存储单只股票的数据"""
        try:
            logging.info(f"[{index + 1}/{total_count}] 开始获取股票 {stock_code} 的日、周、月K线数据")

            # 确定日K线的起始日期
            daily_start_date = start_date
            if not daily_start_date:
                # 如果没有传入起始日期，则从日K线表中获取最新日期
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    latest_daily_date = await loop.run_in_executor(
                        executor, self.db_manager.get_latest_trade_date, stock_code
                    )
                if latest_daily_date:
                    # 从最新日期的次日开始获取数据
                    next_date = (datetime.strptime(latest_daily_date, '%Y-%m-%d') + timedelta(days=1)).strftime(
                        '%Y-%m-%d')
                    daily_start_date = next_date
                else:
                    # 如果没有历史数据，则从默认日期开始
                    daily_start_date = '2010-01-01'

            # 获取该股票的日K线数据 (k_type=1)
            logging.info(f"开始获取日K数据,起始日期:{daily_start_date}")
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                daily_kline = await loop.run_in_executor(
                    executor, 
                    lambda: stock.market.get_market(
                        stock_code=stock_code, 
                        start_date=daily_start_date,
                        end_date=end_date, 
                        k_type=1, 
                        adjust_type=1
                    )
                )
            logging.info(f"股票 {stock_code} 日K线数据获取完成")

            # 确定周K线的起始日期
            weekly_start_date = start_date
            if not weekly_start_date:
                # 如果没有传入起始日期，则从周K线表中获取最新日期
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    latest_weekly_date = await loop.run_in_executor(
                        executor, self.db_manager.get_latest_weekly_trade_date, stock_code
                    )
                if latest_weekly_date:
                    # 从最新日期的次日开始获取数据
                    next_date = (
                            datetime.strptime(latest_weekly_date, '%Y-%m-%d') + timedelta(days=1)).strftime(
                        '%Y-%m-%d')
                    weekly_start_date = next_date
                else:
                    # 如果没有历史数据，则从默认日期开始
                    weekly_start_date = '2010-01-01'

            # 获取该股票的周K线数据 (k_type=2)
            logging.info(f"开始获取周K数据,起始日期:{weekly_start_date}")
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                weekly_kline = await loop.run_in_executor(
                    executor,
                    lambda: stock.market.get_market(
                        stock_code=stock_code, 
                        start_date=weekly_start_date,
                        end_date=end_date, 
                        k_type=2, 
                        adjust_type=1
                    )
                )
            logging.info(f"股票 {stock_code} 周K线数据获取完成")

            # 确定月K线的起始日期
            monthly_start_date = start_date
            if not monthly_start_date:
                # 如果没有传入起始日期，则从月K线表中获取最新日期
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    latest_monthly_date = await loop.run_in_executor(
                        executor, self.db_manager.get_latest_monthly_trade_date, stock_code
                    )
                if latest_monthly_date:
                    # 从最新日期的次日开始获取数据
                    next_date = (datetime.strptime(latest_monthly_date, '%Y-%m-%d') + timedelta(
                        days=1)).strftime('%Y-%m-%d')
                    monthly_start_date = next_date
                else:
                    # 如果没有历史数据，则从默认日期开始
                    monthly_start_date = '2010-01-01'

            # 获取该股票的月K线数据 (k_type=3)
            logging.info(f"开始获取月K数据,起始日期:{monthly_start_date}")
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                monthly_kline = await loop.run_in_executor(
                    executor,
                    lambda: stock.market.get_market(
                        stock_code=stock_code, 
                        start_date=monthly_start_date,
                        end_date=end_date, 
                        k_type=3, 
                        adjust_type=1
                    )
                )
            logging.info(f"股票 {stock_code} 月K线数据获取完成")

            # 存储日K线数据
            if daily_kline is not None and not daily_kline.empty:
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(
                        executor, 
                        self.db_manager.insert_daily_kline_data, 
                        stock_code, 
                        daily_kline
                    )
                logging.info(f"股票 {stock_code} 日K线数据存储完成")
            else:
                logging.warning(f"股票 {stock_code} 未获取到日K线数据")
                return "empty"

            # 存储周K线数据
            if weekly_kline is not None and not weekly_kline.empty:
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(
                        executor, 
                        self.db_manager.insert_weekly_kline_data, 
                        stock_code, 
                        weekly_kline
                    )
                logging.info(f"股票 {stock_code} 周K线数据存储完成")
            else:
                logging.warning(f"股票 {stock_code} 未获取到周K线数据")
                return "empty"

            # 存储月K线数据
            if monthly_kline is not None and not monthly_kline.empty:
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(
                        executor, 
                        self.db_manager.insert_monthly_kline_data, 
                        stock_code, 
                        monthly_kline
                    )
                logging.info(f"股票 {stock_code} 月K线数据存储完成")
            else:
                logging.warning(f"股票 {stock_code} 未获取到月K线数据")
                return "empty"

            return "success"

        except Exception as e:
            logging.error(f"获取股票 {stock_code} 行情数据时出错: {e}", exc_info=True)
            raise

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
