import logging
import pandas as pd
import asyncio
import concurrent.futures
from adata import stock
from pandas import DataFrame

from models.database import DatabaseManager
from utils.config import config

class StockInfoCollector:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        logging.info("初始化股票信息收集器")
        "DROP TABLE IF EXISTS stock_info CASCADE"
    
    async def fetch_and_store_stock_info(self):
        """异步获取并存储所有A股股票基本信息"""
        try:
            logging.info("开始获取所有A股股票基本信息...")
            logging.info("调用 adata.stock.info.all_code() 获取股票代码信息")
            
            # 使用线程池执行同步调用
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                stocks_df:DataFrame = await loop.run_in_executor(executor, stock.info.all_code)
            
            logging.info(f"获取到原始股票数据，共 {len(stocks_df)} 条记录")

            stocks_df = stocks_df.dropna(subset=['list_date'])
            logging.info(f"过滤不存在上市日期的数据，剩余{len(stocks_df)}条记录")

            if stocks_df.empty:
                logging.warning("未获取到任何股票信息")
                return

            logging.info(f"共获取到 {len(stocks_df)} 只上证和深证股票")
            
            # 存储到数据库
            logging.info("开始存储股票基本信息到数据库")
            # 数据库操作也是同步的，也需要在线程池中执行
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                await loop.run_in_executor(
                    executor, 
                    self.db_manager.insert_stock_basic_info, 
                    stocks_df, 
                    config.get('database', {}).get('dml_config', {}).get('batch_size', 10000)
                )
            logging.info("股票基本信息存储完成")
            
            logging.info("股票基本信息存储完成")
            
        except Exception as e:
            logging.error(f"获取股票基本信息时出错: {e}", exc_info=True)
            raise