import schedule
import time
import logging
import asyncio
import threading
from datetime import datetime
from utils.config import config
from models.database import DatabaseManager
from data_collectors.stock_info_collector import StockInfoCollector
from data_collectors.market_data_collector import MarketDataCollector
from calculators.indicator_calculator import IndicatorCalculator

class TaskScheduler:
    def __init__(self):
        logging.info("初始化任务调度器")
        
        # 初始化数据库管理器
        logging.info("初始化数据库管理器，连接PostgreSQL")
        self.db_manager = DatabaseManager()
        
        # 初始化数据收集器和计算器
        logging.info("初始化数据收集器和计算器")
        self.stock_info_collector = StockInfoCollector(self.db_manager)
        self.market_data_collector = MarketDataCollector(self.db_manager)
        self.indicator_calculator = IndicatorCalculator(self.db_manager)
        
        # 设置定时任务
        logging.info("开始设置定时任务")
        self._setup_schedules()
        logging.info("定时任务设置完成")
    
    def _run_async_task(self, coro, task_name):
        """运行异步任务的通用方法"""
        logging.info(f"开始执行{task_name}...")
        try:
            def run_async():
                asyncio.run(coro)
            
            thread = threading.Thread(target=run_async)
            thread.start()
            thread.join()  # 等待完成
            logging.info(f"{task_name}执行完成")
        except Exception as e:
            logging.error(f"执行{task_name}时出错: {e}", exc_info=True)
    
    def _setup_schedules(self):
        """设置定时任务"""
        logging.info("配置定时任务调度规则")
        
        # 每月第一个星期一更新股票基本信息
        # 使用自定义检查函数来实现每月第一个星期一的逻辑
        schedule.every().monday.at("09:00").do(self._check_and_update_stock_info)
        logging.info("配置股票基本信息更新任务: 每月第一个星期一 09:00")
        
        # 每天17点获取日涨跌数据
        schedule.every().day.at("17:00").do(self.update_daily_market_data)
        logging.info("配置日涨跌数据更新任务: 每天 17:00")
        
        # 每天18点计算指标
        schedule.every().day.at("18:00").do(self.calculate_indicators)
        logging.info("配置指标计算任务: 每天 18:00")
        
        logging.info("定时任务设置完成")
    
    def _check_and_update_stock_info(self):
        """检查是否是每月第一个星期一，如果是则更新股票信息"""
        logging.info("检查是否为每月第一个星期一")
        today = datetime.now()
        # 如果是本月的前7天中的星期一，则执行更新
        if today.day <= 7:
            logging.info("今天是每月第一个星期一，执行股票基本信息更新任务")
            self.update_stock_info()
        else:
            logging.info("今天不是每月第一个星期一，跳过股票基本信息更新任务")
    
    def update_stock_info(self):
        """更新股票基本信息"""
        self._run_async_task(
            self.stock_info_collector.fetch_and_store_stock_info(),
            "股票基本信息更新任务"
        )
    
    def update_daily_market_data(self):
        """更新每日行情数据"""
        self._run_async_task(
            self.market_data_collector.fetch_and_store_kline_data(),
            "每日行情数据更新任务"
        )
    
    def calculate_indicators(self):
        """计算股票指标"""
        self._run_async_task(
            self.indicator_calculator.calculate_indicators(),
            "股票指标计算任务"
        )
    
    def run(self):
        """运行调度器"""
        logging.info("股票策略系统调度器启动")
        logging.info("开始循环检查定时任务")
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
                logging.info("定时任务检查完成，等待下次检查")
            except KeyboardInterrupt:
                logging.info("收到键盘中断信号，调度器停止")
                raise
            except Exception as e:
                logging.error(f"调度器运行时出错: {e}", exc_info=True)
                time.sleep(60)  # 出错后等待1分钟再继续