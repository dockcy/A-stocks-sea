import logging
import os
import threading
import time
from contextlib import contextmanager

import pandas as pd
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2 import sql
from psycopg2.extras import execute_values, execute_batch

from utils.config import config

# 加载环境变量
load_dotenv()


class DatabaseManager:
    def __init__(self):
        # 读取配置文件获取数据库配置
        
        # 获取PostgreSQL配置
        db_url = config.get('database', {}).get('postgresSQL', {}).get('db_url', 'localhost:5432')
        # 移除引号
        db_url = db_url.strip('"')
        if ':' in db_url:
            self.host, port_str = db_url.split(':')
            self.port = port_str.strip()  # 移除可能的空格和引号
        else:
            self.host = db_url
            self.port = config.get('database', {}).get('postgresSQL', {}).get('port', '5432').strip('"')
        self.database = config.get('database', {}).get('postgresSQL', {}).get('db_name', 'stocks_db_dev').strip('"')
        self.user = os.getenv('POSTGRES_USER', config.get('database', {}).get('postgresSQL', {}).get('USER', 'postgres').strip('"'))
        self.password = os.getenv('POSTGRES_PASSWORD', config.get('database', {}).get('postgresSQL', {}).get('PASSWORD', '').strip('"'))
        
        self._lock = threading.RLock()  # 可重入锁，支持多读一写
        
        # 创建连接池
        self.connection_pool = pool.ThreadedConnectionPool(
            1, 20,  # 最小1个连接，最大20个连接
            host=self.host,
            port=int(self.port),
            database=self.database,
            user=self.user,
            password=self.password
        )
        
        logging.info(f"初始化数据库管理器，连接PostgreSQL: {self.host}:{self.port}/{self.database}")
        self.init_db()
    
    def init_db(self):
        """初始化数据库表结构"""
        logging.info("开始初始化数据库表结构")
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            # 创建A股基表信息表
            # 字段说明:
            # - stock_code: 股票代码 (来自 adata.stock.info.all_code() 接口)
            # - short_name: 股票简称 (来自 adata.stock.info.all_code() 接口)
            # - exchange: 交易所 (来自 adata.stock.info.all_code() 接口)
            # - list_date: 上市日期 (来自 adata.stock.info.all_code() 接口)
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS stock_basic_info (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) UNIQUE NOT NULL,
                    short_name VARCHAR(100) NOT NULL,
                    exchange VARCHAR(10) NOT NULL,
                    list_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format())
            # 添加字段注释
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_basic_info.updated_at IS '更新时间'").format())
            logging.info("创建/检查 stock_basic_info 表完成")
            
            # 创建日涨跌表
            # 字段说明:
            # - stock_code: 股票代码
            # - trade_time: 交易时间
            # - price: 价格
            # - change: 涨跌额
            # - change_pct: 涨跌幅(%)
            # - volume: 成交量
            # - avg_price: 均价
            # - amount: 成交额
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS daily_market_data (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    trade_time TIMESTAMP NOT NULL,
                    price NUMERIC(10,4),
                    change NUMERIC(10,4),
                    change_pct NUMERIC(10,2),
                    volume NUMERIC(15,0),
                    avg_price NUMERIC(10,4),
                    amount NUMERIC(15,0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format())
            # 添加字段注释
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.stock_code IS '股票代码'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.trade_time IS '交易时间'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.price IS '价格'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.change IS '涨跌额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.change_pct IS '涨跌幅(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.volume IS '成交量'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.avg_price IS '均价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.amount IS '成交额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_market_data.created_at IS '创建时间'").format())
            logging.info("创建/检查 daily_market_data 表完成")
            
            # 创建分时行情表
            # 字段说明 (来自 adata.stock.market.get_market() 接口):
            # - stock_code: 股票代码
            # - trade_date: 交易日期
            # - open: 开盘价
            # - close: 收盘价
            # - high: 最高价
            # - low: 最低价
            # - volume: 成交量
            # - amount: 成交额
            # - change_pct: 涨跌幅(%)
            # - change: 涨跌额
            # - turnover_ratio: 换手率(%)
            # - pre_close: 前收盘价
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS minute_market_data (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open NUMERIC(10,4),
                    close NUMERIC(10,4),
                    high NUMERIC(10,4),
                    low NUMERIC(10,4),
                    volume BIGINT,
                    amount NUMERIC(15,0),
                    change_pct NUMERIC(10,2),
                    change NUMERIC(10,4),
                    turnover_ratio NUMERIC(10,2),
                    pre_close NUMERIC(10,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format())
            # 添加字段注释
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.stock_code IS '股票代码'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.trade_date IS '交易日期'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.open IS '开盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.close IS '收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.high IS '最高价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.low IS '最低价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.volume IS '成交量'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.amount IS '成交额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.change_pct IS '涨跌幅(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.change IS '涨跌额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.turnover_ratio IS '换手率(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.pre_close IS '前收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN minute_market_data.created_at IS '创建时间'").format())
            logging.info("创建/检查 minute_market_data 表完成")
            
            # 创建日K线表
            # 字段说明 (来自 adata.stock.market.get_market() 接口, k_type=1):
            # - stock_code: 股票代码
            # - trade_date: 交易日期
            # - open: 开盘价
            # - close: 收盘价
            # - high: 最高价
            # - low: 最低价
            # - volume: 成交量
            # - amount: 成交额
            # - change_pct: 涨跌幅(%)
            # - change: 涨跌额
            # - turnover_ratio: 换手率(%)
            # - pre_close: 前收盘价
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS daily_kline_data (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open NUMERIC(10,4),
                    close NUMERIC(10,4),
                    high NUMERIC(10,4),
                    low NUMERIC(10,4),
                    volume BIGINT,
                    amount NUMERIC(15,0),
                    change_pct NUMERIC(10,2),
                    change NUMERIC(10,4),
                    turnover_ratio NUMERIC(10,2),
                    pre_close NUMERIC(10,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format())
            # 添加字段注释
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.stock_code IS '股票代码'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.trade_date IS '交易日期'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.open IS '开盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.close IS '收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.high IS '最高价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.low IS '最低价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.volume IS '成交量'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.amount IS '成交额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.change_pct IS '涨跌幅(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.change IS '涨跌额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.turnover_ratio IS '换手率(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.pre_close IS '前收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN daily_kline_data.created_at IS '创建时间'").format())
            logging.info("创建/检查 daily_kline_data 表完成")
            
            # 创建周K线表
            # 字段说明 (来自 adata.stock.market.get_market() 接口, k_type=2):
            # - stock_code: 股票代码
            # - trade_date: 交易日期
            # - open: 开盘价
            # - close: 收盘价
            # - high: 最高价
            # - low: 最低价
            # - volume: 成交量
            # - amount: 成交额
            # - change_pct: 涨跌幅(%)
            # - change: 涨跌额
            # - turnover_ratio: 换手率(%)
            # - pre_close: 前收盘价
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS weekly_kline_data (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open NUMERIC(10,4),
                    close NUMERIC(10,4),
                    high NUMERIC(10,4),
                    low NUMERIC(10,4),
                    volume BIGINT,
                    amount NUMERIC(15,0),
                    change_pct NUMERIC(10,2),
                    change NUMERIC(10,4),
                    turnover_ratio NUMERIC(10,2),
                    pre_close NUMERIC(10,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format())
            # 添加字段注释
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.stock_code IS '股票代码'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.trade_date IS '交易日期'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.open IS '开盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.close IS '收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.high IS '最高价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.low IS '最低价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.volume IS '成交量'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.amount IS '成交额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.change_pct IS '涨跌幅(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.change IS '涨跌额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.turnover_ratio IS '换手率(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.pre_close IS '前收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN weekly_kline_data.created_at IS '创建时间'").format())
            logging.info("创建/检查 weekly_kline_data 表完成")
            
            # 创建月K线表
            # 字段说明 (来自 adata.stock.market.get_market() 接口, k_type=3):
            # - stock_code: 股票代码
            # - trade_date: 交易日期
            # - open: 开盘价
            # - close: 收盘价
            # - high: 最高价
            # - low: 最低价
            # - volume: 成交量
            # - amount: 成交额
            # - change_pct: 涨跌幅(%)
            # - change: 涨跌额
            # - turnover_ratio: 换手率(%)
            # - pre_close: 前收盘价
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS monthly_kline_data (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open NUMERIC(10,4),
                    close NUMERIC(10,4),
                    high NUMERIC(10,4),
                    low NUMERIC(10,4),
                    volume BIGINT,
                    amount NUMERIC(15,0),
                    change_pct NUMERIC(10,2),
                    change NUMERIC(10,4),
                    turnover_ratio NUMERIC(10,2),
                    pre_close NUMERIC(10,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format())
            # 添加字段注释
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.stock_code IS '股票代码'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.trade_date IS '交易日期'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.open IS '开盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.close IS '收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.high IS '最高价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.low IS '最低价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.volume IS '成交量'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.amount IS '成交额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.change_pct IS '涨跌幅(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.change IS '涨跌额'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.turnover_ratio IS '换手率(%)'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.pre_close IS '前收盘价'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN monthly_kline_data.created_at IS '创建时间'").format())
            logging.info("创建/检查 monthly_kline_data 表完成")
            
            # 创建股票指标表
            # 字段说明:
            # - stock_code: 股票代码
            # - trade_date: 交易日期
            # - is_limit_up: 是否涨停
            # - is_limit_down: 是否跌停
            # - consecutive_limit_up_days: 连续涨停天数
            # - ma5: 5日均线
            # - ma10: 10日均线
            # - ma20: 20日均线
            # - ma30: 30日均线
            # - ma60: 60日均线
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS stock_indicators (
                    id SERIAL PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    is_limit_up BOOLEAN,
                    is_limit_down BOOLEAN,
                    consecutive_limit_up_days INTEGER DEFAULT 0,
                    ma5 NUMERIC(10,4),
                    ma10 NUMERIC(10,4),
                    ma20 NUMERIC(10,4),
                    ma30 NUMERIC(10,4),
                    ma60 NUMERIC(10,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format())
            # 添加字段注释
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.stock_code IS '股票代码'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.trade_date IS '交易日期'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.is_limit_up IS '是否涨停'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.is_limit_down IS '是否跌停'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.consecutive_limit_up_days IS '连续涨停天数'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.ma5 IS '5日均线'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.ma10 IS '10日均线'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.ma20 IS '20日均线'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.ma30 IS '30日均线'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.ma60 IS '60日均线'").format())
            cursor.execute(sql.SQL("COMMENT ON COLUMN stock_indicators.updated_at IS '更新时间'").format())
            logging.info("创建/检查 stock_indicators 表完成")
            
            # 创建索引以提高查询性能
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_basic_code ON stock_basic_info(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_market_code ON daily_market_data(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_market_time ON daily_market_data(trade_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_minute_market_code ON minute_market_data(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_minute_market_date ON minute_market_data(trade_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_kline_code ON daily_kline_data(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_kline_date ON daily_kline_data(trade_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_weekly_kline_code ON weekly_kline_data(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_weekly_kline_date ON weekly_kline_data(trade_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_monthly_kline_code ON monthly_kline_data(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_monthly_kline_date ON monthly_kline_data(trade_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_indicators_code ON stock_indicators(stock_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_indicators_date ON stock_indicators(trade_date)')
            logging.info("创建索引完成")
            
            # 修改现有表的列定义以解决数值溢出问题
            try:
                # 修改 daily_market_data 表的 change_pct 列
                cursor.execute("ALTER TABLE daily_market_data ALTER COLUMN change_pct TYPE NUMERIC(10,2)")
                logging.info("修改 daily_market_data.change_pct 列定义完成")
            except Exception as e:
                logging.info(f"修改 daily_market_data.change_pct 列定义时出错（可能是已经修改过）: {e}")
            
            try:
                # 修改 minute_market_data 表的 change_pct 和 turnover_ratio 列
                cursor.execute("ALTER TABLE minute_market_data ALTER COLUMN change_pct TYPE NUMERIC(10,2)")
                cursor.execute("ALTER TABLE minute_market_data ALTER COLUMN turnover_ratio TYPE NUMERIC(10,2)")
                logging.info("修改 minute_market_data.change_pct 和 turnover_ratio 列定义完成")
            except Exception as e:
                logging.info(f"修改 minute_market_data 列定义时出错（可能是已经修改过）: {e}")
            
            try:
                # 修改 daily_kline_data 表的 change_pct 和 turnover_ratio 列
                cursor.execute("ALTER TABLE daily_kline_data ALTER COLUMN change_pct TYPE NUMERIC(10,2)")
                cursor.execute("ALTER TABLE daily_kline_data ALTER COLUMN turnover_ratio TYPE NUMERIC(10,2)")
                logging.info("修改 daily_kline_data.change_pct 和 turnover_ratio 列定义完成")
            except Exception as e:
                logging.info(f"修改 daily_kline_data 列定义时出错（可能是已经修改过）: {e}")
            
            try:
                # 修改 weekly_kline_data 表的 change_pct 和 turnover_ratio 列
                cursor.execute("ALTER TABLE weekly_kline_data ALTER COLUMN change_pct TYPE NUMERIC(10,2)")
                cursor.execute("ALTER TABLE weekly_kline_data ALTER COLUMN turnover_ratio TYPE NUMERIC(10,2)")
                logging.info("修改 weekly_kline_data.change_pct 和 turnover_ratio 列定义完成")
            except Exception as e:
                logging.info(f"修改 weekly_kline_data 列定义时出错（可能是已经修改过）: {e}")
                
            try:
                # 修改 monthly_kline_data 表的 change_pct 和 turnover_ratio 列
                cursor.execute("ALTER TABLE monthly_kline_data ALTER COLUMN change_pct TYPE NUMERIC(10,2)")
                cursor.execute("ALTER TABLE monthly_kline_data ALTER COLUMN turnover_ratio TYPE NUMERIC(10,2)")
                logging.info("修改 monthly_kline_data.change_pct 和 turnover_ratio 列定义完成")
            except Exception as e:
                logging.info(f"修改 monthly_kline_data 列定义时出错（可能是已经修改过）: {e}")
            
            conn.commit()
        logging.info("数据库初始化完成")
    
    @contextmanager
    def _get_read_connection(self):
        """获取读数据库连接（支持并发读取）"""
        with self._lock:
            logging.info("获取读数据库连接")
            try:
                conn = self.connection_pool.getconn()
                yield conn
            finally:
                self.connection_pool.putconn(conn)
                logging.info("归还读数据库连接到连接池")
    
    @contextmanager
    def _get_write_connection(self):
        """获取写数据库连接（独占写入）"""
        with self._lock:
            logging.info("获取写数据库连接")
            try:
                conn = self.connection_pool.getconn()
                conn.autocommit = False
                yield conn
            finally:
                self.connection_pool.putconn(conn)
                logging.info("归还写数据库连接到连接池")
    
    def get_connection(self):
        """获取数据库连接（默认写连接）"""
        return self._get_write_connection()
    
    def insert_stock_basic_info(self, stock_data, batch_size=1000):
        """插入股票基本信息，使用批量处理
        Args:
            stock_data: 包含股票基本信息的DataFrame
            batch_size: 批量大小，默认1000
        """
        start_time = time.time()
        logging.info(f"开始插入股票基本信息，数据条数: {len(stock_data)}")
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            inserted_count = 0
            updated_count = 0
            
            # 将数据转换为列表格式以进行批量操作
            data_list = []
            for _, row in stock_data.iterrows():
                list_date = row['list_date']
                if pd.isna(list_date):
                    list_date = None
                data_list.append((row['stock_code'], row['short_name'], row['exchange'], list_date))
            
            # 使用 execute_values 进行批量插入/更新
            for i in range(0, len(data_list), batch_size):
                batch_start_time = time.time()
                batch = data_list[i:i + batch_size]
                
                # 先检查哪些记录需要更新
                stock_codes = [item[0] for item in batch]
                placeholders = ','.join(['%s' for _ in stock_codes])
                cursor.execute(f'SELECT stock_code FROM stock_basic_info WHERE stock_code IN ({placeholders})', stock_codes)
                existing_codes = {row[0] for row in cursor.fetchall()}
                
                # 分离插入和更新的数据
                insert_data = []
                update_data = []
                
                for item in batch:
                    if item[0] in existing_codes:
                        # 更新数据格式: (short_name, exchange, list_date, stock_code)
                        update_data.append((item[1], item[2], item[3], item[0]))
                    else:
                        # 插入数据格式: (stock_code, short_name, exchange, list_date)
                        insert_data.append(item)
                
                # 批量更新
                if update_data:
                    logging.info("Need to update {}".format(len(update_data)))
                    execute_batch(
                        cursor,
                        '''UPDATE stock_basic_info 
                           SET short_name = %s, exchange = %s, list_date = %s, updated_at = CURRENT_TIMESTAMP
                           WHERE stock_code = %s''',
                        update_data
                    )
                    updated_count += len(update_data)
                
                # 批量插入
                if insert_data:
                    logging.info("Need to insert {}".format(len(insert_data)))
                    execute_batch(
                        cursor,
                        '''INSERT INTO stock_basic_info 
                           (stock_code, short_name, exchange, list_date, updated_at)
                           VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)''',
                        insert_data
                    )
                    inserted_count += len(insert_data)
                    batch_time = time.time() - batch_start_time
                    if batch_time > 5:  # 如果批次处理时间超过5秒，记录警告日志
                        logging.warning(f"股票基本信息批量插入耗时较长: {batch_time:.2f}秒, 数据条数: {len(insert_data)}")
                
            conn.commit()
        total_time = time.time() - start_time
        logging.info(f"股票基本信息插入完成: 新增 {inserted_count} 条，更新 {updated_count} 条，总耗时: {total_time:.2f}秒")
    
    def insert_daily_market_data(self, stock_code, market_data, batch_size=1000):
        """插入日涨跌数据，使用批量处理
        Args:
            stock_code: 股票代码
            market_data: 包含日涨跌数据的DataFrame
            batch_size: 批量大小，默认1000
        """
        logging.info(f"开始插入日涨跌数据，股票代码: {stock_code}，数据条数: {len(market_data)}")
        
        if market_data.empty:
            logging.warning(f"股票 {stock_code} 的日涨跌数据为空")
            return
            
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            # 将数据转换为列表格式以进行批量操作
            data_list = []
            for _, row in market_data.iterrows():
                data_list.append((stock_code, row['trade_time'], row['price'], row['change'], 
                                row['change_pct'], row['volume'], row['avg_price'], row['amount']))
            
            inserted_count = 0
            
            # 使用 execute_values 进行批量插入
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                execute_values(
                    cursor,
                    '''INSERT INTO daily_market_data 
                       (stock_code, trade_time, price, change, change_pct, volume, avg_price, amount)
                       VALUES %s''',
                    batch
                )
                inserted_count += len(batch)
            
            conn.commit()
        logging.info(f"股票 {stock_code} 的日涨跌数据插入完成，共 {inserted_count} 条")
    
    def insert_minute_market_data(self, stock_code, minute_data, batch_size=1000):
        """插入分时行情数据，使用批量处理
        Args:
            stock_code: 股票代码
            minute_data: 包含分时行情数据的DataFrame
            batch_size: 批量大小，默认1000
        """
        start_time = time.time()
        logging.info(f"开始插入分时行情数据，股票代码: {stock_code}，数据条数: {len(minute_data)}")
        
        if minute_data.empty:
            logging.warning(f"股票 {stock_code} 的分时行情数据为空")
            return
            
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            # 将数据转换为列表格式以进行批量操作
            data_list = []
            for _, row in minute_data.iterrows():
                data_list.append((stock_code, row['trade_date'], row['open'], row['close'], row['high'], 
                                row['low'], row['volume'], row['amount'], row['change_pct'], 
                                row['change'], row['turnover_ratio'], row['pre_close']))
            
            inserted_count = 0
            
            # 使用 executemany 进行批量插入
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                execute_values(
                    cursor,
                    '''INSERT INTO minute_market_data 
                       (stock_code, trade_date, open, close, high, low, volume, amount, 
                        change_pct, change, turnover_ratio, pre_close)
                       VALUES %s''',
                    batch
                )
                inserted_count += len(batch)
            
            conn.commit()
        logging.info(f"股票 {stock_code} 的分时行情数据插入完成，共 {inserted_count} 条")
    
    def insert_stock_indicators(self, indicators_data, batch_size=1000):
        """插入股票指标数据，使用批量处理
        Args:
            indicators_data: 包含股票指标数据的DataFrame
            batch_size: 批量大小，默认1000
        """
        logging.info(f"开始插入股票指标数据，数据条数: {len(indicators_data)}")
        
        if indicators_data.empty:
            logging.warning("股票指标数据为空")
            return
            
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            inserted_count = 0
            updated_count = 0
            
            # 将数据转换为列表格式以进行批量操作
            data_list = []
            for _, row in indicators_data.iterrows():
                data_list.append((row['stock_code'], row['trade_date'], row['is_limit_up'], row['is_limit_down'], 
                                row['consecutive_limit_up_days'], row['ma5'], row['ma10'], row['ma20'], 
                                row['ma30'], row['ma60']))
            
            # 使用 execute_values 进行批量插入/更新
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                
                # 先检查哪些记录需要更新
                stock_codes = [item[0] for item in batch]
                trade_dates = [item[1] for item in batch]
                
                # 构建更安全的SQL查询
                if stock_codes and trade_dates and len(stock_codes) == len(trade_dates):
                    # 使用参数化查询构建条件
                    conditions = []
                    params = []
                    for i in range(len(stock_codes)):
                        conditions.append("(stock_code = %s AND trade_date = %s)")
                        params.extend([stock_codes[i], trade_dates[i]])
                    
                    condition_str = " OR ".join(conditions)
                    query = f"SELECT stock_code, trade_date FROM stock_indicators WHERE {condition_str}"
                    cursor.execute(query, params)
                    existing_records = {(row[0], row[1]) for row in cursor.fetchall()}
                else:
                    # 如果数据不完整，直接查询所有记录
                    existing_records = set()
                
                # 分离插入和更新的数据
                insert_data = []
                update_data = []
                
                for item in batch:
                    if (item[0], item[1]) in existing_records:
                        # 更新数据格式: (is_limit_up, is_limit_down, consecutive_limit_up_days, ma5, ma10, ma20, ma30, ma60, stock_code, trade_date)
                        update_data.append((*item[2:], item[0], item[1]))
                    else:
                        # 插入数据格式: (stock_code, trade_date, is_limit_up, is_limit_down, consecutive_limit_up_days, ma5, ma10, ma20, ma30, ma60)
                        insert_data.append(item)
                
                # 批量更新
                if update_data:
                    execute_batch(
                        cursor,
                        '''UPDATE stock_indicators 
                           SET is_limit_up = %s, is_limit_down = %s, consecutive_limit_up_days = %s,
                               ma5 = %s, ma10 = %s, ma20 = %s, ma30 = %s, ma60 = %s, updated_at = CURRENT_TIMESTAMP
                           WHERE stock_code = %s AND trade_date = %s''',
                        update_data
                    )
                    updated_count += len(update_data)
                
                # 批量插入
                if insert_data:
                    execute_values(
                        cursor,
                        '''INSERT INTO stock_indicators 
                           (stock_code, trade_date, is_limit_up, is_limit_down, consecutive_limit_up_days,
                            ma5, ma10, ma20, ma30, ma60, updated_at)
                           VALUES %s''',
                        insert_data,
                        template='(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)'
                    )
                    inserted_count += len(insert_data)
                
            conn.commit()
        logging.info(f"股票指标数据插入完成: 新增 {inserted_count} 条，更新 {updated_count} 条")
    
    def insert_daily_kline_data(self, stock_code, kline_data, batch_size=1000):
        """插入日K线数据，使用批量处理
        Args:
            stock_code: 股票代码
            kline_data: 包含日K线数据的DataFrame
            batch_size: 批量大小，默认1000
        """
        logging.info(f"开始插入日K线数据，股票代码: {stock_code}，数据条数: {len(kline_data)}")
        
        if kline_data.empty:
            logging.warning(f"股票 {stock_code} 的日K线数据为空")
            return
            
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            # 将数据转换为列表格式以进行批量操作
            data_list = []
            for _, row in kline_data.iterrows():
                data_list.append((stock_code, row['trade_date'], row['open'], row['close'], row['high'], 
                                row['low'], row['volume'], row['amount'], row['change_pct'], 
                                row['change'], row['turnover_ratio'], row['pre_close']))
            
            inserted_count = 0
            
            # 使用 execute_values 进行批量插入
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                execute_values(
                    cursor,
                    '''INSERT INTO daily_kline_data 
                       (stock_code, trade_date, open, close, high, low, volume, amount, 
                        change_pct, change, turnover_ratio, pre_close)
                       VALUES %s''',
                    batch
                )
                inserted_count += len(batch)
            
            conn.commit()
        logging.info(f"股票 {stock_code} 的日K线数据插入完成，共 {inserted_count} 条")
    
    def insert_weekly_kline_data(self, stock_code, kline_data, batch_size=1000):
        """插入周K线数据，使用批量处理
        Args:
            stock_code: 股票代码
            kline_data: 包含周K线数据的DataFrame
            batch_size: 批量大小，默认1000
        """
        logging.info(f"开始插入周K线数据，股票代码: {stock_code}，数据条数: {len(kline_data)}")
        
        if kline_data.empty:
            logging.warning(f"股票 {stock_code} 的周K线数据为空")
            return
            
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            # 将数据转换为列表格式以进行批量操作
            data_list = []
            for _, row in kline_data.iterrows():
                data_list.append((stock_code, row['trade_date'], row['open'], row['close'], row['high'], 
                                row['low'], row['volume'], row['amount'], row['change_pct'], 
                                row['change'], row['turnover_ratio'], row['pre_close']))
            
            inserted_count = 0
            
            # 使用 execute_values 进行批量插入
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                execute_values(
                    cursor,
                    '''INSERT INTO weekly_kline_data 
                       (stock_code, trade_date, open, close, high, low, volume, amount, 
                        change_pct, change, turnover_ratio, pre_close)
                       VALUES %s''',
                    batch
                )
                inserted_count += len(batch)
            
            conn.commit()
        logging.info(f"股票 {stock_code} 的周K线数据插入完成，共 {inserted_count} 条")
    
    def insert_monthly_kline_data(self, stock_code, kline_data, batch_size=1000):
        """插入月K线数据，使用批量处理
        Args:
            stock_code: 股票代码
            kline_data: 包含月K线数据的DataFrame
            batch_size: 批量大小，默认1000
        """
        logging.info(f"开始插入月K线数据，股票代码: {stock_code}，数据条数: {len(kline_data)}")
        
        if kline_data.empty:
            logging.warning(f"股票 {stock_code} 的月K线数据为空")
            return
            
        with self._get_write_connection() as conn:
            cursor = conn.cursor()
            
            # 将数据转换为列表格式以进行批量操作
            data_list = []
            for _, row in kline_data.iterrows():
                data_list.append((stock_code, row['trade_date'], row['open'], row['close'], row['high'], 
                                row['low'], row['volume'], row['amount'], row['change_pct'], 
                                row['change'], row['turnover_ratio'], row['pre_close']))
            
            inserted_count = 0
            
            # 使用 execute_values 进行批量插入
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                execute_values(
                    cursor,
                    '''INSERT INTO monthly_kline_data 
                       (stock_code, trade_date, open, close, high, low, volume, amount, 
                        change_pct, change, turnover_ratio, pre_close)
                       VALUES %s''',
                    batch
                )
                inserted_count += len(batch)
            
            conn.commit()
        logging.info(f"股票 {stock_code} 的月K线数据插入完成，共 {inserted_count} 条")
    
    def get_all_stocks(self):
        """获取所有股票代码"""
        logging.info("开始获取所有股票代码")
        with self._get_read_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT stock_code FROM stock_basic_info')
            stocks = [row[0] for row in cursor.fetchall()]
        logging.info(f"获取到 {len(stocks)} 只股票代码")
        return stocks

    def get_latest_trade_date(self, stock_code: str) -> str:
        """获取指定股票在日K线表中的最新交易日
        Args:
            stock_code: 股票代码
        Returns:
            最新交易日，格式为YYYY-MM-DD，如果没有数据则返回None
        """
        logging.info(f"开始查询股票 {stock_code} 在日K线表中的最新交易日")
        with self._get_read_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT MAX(trade_date) FROM daily_kline_data WHERE stock_code = %s',
                (stock_code,)
            )
            result = cursor.fetchone()[0]
            latest_date = result.strftime('%Y-%m-%d') if result else None
            logging.info(f"股票 {stock_code} 在日K线表中的最新交易日: {latest_date}")
            return latest_date

    def get_latest_weekly_trade_date(self, stock_code: str) -> str:
        """获取指定股票在周K线表中的最新交易日
        Args:
            stock_code: 股票代码
        Returns:
            最新交易日，格式为YYYY-MM-DD，如果没有数据则返回None
        """
        logging.info(f"开始查询股票 {stock_code} 在周K线表中的最新交易日")
        with self._get_read_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT MAX(trade_date) FROM weekly_kline_data WHERE stock_code = %s',
                (stock_code,)
            )
            result = cursor.fetchone()[0]
            latest_date = result.strftime('%Y-%m-%d') if result else None
            logging.info(f"股票 {stock_code} 在周K线表中的最新交易日: {latest_date}")
            return latest_date

    def get_latest_monthly_trade_date(self, stock_code: str) -> str:
        """获取指定股票在月K线表中的最新交易日
        Args:
            stock_code: 股票代码
        Returns:
            最新交易日，格式为YYYY-MM-DD，如果没有数据则返回None
        """
        logging.info(f"开始查询股票 {stock_code} 在月K线表中的最新交易日")
        with self._get_read_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT MAX(trade_date) FROM monthly_kline_data WHERE stock_code = %s',
                (stock_code,)
            )
            result = cursor.fetchone()[0]
            latest_date = result.strftime('%Y-%m-%d') if result else None
            logging.info(f"股票 {stock_code} 在月K线表中的最新交易日: {latest_date}")
            return latest_date

    def get_stock_history_for_indicators(self, stock_code: str, days: int = 60) -> pd.DataFrame:
        """获取指定股票用于指标计算的历史行情数据
        Args:
            stock_code: 股票代码
            days: 获取的天数，默认60天
        Returns:
            包含历史行情数据的DataFrame
        """
        logging.info(f"开始获取股票 {stock_code} 的历史行情数据用于指标计算，天数: {days}")
        with self._get_read_connection() as conn:
            cursor = conn.cursor()
            # 获取最近60个交易日的数据，按交易日期降序排列
            cursor.execute(
                '''SELECT trade_date, open, close, high, low, volume, amount, change, change_pct
                   FROM daily_kline_data 
                   WHERE stock_code = %s 
                   ORDER BY trade_date DESC 
                   LIMIT %s''',
                (stock_code, days)
            )
            
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            # 获取数据
            data = cursor.fetchall()
            
            # 创建DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # 转换数据类型
            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                # 按交易日期升序排列，以便计算移动平均线
                df = df.sort_values('trade_date').reset_index(drop=True)
                
            logging.info(f"获取到股票 {stock_code} 的历史行情数据 {len(df)} 条")
            return df