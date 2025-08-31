# 股票策略系统

一个基于Python的中国股市数据收集、存储和分析系统，使用`adata`库获取股票数据。

1. **股票基本信息管理**：每月第一个星期一自动更新上证和深证所有股票的基本信息
2. **行情数据收集**：每天17点自动获取所有股票的当日行情数据
3. **技术指标计算**：每天18点自动计算股票的技术指标，包括：
   - 涨跌停判断
   - 连续涨停天数
   - 5/10/20/30/60日均线
   - 可扩展的其他技术指标
4. **数据存储**：使用PostgreSQL数据库存储所有数据
5. **交易日支持**：使用交易日历API确保只在交易日获取数据
6. **并发数据库访问**：支持多读一写的并发访问模式
7. **异步数据处理**：使用异步编程提高数据获取和处理效率

## 系统要求

- Python 3.7+
- PostgreSQL数据库
- Ubuntu 2C2G服务器（推荐）
- Windows 10 + WSL（开发调试）

## 安装步骤

1. 克隆项目代码：
   ```bash
   git clone <项目地址>
   cd stocks_strategy
   ```

2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

3. 配置环境变量：
   在项目根目录创建 `.env` 文件，配置数据库连接信息：
   ```
   POSTGRES_USER=your_username
   POSTGRES_PASSWORD=your_password
   ```

4. 初始化数据库：
   ```bash
   python main.py --init-db
   ```

## 使用方法

### 命令行操作

```bash
# 初始化数据库
python main.py --init-db

# 手动更新股票基本信息
python main.py --update-stock-info

# 手动更新行情数据
python main.py --update-market-data

# 手动计算技术指标
python main.py --calculate-indicators

# 启动定时任务调度器
python main.py --run-scheduler

# 开发调试模式（执行所有任务）
python main.py --dev-mode
```

### 自动运行

推荐使用crontab或systemd服务来自动运行调度器：

```bash
# 使用nohup后台运行
nohup python main.py --run-scheduler &

# 或者使用screen/tmux
screen -dmS stocks_strategy python main.py --run-scheduler
```

## 项目结构

```
stocks_strategy/
├── config.toml         # 配置文件
├── logs/               # 日志文件
├── data_collectors/    # 数据收集器
├── models/             # 数据库模型
├── calculators/        # 指标计算器
├── scheduler/          # 任务调度器
├── utils/              # 工具函数
├── tests/              # 测试文件
├── main.py             # 主程序入口
├── requirements.txt    # 依赖包列表
└── README.md           # 说明文档
```

## 配置说明

配置文件位于 `config.toml`：

```toml
[database]
[database.postgresSQL]
db_url = "localhost:5432"
db_name = "stocks_db_dev"
USER = ""
PASSWORD = ""

[database.dml_config]
batch_size = 10000

[logging]
level = "INFO"
file = "logs/stocks_strategy.log"

[schedule]
# 每月第一个星期一更新股票基本信息
monthly_update = "0 9 * * 1"
# 每天17点获取日涨跌数据
daily_market_update = "0 17 * * 1-5"
# 每天18点计算指标
daily_indicator_calculation = "0 18 * * 1-5"

[api]
# adata API配置
timeout = 30
```

## 数据库表结构

1. **stock_basic_info**：股票基本信息表
2. **daily_market_data**：日涨跌数据表
3. **minute_market_data**：分时行情数据表
4. **daily_kline_data**：日K线数据表
5. **weekly_kline_data**：周K线数据表
6. **monthly_kline_data**：月K线数据表
7. **stock_indicators**：股票指标表

## 开发调试模式

开发调试模式 (`--dev-mode`) 会按顺序执行以下所有任务：
1. 更新股票基本信息
2. 更新行情数据
3. 计算技术指标

这在开发和测试时非常有用，可以快速验证整个数据流程。

## 交易日支持

系统使用 `adata` 的交易日历 API 来获取交易日信息，确保所有数据获取操作都基于实际的交易日进行，避免在非交易日获取无效数据。

## 数据库并发访问

数据库模块支持多读一写的并发访问模式：
- **多读**：多个线程可以同时读取数据库
- **一写**：写操作是独占的，确保数据一致性
- **连接池**：使用PostgreSQL连接池提高并发性能
- **线程安全**：使用可重入锁确保线程安全

## 异步数据处理

为了提高数据获取和处理效率，系统对所有涉及网络请求的操作都进行了异步改造：
- 使用 `asyncio` 和 `concurrent.futures.ThreadPoolExecutor` 包装同步的 `adata` API 调用
- 使用异步并发处理多只股票的数据获取和指标计算
- 通过信号量控制并发数量，避免API过载

## 扩展开发

### 添加新的技术指标

1. 修改 `calculators/indicator_calculator.py` 中的 `_calculate_single_stock_indicators` 方法
2. 在 `models/database.py` 中更新 `stock_indicators` 表结构
3. 更新数据库（可能需要重新初始化）

### 添加新的数据源

1. 在 `data_collectors/` 目录下创建新的数据收集器
2. 在 `scheduler/task_scheduler.py` 中添加相应的定时任务
3. 在 `main.py` 中添加命令行接口

## 注意事项

1. 请遵守股票数据接口的使用规范，避免过于频繁的请求
2. 建议在服务器上部署时使用虚拟环境
3. 定期备份数据库文件
4. 监控日志文件大小，避免占用过多磁盘空间
5. 根据网络环境和API限制调整并发参数

## 许可证

MIT License