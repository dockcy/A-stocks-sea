@echo off
REM 启动股票策略系统 (Windows版本)

REM 检查Python环境
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo 未找到Python，请先安装Python
    pause
    exit /b 1
)

REM 检查依赖
if not exist "requirements.txt" (
    echo 未找到requirements.txt文件
    pause
    exit /b 1
)

REM 安装依赖
echo 安装依赖包...
pip install -r requirements.txt

REM 检查数据目录
if not exist "data" (
    echo 创建数据目录...
    mkdir data
)

REM 检查日志目录
if not exist "logs" (
    echo 创建日志目录...
    mkdir logs
)

REM 初始化数据库
echo 初始化数据库...
python main.py --init-db

REM 启动调度器
echo 启动定时任务调度器...
python main.py --run-scheduler

pause