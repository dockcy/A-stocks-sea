#!/bin/bash
# 启动股票策略系统

# 检查Python环境
if ! command -v python3 &> /dev/null
then
    echo "未找到Python3，请先安装Python3"
    exit 1
fi

# 检查依赖
if [ ! -f "requirements.txt" ]; then
    echo "未找到requirements.txt文件"
    exit 1
fi

# 安装依赖
echo "安装依赖包..."
pip install -r requirements.txt

# 检查数据目录
if [ ! -d "data" ]; then
    echo "创建数据目录..."
    mkdir -p data
fi

# 检查日志目录
if [ ! -d "logs" ]; then
    echo "创建日志目录..."
    mkdir -p logs
fi

# 初始化数据库
echo "初始化数据库..."
python main.py --init-db

# 启动调度器
echo "启动定时任务调度器..."
python main.py --run-scheduler