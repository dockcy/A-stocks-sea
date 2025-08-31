import adata
import pandas as pd

def analyze_stock_info_all_code():
    """分析 stock.info.all_code() 方法的返回结构"""
    print("=== 分析 stock.info.all_code() 方法 ===")
    try:
        # 获取股票基本信息
        stocks_df = adata.stock.info.all_code()
        print(f"返回数据类型: {type(stocks_df)}")
        print(f"数据形状: {stocks_df.shape}")
        print(f"列名: {list(stocks_df.columns)}")
        
        # 显示前几行数据
        print("\n前5行数据:")
        print(stocks_df.head())
        
        # 显示每列的数据类型
        print("\n各列数据类型:")
        print(stocks_df.dtypes)
        
        # 显示一些统计信息
        print("\n数据统计信息:")
        print(stocks_df.describe())
        
        return stocks_df
    except Exception as e:
        print(f"调用 stock.info.all_code() 出错: {e}")
        return None

def analyze_stock_market_get_market():
    """分析 stock.market.get_market() 方法的返回结构"""
    print("\n=== 分析 stock.market.get_market() 方法 ===")
    try:
        # 获取某只股票的市场数据（以平安银行为例）
        stock_code = '000001'  # 平安银行
        market_df = adata.stock.market.get_market(stock_code=stock_code, start_date='2025-08-01', 
                                                 end_date=None, k_type=1, adjust_type=1)
        print(f"返回数据类型: {type(market_df)}")
        print(f"数据形状: {market_df.shape}")
        print(f"列名: {list(market_df.columns)}")
        
        # 显示前几行数据
        print("\n前5行数据:")
        print(market_df.head())
        
        # 显示每列的数据类型
        print("\n各列数据类型:")
        print(market_df.dtypes)
        
        # 显示一些统计信息
        print("\n数据统计信息:")
        print(market_df.describe())
        
        return market_df
    except Exception as e:
        print(f"调用 stock.market.get_market() 出错: {e}")
        return None

def analyze_weekly_monthly_data():
    """分析周K线和月K线数据结构"""
    print("\n=== 分析周K线数据 ===")
    try:
        # 获取周K线数据
        stock_code = '000001'
        weekly_df = adata.stock.market.get_market(stock_code=stock_code, start_date='2025-01-01', 
                                                 end_date=None, k_type=2, adjust_type=1)
        print(f"周K线数据形状: {weekly_df.shape}")
        print(f"周K线列名: {list(weekly_df.columns)}")
        print("周K线前3行:")
        print(weekly_df.head(3))
    except Exception as e:
        print(f"获取周K线数据出错: {e}")
    
    print("\n=== 分析月K线数据 ===")
    try:
        # 获取月K线数据
        stock_code = '000001'
        monthly_df = adata.stock.market.get_market(stock_code=stock_code, start_date='2025-01-01', 
                                                  end_date=None, k_type=3, adjust_type=1)
        print(f"月K线数据形状: {monthly_df.shape}")
        print(f"月K线列名: {list(monthly_df.columns)}")
        print("月K线前3行:")
        print(monthly_df.head(3))
    except Exception as e:
        print(f"获取月K线数据出错: {e}")

if __name__ == "__main__":
    # 分析股票基本信息
    stock_info_df = analyze_stock_info_all_code()
    
    # 分析市场数据
    market_df = analyze_stock_market_get_market()
    
    # 分析周K线和月K线
    analyze_weekly_monthly_data()
    
    print("\n=== 分析完成 ===")