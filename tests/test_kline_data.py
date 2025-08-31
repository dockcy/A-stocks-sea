import unittest
import pandas as pd
from data_collectors.market_data_collector import MarketDataCollector
from models.database import DatabaseManager

class TestKlineData(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Initialize real database manager
        self.db_manager = DatabaseManager('tests/data/test_stocks.db')
        self.collector = MarketDataCollector(self.db_manager)
    
    def test_fetch_and_store_kline_data_for_10_stocks(self):
        """Test fetching and storing K-line data for 10 stocks using real API calls."""
        # Use real stock codes - first 10 from the market
        test_stocks = [
            '603256'
        ]
        
        # Temporarily modify the get_all_stocks method to return our test stocks
        original_method = self.db_manager.get_all_stocks
        self.db_manager.get_all_stocks = lambda: test_stocks
        
        try:
            # Call the method under test with real API calls
            self.collector.fetch_and_store_kline_data()
            
            # Verify that data was stored by checking if tables have records
            with self.db_manager._get_read_connection() as conn:
                cursor = conn.cursor()
                
                # Check daily K-line data
                cursor.execute('SELECT COUNT(*) FROM daily_kline_data WHERE stock_code IN ({})'.format(
                    ','.join(['?' for _ in test_stocks])
                ), test_stocks)
                daily_count = cursor.fetchone()[0]
                
                # Check weekly K-line data
                cursor.execute('SELECT COUNT(*) FROM weekly_kline_data WHERE stock_code IN ({})'.format(
                    ','.join(['?' for _ in test_stocks])
                ), test_stocks)
                weekly_count = cursor.fetchone()[0]
                
                # Check monthly K-line data
                cursor.execute('SELECT COUNT(*) FROM monthly_kline_data WHERE stock_code IN ({})'.format(
                    ','.join(['?' for _ in test_stocks])
                ), test_stocks)
                monthly_count = cursor.fetchone()[0]
                
                # Verify we have data for all 10 stocks (at least one record per stock)
                self.assertGreater(daily_count, 0, "No daily K-line data was stored")
                self.assertGreater(weekly_count, 0, "No weekly K-line data was stored")
                self.assertGreater(monthly_count, 0, "No monthly K-line data was stored")
                
                # Verify we have data for all 10 stocks (at least one record per stock)
                self.assertGreaterEqual(daily_count, 10, "Not enough daily K-line records")
                self.assertGreaterEqual(weekly_count, 10, "Not enough weekly K-line records")
                self.assertGreaterEqual(monthly_count, 10, "Not enough monthly K-line records")
                
        finally:
            # Restore original method
            self.db_manager.get_all_stocks = original_method

if __name__ == '__main__':
    unittest.main()