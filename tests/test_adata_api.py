import unittest

import adata
from adata.stock.info.concept.stock_concept_ths import StockConceptThs


class TestAdataAPI(unittest.TestCase):
    def test_day_kline(self):
        result = adata.stock.market.get_market(stock_code = '000001',  start_date = '2025-08-18', end_date=None, k_type=1,
                   adjust_type = 1)
        print(result)

    def test_index_constituent_ths_by_index_code(self):
        concept = StockConceptThs()
        data = concept.concept_constituent_ths(name='医药')
        print(data)

    def test_index_ma5(self):
        result = adata.stock.market.baidu_index.get_market_index()
        print(result)
