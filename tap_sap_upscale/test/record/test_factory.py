import unittest

from tap_sap_upscale.record.factory import build_record_handler
from tap_sap_upscale.record.record import Record

from tap_sap_upscale.record.handler.category_handler import CategoryHandler
from tap_sap_upscale.record.handler.customer_specific_price_handler import CustomerSpecificPriceHandler
from tap_sap_upscale.record.handler.price_point_handler import PricePointHandler
from tap_sap_upscale.record.handler.product_handler import ProductHandler
from tap_sap_upscale.record.handler.product_spec_handler import ProductSpecHandler
from tap_sap_upscale.record.handler.spec_handler import SpecHandler
from tap_sap_upscale.record.handler.stock_point_handler import StockPointHandler


class TestFactory(unittest.TestCase):

    def test_should_build_category_handler(self):
        category_handler = build_record_handler(Record.CATEGORY)
        self.assertTrue(isinstance(category_handler, CategoryHandler))

    def test_should_build_customer_specific_price_handler(self):
        customer_specific_price_handler = build_record_handler(Record.CUSTOMER_SPECIFIC_PRICE)
        self.assertTrue(isinstance(customer_specific_price_handler, CustomerSpecificPriceHandler))

    def test_should_build_price_point_handler(self):
        price_point_handler = build_record_handler(Record.PRICE_POINT)
        self.assertTrue(isinstance(price_point_handler, PricePointHandler))

    def test_should_build_product_handler(self):
        product_handler = build_record_handler(Record.PRODUCT)
        self.assertTrue(isinstance(product_handler, ProductHandler))

    def test_should_build_product_spec_handler(self):
        product_spec_handler = build_record_handler(Record.PRODUCT_SPEC)
        self.assertTrue(isinstance(product_spec_handler, ProductSpecHandler))

    def test_should_build_spec_handler(self):
        spec_handler = build_record_handler(Record.SPEC)
        self.assertTrue(isinstance(spec_handler, SpecHandler))

    def test_should_stock_point_handler(self):
        stock_point_handler = build_record_handler(Record.STOCK_POINT)
        self.assertTrue(isinstance(stock_point_handler, StockPointHandler))
