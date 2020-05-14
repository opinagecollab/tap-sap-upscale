import unittest
from datetime import datetime, timezone

from tap_sap_upscale.record.factory import build_record_handler
from tap_sap_upscale.record.record import Record


class TestStockPointHandler(unittest.TestCase):
    def test_should_generate_stock_point_record(self):
        pass
