import unittest
from datetime import datetime, timezone

from tap_sap_upscale.record.factory import build_record_handler
from tap_sap_upscale.record.record import Record


class TestPricePointHandler(unittest.TestCase):
    def test_should_generate_price_point_record(self):
        timestamp = datetime.now(timezone.utc).isoformat()

        price_points = [
            build_record_handler(Record.PRICE_POINT).generate({
                    'sku': 'abc123',
                    'price': {
                        'sellingPrice': 50.0
                    }
                },
                tenant_id='t1',
                timestamp=timestamp
            ),
            build_record_handler(Record.PRICE_POINT).generate({
                    'sku': 'abc234',
                    'price': {
                        'sellingPrice': 54.0
                    }
                },
                tenant_id='t1',
                timestamp=timestamp
            )
        ]

        self.assertEqual(price_points, [
            {
                'id': "{}.{}".format(timestamp, 1),
                'tenantId': 't1',
                'sku': 'abc123',
                'price': 50.0,
                'timestamp': timestamp
            },
            {
                'id': "{}.{}".format(timestamp, 2),
                'tenantId': 't1',
                'sku': 'abc234',
                'price': 54.0,
                'timestamp': timestamp
            }
        ])


