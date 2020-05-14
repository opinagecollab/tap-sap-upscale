import unittest

from tap_sap_upscale.record.factory import build_record_handler
from tap_sap_upscale.record.record import Record


class TestProductSpecHandler(unittest.TestCase):
    def test_should_generate_product_spec_record(self):
        specs = [
            build_record_handler(Record.PRODUCT_SPEC).generate({
                    'attributeKey': '123',
                    'label': 'Attribute label',
                    'value': '1.23'
                },
                tenant_id='t1',
                sku='abc123',
                spec_id='t11'
            ),

            build_record_handler(Record.PRODUCT_SPEC).generate({
                    'attributeKey': '234',
                    'label': 'Attribute label',
                    'value': '2.34 unit'
                },
                tenant_id='t1',
                sku='abc123',
                spec_id='t12'
            )
        ]

        self.assertEqual(specs, [
            {
                'tenantId': 't1',
                'sku': 'abc123',
                'specId': 't11',
                'value': '1.23',
                'pureValue': '1.23',
                'type': None,
                'interpretedType': None,
                'interpretedValue': None,
            },
            {
                'tenantId': 't1',
                'sku': 'abc123',
                'specId': 't12',
                'value': '2.34 unit',
                'pureValue': None,
                'type': None,
                'interpretedType': None,
                'interpretedValue': None,
            },
        ])
