import unittest

from tap_sap_upscale.record.factory import build_record_handler
from tap_sap_upscale.record.record import Record


class TestSpecHandler(unittest.TestCase):
    def test_should_generate_spec_record(self):
        specs = [
            build_record_handler(Record.SPEC).generate({
                    'attributeKey': '123',
                    'label': 'Attribute 1 label',
                    'value': '1.23'
                },
                tenant_id='t1'
            ),
            build_record_handler(Record.SPEC).generate({
                    'attributeKey': '234',
                    'label': 'Attribute 2 label',
                    'value': '2.34'
                },
                tenant_id='t1'
            )
        ]

        self.assertEqual(specs, [
            {
                'id': 't11',
                'name': 'Attribute 1 label',
                'unitName': None,
                'unitSymbol': None,
                'comparable': None
            },
            {
                'id': 't12',
                'name': 'Attribute 2 label',
                'unitName': None,
                'unitSymbol': None,
                'comparable': None
            }
        ])

    def test_should_ignore_handled_spec_record(self):
        self.assertEqual(build_record_handler(Record.SPEC).generate(
            {
                'attributeKey': '234',
                'label': 'Attribute 2 label',
                'value': '2.34'
            },
            tenant_id='t1'
        ), 't12')
