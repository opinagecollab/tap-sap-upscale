import unittest

from tap_sap_upscale.record.factory import build_record_handler
from tap_sap_upscale.record.record import Record


class TestProductHandler(unittest.TestCase):

    def test_should_generate_product_record(self):
        config = {
          "api_scheme": "https",
          "api_base_url": "api.test.com",
          "ui_scheme": "http",
          "ui_base_url": "storefront.test.com"
        }
        products = [
            build_record_handler(Record.PRODUCT).generate(
                {
                    'id': '123456',
                    'price': {
                        'originalPrice': 2.34,
                        'sellingPrice': 1.23
                    },
                    'media': [
                        {
                            "thumbnail": "https://api.test.com/thumbnail-image",
                            "fullSize": "https://api.test.com/fullsize-image"
                        }
                    ],
                    'name': 'Sony Product',
                    'description': 'Product description'
                },
                tenant_id='t1',
                config=config
            ),
            build_record_handler(Record.PRODUCT).generate(
                {
                    'id': '234567',
                    'price': {
                        'originalPrice': 4.56,
                        'sellingPrice': 3.45
                    },
                    'media': [
                        {
                            "thumbnail": "https://api.test.com/thumbnail-image",
                            "fullSize": "https://api.test.com/fullsize-image"
                        }
                    ],
                    'name': 'Apple Product',
                    'description': 'Product description'
                },
                tenant_id='t1',
                config=config
            )
        ]
        self.assertEqual(products, [
            {
                'sku': '123456',
                'tenantId': 't1',
                'regularPrice': 1.23,
                'salePrice': None,
                'currency': None,
                'stock': None,
                'imageUri': 'https://api.test.com/fullsize-image',
                'name': 'Sony Product',
                'description': 'Product description',
                'summary': None,
                'manufacturer': None,
                'reviewAverage': None,
                'reviewCount': None,
                'detailsUri': 'http://storefront.test.com/product/123456'
            },
            {
                'sku': '234567',
                'tenantId': 't1',
                'regularPrice': 3.45,
                'currency': None,
                'salePrice': None,
                'stock': None,
                'imageUri': 'https://api.test.com/fullsize-image',
                'name': 'Apple Product',
                'description': 'Product description',
                'summary': None,
                'manufacturer': None,
                'reviewAverage': None,
                'reviewCount': None,
                'detailsUri': 'http://storefront.test.com/product/234567'
            }
        ])
