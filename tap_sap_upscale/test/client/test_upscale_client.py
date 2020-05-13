import httpretty
import json
import unittest
import warnings

from tap_sap_upscale.client.upscale_client import UpscaleClient

product_search_response = {
    'links': [],
    'content': [
        {
            'sku': 'a1b2c3',
            'name': 'Product 1 name',
            'description': 'Product 1 description',
            'published': True,
            'active': True,
            'price': {
                'originalPrice': 1.1,
                'sellingPrice': 1.1,
                'surcharge': {}
            },
            'returnInfo': {},
            'media': []
        },
        {
            'sku': 'd4e5f6',
            'name': 'Product 2 name',
            'description': 'Product 2 description',
            'published': True,
            'active': True,
            'price': {
                'originalPrice': 2.2,
                'sellingPrice': 2.2,
                'surcharge': {}
            },
            'returnInfo': {},
            'media': []
        }
    ],
    'page': {
        'size': 50,
        'totalElements': 100,
        'totalPages': 1,
        'number': 1
    },
}


class TestUpscaleClient(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*")

    def test_should_build_proper_product_search_url(self):
        self.client = UpscaleClient({
            'api_scheme': 'https',
            'api_base_url': 'api.test.com',
        })

        self.assertEqual(
            self.client.product_search_url,
            'https://api.test.com/consumer/product-content'
            '/products?editionId={}&expand=productCategoryIds&pageNumber={}&pageSize=50'
        )

    def test_should_build_proper_product_sellingtree_search_url(self):
        self.client = UpscaleClient({
            'api_scheme': 'https',
            'api_base_url': 'api.test.com',
            'api_selling_tree': 'a1b2-c3d4-e5f6'
        })

        self.assertEqual(
            self.client.product_search_url,
            'https://api.test.com/consumer/product-content'
            '/sellingtrees/{}/products?editionId={}&expand=productCategoryIds&pageNumber={}&pageSize=50'
        )

    @httpretty.activate
    def test_should_get_products(self):
        self.client = UpscaleClient({
            'api_scheme': 'https',
            'api_base_url': 'api.test.com',
            'api_edition_id': 'a1b2-c3d4-e5f6'
        })

        initial_page = 1
        httpretty.register_uri(
            httpretty.GET,
            self.client.product_search_url.format('a1b2-c3d4-e5f6', initial_page),
            body=json.dumps(product_search_response))

        product_list = []
        for product in product_search_response['content']:
            product_list.append(product)

        result = self.client.fetch_products()
        self.assertEqual(product_list, result)
