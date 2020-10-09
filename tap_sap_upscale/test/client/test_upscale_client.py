import httpretty
import json
import unittest
import warnings

from tap_sap_upscale.client.upscale_client import UpscaleClient

category_search_response = {
    'links': [],
    'content': [
        {
            'id': 'category_id',
            'name': 'Category name'
        },
        {
            'id': 'category_id_2',
            'name': 'Category name 2'
        },
        {
            'id': 'category_id_3',
            'name': 'Category name 3'
        }
    ],
    'page': {
        'size': 50,
        'totalElements': 2,
        'totalPages': 1,
        'number': 1
    },
}

product_search_response = {
    'links': [],
    'content': [
        {
            'sku': 'a1b2c3',
            'id': 'a1b2c3',
            'name': 'Product 1 name',
            'description': 'Product 1 description',
            'price': {
                'originalPrice': 1.1,
                'sellingPrice': 1.1,
                'surcharge': {}
            },
            'customAttributes': {
                'attribute_key': 'value'
            },
            'productCategoryIds': [
                "category_id"
            ]
        },
        {
            'sku': 'd4e5f6',
            'id': 'd4e5f6',
            'name': 'Product 2 name',
            'description': 'Product 2 description',
            'price': {
                'originalPrice': 2.2,
                'sellingPrice': 2.2,
                'surcharge': {}
            },
            'customAttributes': {
                'attribute_key': 'value'
            },
            'productCategoryIds': [
                "category_id_2"
            ]
        }
    ],
    'page': {
        'size': 50,
        'totalElements': 100,
        'totalPages': 1,
        'number': 1
    },
}

inventory_result = {
    "atpChecks": [
        {
            "productId": "a1b2c3",
            "quantityAvailable": 5,
            "availability": True,
            "inventoryLevel": "IN_STOCK"
        },
        {
            "productId": "d4e5f6",
            "quantityAvailable": 1,
            "availability": True,
            "inventoryLevel": "IN_STOCK"
        }
    ] 
}

expected_product_list = [
    {
        'sku': 'a1b2c3',
        'id': 'a1b2c3',
        'name': 'Product 1 name',
        'description': 'Product 1 description',
        'price': {
            'originalPrice': 1.1,
            'sellingPrice': 1.1,
            'surcharge': {}
        },
        'customAttributes': {
            'attribute_key': 'value'
        }, 
        'productCategoryIds': ['category_id'], 
        'quantityAvailable': 5,
        'categories': [
            {
                'id': 'category_id',
                'name': 'Category name'
            }
        ]
    },
    {
        'sku': 'd4e5f6',
        'id': 'd4e5f6',
        'name': 'Product 2 name',
        'description': 'Product 2 description',
        'price': {
            'originalPrice': 2.2,
            'sellingPrice': 2.2,
            'surcharge': {}
        },
        'customAttributes': {
            'attribute_key': 'value'
        }, 
        'productCategoryIds': ['category_id_2'],
        'quantityAvailable': 1,
        'categories': [
            {
                'id': 'category_id_2',
                'name': 'Category name 2'
            }
        ]
    }
]


class TestUpscaleClient(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*")

    # def test_should_build_proper_product_search_url(self):
    #     self.client = UpscaleClient({
    #         'api_scheme': 'https',
    #         'api_base_url': 'api.test.com',
    #     })

    #     self.assertEqual(
    #         self.client.product_search_url,
    #         'https://api.test.com/consumer/product-content'
    #         '/products?expand=productCategoryIds&pageNumber={}&pageSize={}'
    #     )

    def test_should_build_proper_product_search_url(self):
        self.client = UpscaleClient({
            'api_scheme': 'https',
            'api_base_url': 'api.test.com',
            'api_selling_tree': 'a1b2-c3d4-e5f6'
        })

        self.assertEqual(
            self.client.product_search_url,
            'https://api.test.com/consumer/product-content'
            '/sellingtrees/{}/products?expand=productCategoryIds&pageNumber={}&pageSize={}'
        )

    @httpretty.activate
    def test_should_get_products(self):
        self.client = UpscaleClient({
            'api_scheme': 'https',
            'api_base_url': 'api.test.com',
            'api_selling_tree': 'a1b2-c3d4-e5f6'
        })

        initial_page = 1
        page_size = 100
        print(self.client.product_search_url)
        print(self.client.category_search_url)
        print(self.client.inventory_search_url)
        httpretty.register_uri(
            httpretty.GET,
            self.client.product_search_url.format('a1b2-c3d4-e5f6', initial_page, 100),
            body=json.dumps(product_search_response))

        httpretty.register_uri(
            httpretty.GET,
            self.client.category_search_url.format(initial_page, page_size),
            body=json.dumps(category_search_response))

        httpretty.register_uri(
            httpretty.GET,
            self.client.inventory_search_url.format('a1b2c3,d4e5f6'),
            body=json.dumps(inventory_result))

        products = self.client.fetch_products()
        
        self.assertEqual(expected_product_list, products)
