from urllib.parse import urlunparse

import requests
import singer
import urllib3

LOGGER = singer.get_logger()


class UpscaleClient:
    PRODUCT_CONTENT_PATH = '/consumer/product-content'

    PRODUCT_SEARCH_PATH = '/products?expand=productCategoryIds&pageNumber={}&pageSize={}'
    SELLINGTREE_PRODUCT_SEARCH_PATH = '/sellingtrees/{}/products' \
                                      '?editionId={}&expand=productCategoryIds&pageNumber={}&pageSize={}'

    CATEGORY_PATH = '/categories/{}'
    CUSTOM_ATTRIBUTE_PATH = '/custom-attributes/{}'

    PAGE_SIZE = 100

    def __init__(self, config):
        self.scheme = config.get('api_scheme')
        self.base_url = config.get('api_base_url')
        self.edition_id = config.get('api_edition_id')
        self.selling_tree = config.get('api_selling_tree')

        if self.selling_tree is None:
            self.product_search_url = urlunparse((
                self.scheme, self.base_url, self.PRODUCT_CONTENT_PATH + self.PRODUCT_SEARCH_PATH, None, None, None
            ))
        else:
            self.product_search_url = urlunparse((
                self.scheme, self.base_url, self.PRODUCT_CONTENT_PATH + self.SELLINGTREE_PRODUCT_SEARCH_PATH, None, None, None
            ))

        self.category_url = urlunparse((
            self.scheme, self.base_url, self.PRODUCT_CONTENT_PATH + self.CATEGORY_PATH, None, None, None
        ))

        self.custom_attribute_url = urlunparse((
            self.scheme, self.base_url, self.PRODUCT_CONTENT_PATH + self.CUSTOM_ATTRIBUTE_PATH, None, None, None
        ))

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def fetch_products(self):
        initial_page = 1
        products = []

        response = self.fetch_product_search_list(initial_page, self.PAGE_SIZE)
        for product in response['content']:
            products.append(product)

        for page in range(2, response['page']['totalPages'] + 1):
            response = self.fetch_product_search_list(page, self.PAGE_SIZE)
            for product in response['content']:
                products.append(product)

        for product in products:
            product['augmentedCustomAttributes'] = []
            product['categories'] = []

            augmented_custom_attributes = {}
            custom_attributes = product.get('customAttributes', {})
            for custom_attribute_key in custom_attributes.keys():
                if custom_attribute_key not in augmented_custom_attributes.keys():
                    attribute_details = self.fetch_custom_attribute_details(custom_attribute_key)
                    augmented_custom_attribute = attribute_details
                    augmented_custom_attribute['value'] = custom_attributes[custom_attribute_key]
                    augmented_custom_attributes[custom_attribute_key] = augmented_custom_attribute

                product['augmentedCustomAttributes'].append(augmented_custom_attributes.get(custom_attribute_key))

            categories = {}
            category_ids = product.get('productCategoryIds', [])
            for category_id in category_ids:
                if category_id not in categories.keys():
                    category_details = self.fetch_category_details(category_id)
                    categories[category_id] = category_details

                product['categories'].append(categories[category_id])

            del product['customAttributes']
            del product['productCategoryIds']

        return products

    def fetch_product_search_list(self, page, page_size):
        if self.selling_tree is None:
            product_search_url = self.product_search_url.format(page, page_size)
        else:
            product_search_url = self.product_search_url.format(self.selling_tree, self.edition_id, page, page_size)

        response = requests.get(product_search_url,
                                headers={'Accept-Language': 'en-US'},
                                verify=False,
                                timeout=10)

        if response.status_code != 200:
            raise Exception('Failed to fetch product list with status code: {}'.format(response.status_code))

        return response.json()

    def fetch_category_details(self, category_id):
        category_url = self.category_url.format(category_id)

        response = requests.get(category_url,
                                headers={'Accept-Language': 'en-US'},
                                verify=False,
                                timeout=10)

        if response.status_code != 200:
            raise Exception('Failed to fetch category details with status code: {}'.format(response.status_code))

        return response.json()

    def fetch_custom_attribute_details(self, custom_attribute_id):
        custom_attribute_url = self.custom_attribute_url.format(custom_attribute_id)

        response = requests.get(custom_attribute_url,
                                headers={'Accept-Language': 'en-US'},
                                verify=False,
                                timeout=10)

        if response.status_code != 200:
            raise Exception('Failed to fetch custom attribute details with status code: {}'.format(response.status_code))

        return response.json()

    def fetch_product_inventory(self):
        pass
