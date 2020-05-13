from urllib.parse import urlunparse

import requests
import singer
import urllib3

LOGGER = singer.get_logger()


class UpscaleClient:
    PRODUCT_BASE_PATH = '/consumer/product-content'
    PRODUCT_SEARCH_PATH = '/products?editionId={}&expand=productCategoryIds&pageNumber={}&pageSize=50'
    PRODUCT_SELLINGTREE_SEARCH_PATH = '/sellingtrees/{}' \
                                      '/products?editionId={}&expand=productCategoryIds&pageNumber={}&pageSize=50'

    def __init__(self, config):
        self.scheme = config.get('api_scheme')
        self.base_url = config.get('api_base_url')
        self.edition_id = config.get('api_edition_id')
        self.selling_tree = config.get('api_selling_tree')

        if self.selling_tree is None:
            self.product_search_url = urlunparse((
                self.scheme, self.base_url, self.PRODUCT_BASE_PATH + self.PRODUCT_SEARCH_PATH, None, None, None
            ))
        else:
            self.product_search_url = urlunparse((
                self.scheme, self.base_url, self.PRODUCT_BASE_PATH + self.PRODUCT_SELLINGTREE_SEARCH_PATH, None, None, None
            ))

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def fetch_products(self):
        initial_page = 1
        products = []

        response = self.fetch_product_list(initial_page)
        for product in response['content']:
            products.append(product)

        for page in range(2, response['page']['totalPages'] + 1):
            response = self.fetch_product_list(page)
            for product in response['content']:
                products.append(product)

        return products

    def fetch_product_list(self, page):
        if self.selling_tree is None:
            product_search_url = self.product_search_url.format(self.edition_id, page)
        else:
            product_search_url = self.product_search_url.format(self.selling_tree, self.edition_id, page)

        response = requests.get(product_search_url.format(page), verify=False, timeout=10)

        if response.status_code != 200:
            raise Exception('Failed to fetch product list with status code: {}'.format(response.status_code))

        return response.json()

    def fetch_product_inventory(self):
        pass
