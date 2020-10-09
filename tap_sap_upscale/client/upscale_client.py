from urllib.parse import urlunparse

import requests
import singer
import urllib3

LOGGER = singer.get_logger()

class UpscaleClient:
    PRODUCT_CONTENT_PATH = '/consumer/product-content'
    INVENTORY_CONTENT_PATH = 'consumer/inventory-service'

    ## This client only retrieves products assigned to a selling tree. 
    ## - Note that editionId is optional. 
    PRODUCT_SEARCH_PATH = '/sellingtrees/{}/products' \
                                      '?expand=productCategoryIds&pageNumber={}&pageSize={}'

    INVENTORY_SEARCH_PATH = '/atp?productIds={}'

    CATEGORY_SEARCH_PATH = '/categories?pageNumber={}&pageSize={}'
    CUSTOM_ATTRIBUTE_PATH = '/custom-attributes/{}'

    # Upscale appears to have a 50 items per-page maximum. 
    PAGE_SIZE = 50

    def __init__(self, config):
        self.scheme = config.get('api_scheme')
        self.base_url = config.get('api_base_url')
        self.edition_id = config.get('api_edition_id')
        self.selling_tree = config.get('api_selling_tree')

        self.product_search_url = urlunparse((
            self.scheme, self.base_url, self.PRODUCT_CONTENT_PATH + self.PRODUCT_SEARCH_PATH, None, None, None
        ))

        self.category_search_url = urlunparse((
            self.scheme, self.base_url, self.PRODUCT_CONTENT_PATH + self.CATEGORY_SEARCH_PATH, None, None, None
        ))

        self.custom_attribute_url = urlunparse((
            self.scheme, self.base_url, self.PRODUCT_CONTENT_PATH + self.CUSTOM_ATTRIBUTE_PATH, None, None, None
        ))

        self.inventory_search_url = urlunparse((
            self.scheme, self.base_url, self.INVENTORY_CONTENT_PATH + self.INVENTORY_SEARCH_PATH, None, None, None
        ))

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def fetch_products(self):
        initial_page = 1

        LOGGER.info('Fetching products')
        products, page_info = self.fetch_product_search_list(initial_page, self.PAGE_SIZE)

        for page in range(2, page_info['totalPages'] + 1):
            response, _ = self.fetch_product_search_list(page, self.PAGE_SIZE)
            products.extend(response)

        augmented_products = self.augment_product_details(products)        

        return augmented_products

    def fetch_product_search_list(self, page, page_size):
        product_search_url = self.product_search_url.format(self.selling_tree, page, page_size)
        if self.edition_id != None:
            product_search_url += "&editionId=" + self.edition_id

        response = requests.get(product_search_url,
                                headers={'Accept-Language': 'en-US'},
                                verify=False,
                                timeout=10)

        if response.status_code != 200:
            raise Exception('Failed to fetch product list with status code: {}'.format(response.status_code))

        json_reponse = response.json()
        products = json_reponse['content']
        self.fetch_products_inventory(products)

        return products, json_reponse['page']

    def fetch_products_inventory(self, products):
        product_ids = ",".join(map(lambda product: product.get('id'), products))
        products_inventory_url = self.inventory_search_url.format(product_ids)

        response = requests.get(products_inventory_url,
                        headers={'Accept-Language': 'en-US'},
                        verify=False,
                        timeout=10)

        if response.status_code != 200:
            raise Exception('Failed to fetch products inventory with status code: {}'.format(response.status_code))

        # ATP stands for Available to Promise - which articles are available
        products_availability = {}
        for atp in response.json()['atpChecks']:
            product_id = atp.get('productId')
            products_availability[product_id] = atp.get('quantityAvailable')
        
        for product in products:
            quantityAvailable = products_availability.get(product.get('id'))
            if quantityAvailable == None:
                quantityAvailable = 0
            product['quantityAvailable'] = quantityAvailable

    def fetch_categories(self):
        initial_page = 1
        categories = {}

        LOGGER.info('Fetching categories')
        response = self.fetch_categories_list(initial_page, self.PAGE_SIZE)

        for category in response['content']:
            category_id = category.get('id')
            categories[category_id] = category

        for page in range(2, response['page']['totalPages'] + 1):
            response = self.fetch_categories_list(page, self.PAGE_SIZE)
            for category in response['content']:
                category_id = category.get('id')
                categories[category_id] = category

        return categories

    def fetch_categories_list(self, page, page_size):
        category_search_url = self.category_search_url.format(page, page_size)
        if self.edition_id != None:
            category_search_url += "&editionId=" + self.edition_id
        
        response = requests.get(category_search_url, 
                                headers={'Accept-Language': 'en-US'},
                                verify=False,
                                timeout=10)
        
        if response.status_code != 200:
            raise Exception('Failed to fetch categories list with status code: {}'.format(response.status_code))
        
        return response.json()

    def augment_product_details(self, products):
        augmented_products = []

        categories = self.fetch_categories()
        for product in products:
            product = self.augment_product_categories(product, categories)
            augmented_products.append(product)

        return augmented_products

    def augment_product_categories(self, product, categories):
        product['categories'] = []

        category_ids = product.get('productCategoryIds', [])
        for category_id in category_ids:
            if category_id is "":
                continue

            if category_id not in categories.keys():
                product_id = product.get('id')
                raise Exception(f'Product {product_id} has unknown category {category_id}')
            
            product['categories'].append(categories[category_id])

        return product

