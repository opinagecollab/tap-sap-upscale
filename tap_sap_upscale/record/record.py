from enum import Enum


class Record(Enum):
    CATEGORY = 'category'
    CATEGORY_PARENT = 'category_parent'
    CUSTOMER_SPECIFIC_PRICE = 'customer_specific_price'
    PRICE_POINT = 'price_point'
    PRODUCT = 'product'
    CATEGORY_PRODUCT = 'category_product'
    PRODUCT_SPEC = 'product_spec'
    SPEC = 'spec'
    STOCK_POINT = 'stock_point'
