from urllib.parse import urlunparse

from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class ProductHandler(BaseHandler):

    def generate(self, product, **options):
        return {
            'sku': product.get('id'),
            'tenantId': options.get('tenant_id'),
            'categoryId': options.get('category_id'),
            'regularPrice': product.get('price', {}).get('sellingPrice'),
            'salePrice': None,
            'currency': None,
            'stock': None,
            'imageUri':
                product.get('media')[0]['fullSize']
                if len(product.get('media', [])) > 0
                and product.get('media').get('fullSize') is not None
                else None,
            'detailsUri': urlunparse((
                options.get('config').get('ui_scheme'),
                options.get('config').get('ui_base_url'),
                '/product/' + product.get('id'),
                None,
                None,
                None
            )),
            'name': product.get('name'),
            'description': product.get('description'),
            'summary': None,
            'manufacturer': None,
            'reviewAverage': None,
            'reviewCount': None
        }
