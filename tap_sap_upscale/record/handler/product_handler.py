from urllib.parse import urlunparse

from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton

def get_images(product):
    links = ""
    if len(product.get('media', [])) > 0:
        for i in range(len(product.get('media', []))):
            links += product.get('media')[i]['fullSize'] + " | "
            #links.append(product.get('media')[i]['fullSize'])

    return links[:-2]


@Singleton
class ProductHandler(BaseHandler):

    def generate(self, product, **options):
        return {
            'sku': product.get('id'),   
            'tenantId': options.get('tenant_id'),
            # Currently, we don't consider variants and their potential surcharge. 
            'regularPrice': product.get('price', {}).get('sellingPrice'),
            'salePrice': None,

            # Currently, Upscale only supports one currency per tenant. Here we could hardcode it or leave it None.
            'currency': None,

            # Will have to retrieve it. Requires authentication. 
            'stock': None,

            # There can be more than one images. Shouldn't we change this in the schema?  
            'imageUri':
                product.get('media')[0]['fullSize']
                if len(product.get('media', [])) > 0 and product.get('media')[0].get('fullSize') is not None
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
            'reviewCount': None,
            'images': get_images(product)#product.get('media')[0]['fullSize']
        }






git config --global url."https://7091926f089e3fbe38a7c91bf8123a2b75f91:@github.wdf.sap.corp/".insteadOf "https://github.wdf.sap.corp/"