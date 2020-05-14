from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class PricePointHandler(BaseHandler):
    _counter = 0

    def generate(self, product, **options):
        self._counter += 1

        return {
            'id': "{}.{}".format(options.get('timestamp'), self._counter),
            'tenantId': options.get('tenant_id'),
            'sku': product.get('sku'),
            'timestamp': options.get('timestamp'),
            'price': product.get('price', {}).get('sellingPrice')
        }
