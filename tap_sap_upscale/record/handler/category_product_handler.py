from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton

@Singleton
class CategoryProductHandler(BaseHandler):

    def generate(self, **options):
        return {
            'tenantId': options.get('tenant_id'),
            'sku': options.get('sku'),
            'categoryId': options.get('category_id')
        } 