from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class ProductSpecHandler(BaseHandler):

    def generate(self, spec, **options):
        return {
            'tenantId': options.get('tenant_id'),
            'sku': options.get('sku'),
            'specId': options.get('spec_id'),
            'value': spec.get('value'),
            'pureValue': spec.get('value') if self.is_numeric(spec.get('value')) else None,
            'type': None,
            'interpretedType': None,
            'interpretedValue': None
        }

    def is_numeric(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False
