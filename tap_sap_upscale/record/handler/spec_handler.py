from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class SpecHandler(BaseHandler):
    _handled_codes = {}
    _code = 0

    def generate(self, spec, **options):
        attribute_key = spec.get('attributeKey')

        if attribute_key in self._handled_codes:
            return self._handled_codes.get(attribute_key)

        self._code += 1
        spec_id = options.get('tenant_id') + str(self._code)

        self._handled_codes[attribute_key] = spec_id

        return {
            'id': spec_id,
            'name': spec.get('label'),
            'unitName': None,
            'unitSymbol': None,
            'comparable': None
        }
