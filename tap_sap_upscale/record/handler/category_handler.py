from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class CategoryHandler(BaseHandler):
    _handled_categories = {}
    _code = 0

    def generate(self, category, **options):
        category_id = category.get('id')

        if category_id in self._handled_categories:
            return self._handled_categories.get(category_id)

        self._code += 1
        generated_category_id = options.get('tenant_id') + str(self._code)

        self._handled_categories[category_id] = generated_category_id

        return {
            'id': generated_category_id,
            'name': category.get('name')
        }
