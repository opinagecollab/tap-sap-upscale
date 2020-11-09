from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class CategoryHandler(BaseHandler):
    _handled_categories = {}
    _code = 0

    def generate(self, category, **options):
        category_id = category.get('id')

        if self.is_handled(category_id):
            return self._handled_categories.get(category_id)

        self._code += 1
        generated_category_id = options.get('tenant_id') + str(self._code)

        category_record = {
            'originalId': category_id,
            'id': generated_category_id,
            'name': category.get('name')
        }
        self._handled_categories[category_id] = category_record

        return category_record
    
    def is_handled(self, category_id):
        return category_id in self._handled_categories
    
    def get_category_record(self, category_id): 
        return self._handled_categories.get(category_id)
