from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class CategoryParentHandler(BaseHandler):

    def generate(self, **options):
        return {
            'categoryId': options.get('category_id'),
            'parentId': options.get('parent_id')
        }