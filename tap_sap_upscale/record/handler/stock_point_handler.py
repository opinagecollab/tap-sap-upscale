from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class StockPointHandler(BaseHandler):

    def generate(self, product, **options):
        pass
