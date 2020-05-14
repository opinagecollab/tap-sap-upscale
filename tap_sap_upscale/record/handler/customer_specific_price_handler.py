from tap_sap_upscale.record.handler.base import BaseHandler
from tap_sap_upscale.record.handler.decorators import Singleton


@Singleton
class CustomerSpecificPriceHandler(BaseHandler):

    def generate(self, customer_specific_price, **options):
        pass
