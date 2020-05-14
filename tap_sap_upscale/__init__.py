#!/usr/bin/env python3
import os
import json
import singer

from datetime import datetime, timezone
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from tap_sap_upscale.record.record import Record
from tap_sap_upscale.client.upscale_client import UpscaleClient

REQUIRED_CONFIG_KEYS = []

LOGGER = singer.get_logger()
LOGGER.setLevel(level='DEBUG')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover():
    LOGGER.debug('Discovering available schemas')
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        stream_metadata = []
        stream_key_properties = []

        is_selected = \
            {
                'metadata': {
                    'selected': True
                },
                'breadcrumb': []
            }

        if schema_name == Record.CATEGORY.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('id')

        if schema_name == Record.CUSTOMER_SPECIFIC_PRICE.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('customerId')
            stream_key_properties.append('tenantId')
            stream_key_properties.append('sku')

        if schema_name == Record.PRICE_POINT.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('id')

        if schema_name == Record.PRODUCT.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('sku')
            stream_key_properties.append('tenantId')

        if schema_name == Record.PRODUCT_SPEC.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('tenantId')
            stream_key_properties.append('sku')
            stream_key_properties.append('specId')

        if schema_name == Record.SPEC.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('id')

        if schema_name == Record.STOCK_POINT.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('id')

        streams.append(
            CatalogEntry(
                tap_stream_id=schema_name,
                stream=schema_name,
                schema=schema,
                key_properties=stream_key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )

    return Catalog(streams)


def sync(config, state, catalog):
    LOGGER.info('Syncing selected streams')
    timestamp = datetime.now(timezone.utc).isoformat()
    tenant_id = config.get('tenant_id')

    category_stream = catalog.get_stream(Record.CATEGORY.value)
    if category_stream is not None:
        LOGGER.debug('Writing category schema: \n {} \n and key properties: {}'.format(
            category_stream.schema,
            category_stream.key_properties))
        singer.write_schema(
            category_stream.tap_stream_id,
            category_stream.schema,
            category_stream.key_properties)

    spec_stream = catalog.get_stream(Record.SPEC.value)
    if spec_stream is not None:
        LOGGER.debug('Writing spec schema: \n {} \n and key properties: {}'.format(
            spec_stream.schema,
            spec_stream.key_properties))
        singer.write_schema(
            spec_stream.tap_stream_id,
            spec_stream.schema,
            spec_stream.key_properties)

    product_stream = catalog.get_stream(Record.PRODUCT.value)
    if product_stream is not None:
        LOGGER.debug('Writing product schema: \n {} \n and key properties: {}'.format(
            product_stream.schema,
            product_stream.key_properties))
        singer.write_schema(
            product_stream.tap_stream_id,
            product_stream.schema,
            product_stream.key_properties)

    product_spec_stream = catalog.get_stream(Record.PRODUCT_SPEC.value)
    if product_spec_stream is not None:
        LOGGER.debug('Writing product_spec schema: \n {} \n and key properties: {}'.format(
            product_spec_stream.schema,
            product_spec_stream.key_properties))
        singer.write_schema(
            product_spec_stream.tap_stream_id,
            product_spec_stream.schema,
            product_spec_stream.key_properties)

    customer_specific_price_stream = catalog.get_stream(Record.CUSTOMER_SPECIFIC_PRICE.value)
    if customer_specific_price_stream is not None:
        LOGGER.debug('Writing customer_specific_price schema: \n {} \n and key properties: {}'.format(
            customer_specific_price_stream.schema,
            customer_specific_price_stream.key_properties))
        singer.write_schema(
            customer_specific_price_stream.tap_stream_id,
            customer_specific_price_stream.schema,
            customer_specific_price_stream.key_properties)

    price_point_stream = catalog.get_stream(Record.PRICE_POINT.value)
    if price_point_stream is not None:
        LOGGER.debug('Writing price_point schema: \n {} \n and key properties: {}'.format(
            price_point_stream.schema,
            price_point_stream.key_properties))
        singer.write_schema(
            price_point_stream.tap_stream_id,
            price_point_stream.schema,
            price_point_stream.key_properties)

    stock_point_stream = catalog.get_stream(Record.STOCK_POINT.value)
    if stock_point_stream is not None:
        LOGGER.debug('Writing stock_point schema: \n {} \n and key properties: {}'.format(
            stock_point_stream.schema,
            stock_point_stream.key_properties))
        singer.write_schema(
            stock_point_stream.tap_stream_id,
            stock_point_stream.schema,
            stock_point_stream.key_properties)

    LOGGER.info('Fetching Upscale products')
    products = UpscaleClient(config).fetch_products()

    for product in products:
        LOGGER.info('Syncing product with code: {}'.format(product.get('sku')))

        product_categories = product.get('categories', [])
        if len(product_categories) == 0:
            LOGGER.info('Product has no category! Skipping ...')
            continue

        category = product.get('categories', [])[0]
        print("Selected category: {}", category)
    #    category_record = build_record_handler(Record.CATEGORY).generate(category, tenant_id=tenant_id)

    #     # category record builder returns a record if the category hasn't been handled yet
    #     # otherwise, it returns the id of an already handled category record
    #     if isinstance(category_record, dict):
    #         category_id = category_record.get('id')
    #
    #         LOGGER.debug('Writing category record: {}'.format(category_record))
    #         singer.write_record(Record.CATEGORY.value, category_record)
    #     else:
    #         category_id = category_record
    #
    #     product_record = \
    #         build_record_handler(Record.PRODUCT).generate(
    #             product, tenant_id=tenant_id, category_id=category_id, config=config)
    #     LOGGER.debug('Writing product record: {}'.format(product_record))
    #     singer.write_record(Record.PRODUCT.value, product_record)
    #
    #     if 'classifications' in product:
    #         for classification in product['classifications']:
    #
    #             for feature in classification['features']:
    #                 # ignore specs with multiple values
    #                 if len(feature.get('featureValues')) > 1:
    #                     continue
    #
    #                 spec_record = build_record_handler(Record.SPEC).generate(feature, tenant_id=tenant_id)
    #
    #                 if isinstance(spec_record, dict):
    #                     spec_id = spec_record.get('id')
    #
    #                     LOGGER.debug('Writing spec record: {}'.format(spec_record))
    #                     singer.write_record(Record.SPEC.value, spec_record)
    #                 else:
    #                     spec_id = spec_record
    #
    #                 product_spec_record = \
    #                     build_record_handler(Record.PRODUCT_SPEC).generate(
    #                         feature, tenant_id=tenant_id, sku=product.get('code'), spec_id=spec_id)
    #
    #                 LOGGER.debug('Writing product spec record: {}'.format(product_spec_record))
    #                 singer.write_record(Record.PRODUCT_SPEC.value, product_spec_record)
    #
    #     price_point_record = \
    #         build_record_handler(Record.PRICE_POINT).generate(product, timestamp=timestamp, tenant_id=tenant_id)
    #     LOGGER.debug('Writing price_point record: {}'.format(price_point_record))
    #     singer.write_record(Record.PRICE_POINT.value, price_point_record)
    #
    #     stock_point_record = \
    #         build_record_handler(Record.STOCK_POINT).generate(product, timestamp=timestamp, tenant_id=tenant_id)
    #     LOGGER.debug('Writing stock_point record: {}'.format(stock_point_record))
    #     singer.write_record(Record.STOCK_POINT.value, stock_point_record)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
