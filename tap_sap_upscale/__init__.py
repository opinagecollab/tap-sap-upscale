#!/usr/bin/env python3
import os
import json
import singer

from datetime import datetime, timezone
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_sap_upscale.record.factory import build_record_handler
from tap_sap_upscale.record.record import Record
from tap_sap_upscale.client.upscale_client import UpscaleClient

TAP_VERSION = '1.0.0'

REQUIRED_CONFIG_KEYS = [
    'tenant_id',
    'api_scheme',
    'api_base_url',
    'api_edition_id',
    'ui_scheme',
    'ui_base_url'
]

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
            schemas[file_raw] = Schema.from_dict(json.load(file))

    return schemas

def discover():
    LOGGER.debug('Discovering available schemas')
    LOGGER.debug(f'- tap-sap-upscale -> version {TAP_VERSION}')
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
        
        if schema_name == Record.CATEGORY_PARENT.value: 
            stream_metadata.append(is_selected)
            stream_key_properties.append('categoryId')
            stream_key_properties.append('parentId')

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

        if schema_name == Record.CATEGORY_PRODUCT.value:
            stream_metadata.append(is_selected)
            stream_key_properties.append('sku')
            stream_key_properties.append('tenantId')
            stream_key_properties.append('categoryId')

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

def write_category_record(category, tenant_id):
    category_id = category.get('id')
    category_handler = build_record_handler(Record.CATEGORY)

    if category_handler.is_handled(category_id): 
       return category_handler.get_category_record(category_id) 

    category_record = build_record_handler(Record.CATEGORY).generate(category, tenant_id=tenant_id)
    LOGGER.debug('Writing category record: {}'.format(category_record))
    singer.write_record(Record.CATEGORY.value, category_record)

    parent_categories = category['parentCategories']
    for parent_category in parent_categories:
        parent_record = write_category_record(parent_category, tenant_id)

        # Write the relationship record
        category_generated_id = category_record.get('id')
        parent_category_generated_id = parent_record.get('id')
        category_parent_record = build_record_handler(Record.CATEGORY_PARENT).generate(
            category_id=category_generated_id, 
            parent_id=parent_category_generated_id)

        LOGGER.debug('Writing category parent record: {}'.format(category_parent_record))
        singer.write_record(Record.CATEGORY_PARENT.value, category_parent_record)

    return category_record

def sync(config, state, catalog):
    LOGGER.info('Syncing selected streams')
    LOGGER.debug(f'- tap-sap-upscale -> version {TAP_VERSION}')
    timestamp = datetime.now(timezone.utc).isoformat()
    tenant_id = config.get('tenant_id')

    category_stream = catalog.get_stream(Record.CATEGORY.value)
    if category_stream is not None:
        LOGGER.debug('Writing category schema: \n {} \n and key properties: {}'.format(
            category_stream.schema,
            category_stream.key_properties))
        singer.write_schema(
            category_stream.tap_stream_id,
            category_stream.schema.to_dict(),
            category_stream.key_properties)
    
    category_parent_stream = catalog.get_stream(Record.CATEGORY_PARENT.value)
    if category_parent_stream is not None: 
        LOGGER.debug('Writing category_parent schema: \n {} \n and key properties: {}'.format(
            category_parent_stream.schema,
            category_parent_stream.key_properties))
        singer.write_schema(
            category_parent_stream.tap_stream_id, 
            category_parent_stream.schema.to_dict(),
            category_parent_stream.key_properties)

    spec_stream = catalog.get_stream(Record.SPEC.value)
    if spec_stream is not None:
        LOGGER.debug('Writing spec schema: \n {} \n and key properties: {}'.format(
            spec_stream.schema,
            spec_stream.key_properties))
        singer.write_schema(
            spec_stream.tap_stream_id,
            spec_stream.schema.to_dict(),
            spec_stream.key_properties)

    product_stream = catalog.get_stream(Record.PRODUCT.value)
    if product_stream is not None:
        LOGGER.debug('Writing product schema: \n {} \n and key properties: {}'.format(
            product_stream.schema,
            product_stream.key_properties))
        singer.write_schema(
            product_stream.tap_stream_id,
            product_stream.schema.to_dict(),
            product_stream.key_properties)

    product_spec_stream = catalog.get_stream(Record.PRODUCT_SPEC.value)
    if product_spec_stream is not None:
        LOGGER.debug('Writing product_spec schema: \n {} \n and key properties: {}'.format(
            product_spec_stream.schema,
            product_spec_stream.key_properties))
        singer.write_schema(
            product_spec_stream.tap_stream_id,
            product_spec_stream.schema.to_dict(),
            product_spec_stream.key_properties)

    category_product_stream = catalog.get_stream(Record.CATEGORY_PRODUCT.value)
    if category_product_stream is not None:
        LOGGER.debug('Writing category_product schema: \n {} \n and key properties: {}'.format(
            category_product_stream.schema,
            category_product_stream.key_properties))
        singer.write_schema(
            category_product_stream.tap_stream_id,
            category_product_stream.schema.to_dict(),
            category_product_stream.key_properties)

    customer_specific_price_stream = catalog.get_stream(Record.CUSTOMER_SPECIFIC_PRICE.value)
    if customer_specific_price_stream is not None:
        LOGGER.debug('Writing customer_specific_price schema: \n {} \n and key properties: {}'.format(
            customer_specific_price_stream.schema,
            customer_specific_price_stream.key_properties))
        singer.write_schema(
            customer_specific_price_stream.tap_stream_id,
            customer_specific_price_stream.schema.to_dict(),
            customer_specific_price_stream.key_properties)

    price_point_stream = catalog.get_stream(Record.PRICE_POINT.value)
    if price_point_stream is not None:
        LOGGER.debug('Writing price_point schema: \n {} \n and key properties: {}'.format(
            price_point_stream.schema,
            price_point_stream.key_properties))
        singer.write_schema(
            price_point_stream.tap_stream_id,
            price_point_stream.schema.to_dict(),
            price_point_stream.key_properties)

    stock_point_stream = catalog.get_stream(Record.STOCK_POINT.value)
    if stock_point_stream is not None:
        LOGGER.debug('Writing stock_point schema: \n {} \n and key properties: {}'.format(
            stock_point_stream.schema,
            stock_point_stream.key_properties))
        singer.write_schema(
            stock_point_stream.tap_stream_id,
            stock_point_stream.schema.to_dict(),
            stock_point_stream.key_properties)

    LOGGER.info('Fetching Upscale products')
    products = UpscaleClient(config).fetch_products()

    for product in products:
        LOGGER.info('Syncing product with code: {}'.format(product.get('sku')))

        product_categories = product.get('categories', [])
        if len(product_categories) == 0:
            LOGGER.info('Product has no category! Skipping ...')
            continue
        
        product_record = \
            build_record_handler(Record.PRODUCT).generate(
                product, tenant_id=tenant_id, config=config)
        LOGGER.debug('Writing product record: {}'.format(product_record))
        singer.write_record(Record.PRODUCT.value, product_record)

        for category in product_categories: 
            category_record = write_category_record(category, tenant_id)
            category_id = category_record.get('id')
            
            category_product_record = build_record_handler(Record.CATEGORY_PRODUCT).generate(
                tenant_id=tenant_id, sku=product.get('sku'), category_id=category_id)

            LOGGER.debug('Writing category_product record: {}'.format(category_product_record))
            singer.write_record(Record.CATEGORY_PRODUCT.value, category_product_record)

        # TODO: Uncomment when we have a way to extract specs "metadata" from Upscale. 
        #    In our warehouse data model, a product_spec represents a product's feature and a spec provides information about the type of data 
        #    that feature represents. For example, a product_spec could be 1/2.3". Without some context this data is not that useful. However, with 
        #    the spec we can qualify it to be the optical sensor size and defined in inches. 
        #    The Commerce Cloud already has a rich classification data model that we can use to retrieve both of this pieces of information. However, 
        #    for now, Upscale only exposes specs without any semantics (they are a collection of strings). Until this is available this tap cannot 
        #    import the spec and product_spec data in the same way as the tap-sap-commerce-cloud. Commenting this out for now. 
        # 
        # product_specs = parse_product_specs()
        # for spec in product_specs:
        #     product_spec_record = \
        #         build_record_handler(Record.PRODUCT_SPEC).generate(

        #         )
            
        #     LOGGER.debug(f'Writing product spec record: {product_spec_record}')
        #     singer.write_record(Record.PRODUCT_SPEC.value, product_spec_record)

        price_point_record = \
            build_record_handler(Record.PRICE_POINT).generate(product, timestamp=timestamp, tenant_id=tenant_id)
        LOGGER.debug('Writing price_point record: {}'.format(price_point_record))
        singer.write_record(Record.PRICE_POINT.value, price_point_record)

        stock_point_record = \
            build_record_handler(Record.STOCK_POINT).generate(product, timestamp=timestamp, tenant_id=tenant_id)
        LOGGER.debug('Writing stock_point record: {}'.format(stock_point_record))
        singer.write_record(Record.STOCK_POINT.value, stock_point_record)

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
