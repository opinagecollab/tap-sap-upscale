#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-sap-upscale",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=[
        "tap_sap_upscale.client",
        "tap_sap_upscale.record",
        "tap_sap_upscale.record.handler"
    ],
    install_requires=[
        "singer-python==5.9.0",
        "requests==2.24.0",
    ],
    entry_points="""
    [console_scripts]
    tap-sap-upscale=tap_sap_upscale:main
    """,
    packages=find_packages(),
    package_data = {
        "schemas": ["tap_sap_upscale/schemas/*.json"]
    },
    include_package_data=True,
)
