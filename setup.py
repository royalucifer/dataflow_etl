from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = ["apache_beam"]
PACKAGE_NAME = "dataflow_etl"
PACKAGE_VERSION = "0.01"

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    include_package_data=True,
    requires=[]
)