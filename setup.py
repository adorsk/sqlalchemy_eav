import os
from setuptools import find_packages, setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='sqla_eav',
    version='0.0.1',
    packages=find_packages(),
    include_package_data=True,
    license='Apache 2.0',
    description='SqlAlchemy Entity-Attribute-Value Store',
    author='A. Dorsk',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    install_requires=['sqlalchemy'],
)
