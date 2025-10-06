# setup.py
from setuptools import setup

setup(
    name="eucalyptian-utils",     # package name on pip (choose unique)
    version="0.1.0",
    description="Small utility functions",
    py_modules=["utils"],         # name of the single-module file (utils.py)
    url="https://github.com/eucalyptian/utils",
    author="your-name",
    license="MIT",
    install_requires=[],          # put runtime deps here if any
)
