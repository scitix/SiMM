from setuptools import setup, Extension

ext_modules = [
    Extension("simm.kv._kv", sources=[])
]

setup(ext_modules=ext_modules)
