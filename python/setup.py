from setuptools import setup, Extension

ext_modules = [
    Extension("simm.kv._kv", sources=[])
]

setup(
    name="simm",
    version="0.2.0",
    author="SCITIX XSTOR TEAM",
    packages=["simm", "simm.kv"],
    python_requires=">=3.6",
    include_package_data=True,
    package_data={
        "simm.kv": ["*.so"],
    },
    zip_safe=False,
    ext_modules=ext_modules,
)
