import os
from distutils.util import convert_path
from setuptools import setup, find_packages

# Load version from version.py
path_to_version_file = "version.py"
main_ns = {}
version_path = convert_path(path_to_version_file)
with open(version_path) as ver_file:
    exec(ver_file.read(), main_ns)

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="BIS_TEST",
    version=main_ns["__version__"],
    description="BIS Spark ETL jobs",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="haifa",
    packages=find_packages(include=["common", "jobs", "conf"]),
    include_package_data=True,
    package_data={
        "conf": ["*.conf"]
    },
    entry_points={
        "console_scripts": [
            "bis-main=main:main"  # Optional: if you define a main() in main.py
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    python_requires=">=3.8"
)
