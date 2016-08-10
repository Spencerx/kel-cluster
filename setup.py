import codecs
import os

from setuptools import find_packages, setup


def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding="utf-8") as fp:
        return fp.read()


setup(
    name="kel-cluster",
    description="Kel cluster management library",
    author="Eldarion, Inc.",
    author_email="development@eldarion.com",
    long_description=read("README.rst"),
    version="0.0.3",
    packages=find_packages(),
    install_requires=[
        "cryptography",
        "google-api-python-client",
        "Jinja2",
        "pykube",
        "PyYAML"
    ],
    zip_safe=False
)
