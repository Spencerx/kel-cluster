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
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "cryptography==1.3.1",
        "google-api-python-client==1.5.0",
        "Jinja2==2.8",
        "pykube==0.10.0",
        "PyYAML==3.11"
    ],
    zip_safe=False
)
