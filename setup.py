import os
from setuptools import setup
import sys

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def conf_path(name):
  if sys.prefix == '/usr':
    conf_path = os.path.join('/etc', name)
  else:
    conf_path = os.path.join(sys.prefix, 'etc', name)
  return conf_path

setup(
    name = "download_wunderground",
    version = "0.0.1",
    author = "Ronald van Haren",
    author_email = "r.vanharen@esciencecenter.nl",
    description = ("A python library to download wunderground weather data."),
    license = "Apache 2.0",
    keywords = "Wunderground",
    url = "https://github.com/ERA-URBAN/download_wunderground",
    packages=['download_wunderground'],
    scripts=['download_wunderground/scripts/download_wunderground'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved ::Apache Software License",
    ],
)
