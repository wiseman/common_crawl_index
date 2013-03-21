#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup


# Work around setuptools bug
# http://article.gmane.org/gmane.comp.python.peak/2509
import multiprocessing


PACKAGE_NAME = 'commoncrawlindex'
VERSION = '0.0.1'


settings = dict(
  name=PACKAGE_NAME,
  version=VERSION,
  description='Access to an index of Common Crawl URLs.',
  long_description='Access to an index of Common Crawl URLs',
  author='John Wiseman',
  author_email='jjwiseman@gmail.com',
  url='https://github.com/jjwiseman/common-crawl-index',
  packages=['commoncrawlindex'],
  test_suite='nose.collector',
  install_requires=[
    'boto'
    ],
  tests_require=[
    'nose'
    ],
  license='MIT',
  classifiers=(
    # 'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Natural Language :: English',
    'Programming Language :: Python',
    # 'Programming Language :: Python :: 2.5',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    ),
  entry_points={
    'console_scripts': [
      'cci_lookup = commoncrawlindex.lookup:main',
      'cci_fetch = commoncrawlindex.fetch:main'
      ],
    },
  )


setup(**settings)
