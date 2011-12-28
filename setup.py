#!/usr/bin/env python

import sys
from setuptools import setup, find_packages
import webhdfs

setup(name='WebHDFS',
      version=webhdfs.version,
      description='HDFS Python client based on WebHDFS REST API',
      author='Andre Luckow',
      author_email='andre.luckow@gmail.com',
      url='na',
      classifiers = ['Development Status :: 4 - Beta',                    
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['webhdfs'],
      data_files=['webhdfs/VERSION'],
      install_requires=[],
      entry_points = {
        'console_scripts': []
      }
)
