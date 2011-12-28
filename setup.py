#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

setup(name='WebHDFS',
      version=bigdata.version,
      description='SAGA-based Pilot-Data Implementation',
      author='Andre Luckow',
      author_email='andre.luckow@gmail.com',
      url='na',
      classifiers = ['Development Status :: 4 - Beta',                    
                    'Programming Language :: Python',
                    'Environment :: Console',                    
                    'Topic :: Utilities',
                    ],
      platforms = ('Unix', 'Linux', 'Mac OS'),
      packages=['webhdfs', 'examples'],
      data_files=['webhdfs/VERSION'],
      install_requires=['httplib'],
      entry_points = {
        'console_scripts': []
      }
)
