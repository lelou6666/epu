#!/usr/bin/env python -w
# Copyright 2013 University of Chicago

import os
import sys

from setuptools import setup, find_packages

homebrew_include_path = '/usr/local/include'
if sys.platform == 'darwin' and os.path.isdir(homebrew_include_path):
    os.environ['C_INCLUDE_PATH'] = homebrew_include_path

<<<<<<< HEAD
setupdict['install_requires'] = []
if sys.version_info < (2, 6, 0):
    setupdict['install_requires'].append('ssl==1.15-p1')

setupdict['install_requires'] += ['simplejson>=2.3.3',
                                  'httplib2==0.7.1',
                                  'nimboss==0.4.6',
                                  'apache-libcloud==0.8.0',
                                  'dashi==0.1',
                                  'gevent==0.13.6',
                                  'nose',
                                  'mock',
                                 ]
setupdict['tests_require'] = []
setupdict['test_suite'] = 'nose.collector'

setupdict['entry_points'] = {
        'console_scripts': [
            'epu-management-service=epu.dashiproc.epumanagement:main',
            'epu-provisioner-service=epu.dashiproc.provisioner:main',
            'epu-processdispatcher-service=epu.dashiproc.processdispatcher:main',
            'epu-worker=epu.dashiproc.epu_worker:main',
            'epu-high-availability-service=epu.dashiproc.highavailability:main',
            ]
        }
setupdict['scripts'] = ["scripts/epu-process"]

setupdict['package_data'] = {'epu': ['config/*.yml']}
=======
version = '2.1.1'
requires = [
    'httplib2>=0.7.1',
    'boto >= 2.6',
    'apache-libcloud==0.14.0-beta2',
    'kazoo==1.2.1',
    'dashi>=0.2.1',
    'gevent>=0.13.7',
    'simplejson',
    'pychef',
    'mock'
]
tests_require = [
    'epuharness',
    'nose',
    'mock'
]
extras_require = {
    'test': tests_require,
    'exceptional': ['exceptional-python'],
    'statsd': ['statsd'],
}
entry_points = {
    'console_scripts': [
        'epu-management-service=epu.dashiproc.epumanagement:main',
        'epu-provisioner-service=epu.dashiproc.provisioner:main',
        'epu-processdispatcher-service=epu.dashiproc.processdispatcher:main',
        'epu-zktool=epu.zkcli:main',
        'epu-high-availability-service=epu.dashiproc.highavailability:main',
        'epu-dtrs=epu.dashiproc.dtrs:main',
    ]
}
scripts = [
    "scripts/epu-process"
]
package_data = {
    'epu': [
        'config/*.yml'
    ]
}
>>>>>>> refs/remotes/nimbusproject/master

setup(
    name='epu',
    version=version,
    description='OOICI CEI Elastic Processing Unit Services and Agents',
    url='https://confluence.oceanobservatories.org/display/CIDev/Common+Execution+Infrastructure+Development',
    download_url='http://sddevrepo.oceanobservatories.org/releases',
    license='Apache 2.0',
    author='CEI',
    author_email='nimbus@mcs.anl.gov',
    keywords=['ooici', 'cei', 'epu'],
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering'
    ),
    packages=find_packages(),
    install_requires=requires,
    tests_require=tests_require,
    extras_require=extras_require,
    test_suite='nose.collector',
    entry_points=entry_points,
    scripts=scripts,
    package_data=package_data,
)
