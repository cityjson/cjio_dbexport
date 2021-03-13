#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.rst') as changelog_file:
    changelog = changelog_file.read()

requirements = [
    'Click>=7.0',
    'psycopg2>=2.8',
    'PyYAML>=5.1.2',
    'cjio >= 0.6.0'
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Balázs Dukai",
    author_email='b.dukai@tudelft.nl',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Export tool from PostGIS to CityJSON",
    entry_points={
        'console_scripts': [
            'cjdb=cjio_dbexport.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + changelog,
    include_package_data=True,
    keywords='cjio_dbexport',
    name='cjio_dbexport',
    packages=find_packages(include=['cjio_dbexport', 'cjio_dbexport.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/balazsdukai/cjio_dbexport',
    version='0.8.6',
    zip_safe=False,
)
