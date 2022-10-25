#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

with open("requirements.txt") as requirements_file:
    requirements = requirements_file.read().splitlines()

test_requirements = [""]

setup(
    author="Camptocamp",
    author_email="info@camptocamp.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="CLI tool to publish data from PG database ",
    entry_points={"console_scripts": ["publish=reims_publisher.cli:main"]},
    install_requires=requirements,
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="reims_publisher",
    name="reims_publisher",
    packages=find_packages(include=["reims_publisher", "reims_publisher.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/julsbreakdown/reims_publisher",
    version="0.1.0",
    zip_safe=False,
)
