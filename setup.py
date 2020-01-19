import platform

from setuptools import find_packages, setup

from axonclient import __version__

is_pypy = platform.python_implementation() == "PyPy"

install_requires = ["grpcio<=1.26.99999", "protobuf<=3.11.99999"]

docs_requires = [
    "Sphinx==1.8.5",
    "python_docs_theme",
    "sphinx_py3doc_enhanced_theme",
    "sphinx_rtd_theme==0.4.3",
    "Alabaster",
    "sphinx-autobuild",
]

dev_requires = docs_requires + ["grpcio-tools", "black", "mypy"]

long_description = """
A Python client for Axon Server.

`Please raise issues on GitHub <https://github.com/johnbywater/eventsourcing/python-axon-client>`_.
"""

packages = find_packages(
    exclude=[
        "docs",
    ]
)

setup(
    name="axonclient",
    version=__version__,
    description="Python client for Axon Server",
    author="John Bywater",
    author_email="john.bywater@appropriatesoftware.net",
    url="https://github.com/johnbywater/python-axon-client",
    license="BSD-3-Clause",
    packages=packages,
    package_data={"axonclient": ["py.typed"]},
    install_requires=install_requires,
    extras_require={
        "dev": dev_requires,
        "docs": docs_requires,
    },
    zip_safe=False,
    long_description=long_description,
    keywords=[
        "Axon",
        "Axon Server",
        "event store"
        "event sourcing"
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        # 'Programming Language :: Python :: 3.5',   # we use f-strings
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
