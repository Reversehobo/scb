from pathlib import Path

import setuptools

VERSION = "0.1.0"  # PEP-440

NAME = "scb"


setuptools.setup(
    name=NAME,
    version=VERSION,
    description="A Python wrapper for SCB API",
    author="Ruben Selander",
    author_email="ruben.selander@nordicintel.com",
    url="https://github.com/rubenselander/scb",  # Update with the actual URL
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
    # Requirements
    install_requires=["requests>=2.21.0"],
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
)
