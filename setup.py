# from setuptools import setup, find_packages

# setup(
#     name="scb",
#     version="0.1.0",
#     packages=find_packages(),
#     install_requires=["requests"],
#     description="A Python wrapper for SCB API",
#     author="Ruben Selander",
#     author_email="ruben.selander@nordicintel.com",
#     url="https://github.com/rubenselander/scb",  # Update with the actual URL
#     classifiers=[
#         "Programming Language :: Python :: 3",
#         "License :: OSI Approved :: MIT License",
#         "Operating System :: OS Independent",
#     ],
#     python_requires=">=3.8",
# )

from pathlib import Path

import setuptools

VERSION = "0.1.1"  # PEP-440

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
