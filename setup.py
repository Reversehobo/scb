from setuptools import setup, find_packages

setup(
    name="scb",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["requests"],
    description="A Python wrapper for SCB API",
    author="Ruben Selander",
    author_email="ruben.selander@nordicintel.com",
    url="https://github.com/rubenselander/scb",  # Update with the actual URL
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
