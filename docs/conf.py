"""Sphinx configuration."""
project = "SCB Python Wrapper"
author = "Ruben Selander"
copyright = "2024, Ruben Selander"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
