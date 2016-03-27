import sys
from os import path

tree = path.dirname(path.dirname(path.abspath(__file__)))
sys.path.insert(0, tree)

import microfiber


# Project info
project = 'Microfiber'
copyright = '2011-2016, Novacut Inc'
version = microfiber.__version__[:5]
release = microfiber.__version__


# General config
needs_sphinx = '1.2'
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
]
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
exclude_patterns = ['_build']
pygments_style = 'sphinx'


# HTML config
html_theme = 'default'
html_static_path = ['_static']
htmlhelp_basename = 'Microfiberdoc'

