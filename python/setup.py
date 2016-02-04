try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'h5spark: loading hdf5 in spark',
    'author': 'Jialin Liu',
    'url': 'h5spark github, add later',
    'download_url': 'Where to download it.',
    'author_email': 'jalnliu@lbl.gov',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['h5spark'],
    'scripts': [],
    'name': 'h5spark'
}

setup(**config)

