from setuptools import setup, find_packages
import os
import re


v = open(os.path.join(os.path.dirname(__file__), 'alembic', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()


readme = os.path.join(os.path.dirname(__file__), 'README.rst')

requires = [
    'SQLAlchemy>=0.7.3',
    'Mako',
]

# Hack to prevent "TypeError: 'NoneType' object is not callable" error
# in multiprocessing/util.py _exit_function when running `python
# setup.py test` (see
# http://www.eby-sarna.com/pipermail/peak/2010-May/003357.html)
try:
    import multiprocessing
except ImportError:
    pass

try:
    import argparse
except ImportError:
    requires.append('argparse')

setup(name='uliweb-alembic',
      version=VERSION,
      description="A port of alembic for uliweb framework.",
      long_description=open(readme).read(),
      classifiers=[
      'Development Status :: 4 - Beta',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Programming Language :: Python :: Implementation :: PyPy',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='SQLAlchemy migrations',
      author='limodou',
      author_email='limodou@gmail.com',
      url='https://github.com/limodou/uliweb-alembic',
      license='MIT',
      packages=find_packages('.', exclude=['examples*', 'test*']),
      include_package_data=True,
      tests_require = ['nose >= 0.11', 'mock'],
      test_suite = "nose.collector",
      zip_safe=False,
      install_requires=requires,
      entry_points = {
        'console_scripts': [ 'alembic = alembic.config:main' ],
      }
)
