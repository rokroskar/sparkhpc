from setuptools import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy 

import os

currdir = os.getcwd()

setup(name="sparkhpc",
      author="Rok Roskar",
      author_email="roskar@ethz.ch",
      package_dir={'sparkhpc/':''},
      packages=['sparkhpc'],
      scripts=['scripts/startspark.py', 'scripts/start_spark_lsf.py', 'scripts/start_notebook.py']
)
