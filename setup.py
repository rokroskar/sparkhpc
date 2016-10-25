from setuptools import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy 

import os

currdir = os.getcwd()

setup(name="spark-on-hpc-clusters",
      author="Rok Roskar",
      author_email="roskar@ethz.ch",
      scripts=['start_spark_lsf.py', 'start_notebook.py']
)
