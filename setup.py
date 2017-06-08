from setuptools import setup

setup(name="sparkhpc",
      version='0.3.post4',
      author="Rok Roskar",
      author_email="roskar@ethz.ch",
      url="http://sparkhpc.readthedocs.io",
      description="spark deployment on hpc resources made easy",
      package_dir={'sparkhpc/':''},
      packages=['sparkhpc'],
      scripts=['scripts/sparkcluster', 'scripts/hpcnotebook'],
      include_package_data=True,
      install_requires=['click', 'findspark'],
      keywords=['pyspark', 'spark', 'hpc', 'slurm', 'lsf']
)
