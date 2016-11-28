from setuptools import setup

setup(name="sparkhpc",
      version='0.1',
      author="Rok Roskar",
      author_email="roskar@ethz.ch",
      url="http://sparkhpc.readthedocs.io",
      package_dir={'sparkhpc/':''},
      packages=['sparkhpc'],
      scripts=['scripts/sparkcluster', 'scripts/hpcnotebook'],
      include_package_data=True
)
