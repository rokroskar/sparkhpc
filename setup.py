from setuptools import setup

setup(name="sparkhpc",
      version='0.0.1',
      author="Rok Roskar",
      author_email="roskar@ethz.ch",
      package_dir={'sparkhpc/':''},
      packages=['sparkhpc'],
      scripts=['scripts/sparkcluster', 'scripts/start_notebook.py'],
      include_package_data=True
)
