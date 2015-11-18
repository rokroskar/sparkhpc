#!/bin/sh

echo "setting up cluster python environment"
echo "prepending /cluster/apps/spark/miniconda/bin to PATH"
export PATH=/cluster/apps/spark/miniconda/bin:$PATH
echo "removing everything from the PYTHONPATH"
unset PYTHONPATH
echo "loading hadoop and spark modules"
module load spark
module load hadoop

