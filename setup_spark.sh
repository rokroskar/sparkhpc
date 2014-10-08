#!/bin/bash
export http_proxy=http://proxy.ethz.ch:3128/   # To use wget
module load new java   # To use java

startingDir=$(pwd)
sparkVersion="spark-1.1.0-bin-hadoop2.4"
cd ~ # go to home directory
# check for / install spark
if [ -d "$sparkVersion" ]
then   
    echo "Spark is already installed @ $sparkVersion"
else
    echo "Installing Spark $sparkVersion"
    sparkFile="$sparkVersion.tgz"
    fetchCmd="wget http://d3kbcqa49mib13.cloudfront.net/$sparkFile"
    extCmd="tar -xvf $sparkFile"
    rmCmd="rm $sparkFile"
    $fetchCmd
    $extCmd
    $rmCmd
fi
SPARK_HOME=$(pwd)/spark-1.1.0-bin-hadoop2.4
export SPARK_HOME
echo "Setting up temporary directories in defaults"
echo "spark.local.dir  /scratch" > $SPARK_HOME/conf/spark-defaults.conf

cd $startingDir
