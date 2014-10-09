#!/bin/sh
#BSUB -J sparkpi_job
#BSUB -W 00:10 # requesting 10 minutes
#BSUB -o sparkpi.log
#BSUB -e sparkpi.err
#BSUB -cwd "$(pwd)" # set the directory to the current one
#BSUB -n 24 # requesting 24 cores -- request in multiples of 24 to get entire nodes

module load new
module load java
module load open_mpi

# setup the spark path
. setup_spark.sh

# initialize the nodes
python start_spark_euler.py -c 8 -m 20g 

# creates the slaves file, starts the spark master and worker processes using mpirun
# the "-c" option specifies number of cores per worker
# -m specifies SPARK_MEMORY

# the specific example runs spark's pi estimation with a slices = 100 (first and only argument)

echo " Master is set as $HOSTNAME"

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi --master \
    spark://$HOSTNAME:7077 $SPARK_HOME/lib/spark-examples-1.1.0-hadoop2.4.0.jar \
    100

SPARK_SLAVES=$HOME/slaves_$LSB_JOBID
$SPARK_HOME/sbin/stop-all.sh
$SPARK_HOME/sbin/stop-slaves.sh