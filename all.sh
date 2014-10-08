#!/bin/sh
#BSUB -J spark_job
#BSUB -W 01:00 # requesting one hour of walltime
#BSUB -n 48 # requesting 48 cores -- request in multiples of 24 to get entire nodes

module load new
module load java
module load open_mpi

python path_to/start_spark_euler.py -c 8 -m 20g 
# creates the slaves file, starts the spark master and worker processes using mpirun
# the "-c" option specifies number of cores per worker
# -m specifies SPARK_MEMORY

spark/bin/spark-submit --class "YourMain" --master spark://$HOSTNAME:7077 path_to_your_jar.jar args

SPARK_SLAVES=$HOME/slaves_$LSB_JOBID spark/sbin/stop-all.sh
spark/sbin/stop-slaves.sh