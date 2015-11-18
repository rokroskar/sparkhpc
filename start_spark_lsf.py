#! /usr/bin/python
import os, socket, sys, re

spark_dir = os.environ.get('SPARK_HOME','$HOME/spark')
spark_sbin = spark_dir + '/sbin'

# figure out which hosts we have
pat = re.compile('[e]\d+')
hosts = pat.findall(os.environ.get('LSB_MCPU_HOSTS'))

print os.environ.get('LSB_MCPU_HOSTS')

# main host
my_host = socket.gethostname()

# set up slave file
slave_file = '%s/slaves_%s'%(os.environ['HOME'],os.environ['LSB_JOBID'])

# set up the slaves file
with open(slave_file,'w') as slaves:
    for host in hosts :
        slaves.write("%s\n"%host)

if  __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Start a standalone spark cluster on an HPC resource')

    parser.add_argument('--executor-cores', dest='cores', action='store',
                        default=24, help='How many cores per executor')

    parser.add_argument('--executor-memory', dest='mem', action='store',
                        default=None)

    
    args = parser.parse_args()

    cores = args.cores
    mem = args.mem

    if mem is not None: os.environ['SPARK_EXECUTOR_MEMORY'] = mem

    os.environ['SPARK_SLAVES'] = slave_file

    master_command = '%s/start-master.sh'%spark_sbin

    slaves_command = 'mpirun --cpus-per-rank 24 --pernode %s/start-slave.sh spark://%s:7077 -c %s &'%(spark_sbin,my_host,str(cores))

    print slaves_command

    # set up the spark environment
 #   os.system('%s/spark_config.sh'%spark_sbin)
 #   os.system('%s/../bin/load-spark-env.sh'%spark_sbin)

    # start the host and slaves
    print 'master command ', master_command
    print 'slaves command ', slaves_command
    #os.system(master_command)
    #os.system(slaves_command)

