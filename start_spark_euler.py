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
        if host != my_host : 
            slaves.write("%s\n"%host)

if  __name__ == "__main__":
    import getopt

    # set defaults
    mem = None #os.environ.get('SPARK_MEMORY','2g')
    cores = None

    try  :
        opts, args  = getopt.getopt(sys.argv[1:], "m:c:")
    except getopt.GetoptError :
        usage()
        sys.exit(2)

    for opt, arg in opts :
        if opt == '-m' :
            mem = arg

        if opt == '-c' :     # set to 24 if sufficient memory
            cores = arg
	else : 
	    cores = 24

    if mem is not None: os.environ['SPARK_EXECUTOR_MEMORY'] = mem

    os.environ['SPARK_SLAVES'] = slave_file

    master_command = '%s/start-master.sh'%spark_sbin

    slaves_command = 'mpirun --cpus-per-rank 24 --pernode %s/start-slave.sh 1 spark://%s:7077 -c %s &'%(spark_sbin,my_host,str(cores))

    print slaves_command

    # set up the spark environment
 #   os.system('%s/spark_config.sh'%spark_sbin)
 #   os.system('%s/../bin/load-spark-env.sh'%spark_sbin)

    # start the host and slaves
    print 'master command ', master_command
    print 'slaves command ', slaves_command
    os.system(master_command)
    os.system(slaves_command)
