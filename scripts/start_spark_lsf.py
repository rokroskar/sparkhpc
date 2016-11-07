#! /usr/bin/env python
import os, socket, sys, re
import argparse
import subprocess
import shlex
import time

class bc:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


home_dir = os.environ['HOME']
spark_dir = os.environ.get('SPARK_HOME','{home_dir}/spark'.format(home_dir=home_dir))
spark_sbin = spark_dir + '/sbin'

# figure out which hosts we have
hosts = list(set(os.environ.get('LSB_HOSTS').split(' ')))

# main host
my_host = socket.gethostname()

# set up slave file
slave_file = '%s/slaves_%s'%(os.environ['HOME'],os.environ['LSB_JOBID'])

# set up the slaves file
# with open(slave_file,'w') as slaves:
#     for host in hosts :
#         slaves.write("%s\n"%host)

slaves_template ="mpirun {spark_sbin}/start-slave.sh {master} -c 1"

if  __name__ == "__main__":
    description = """
    Start a standalone spark cluster on an HPC resource running the LSF scheduler. 
    You must specify the number of cores and total memory and have the mpirun executable in your path.
    The script gets information about available resources from the LSF environment variables. 
    """
    
    epilog = "Note that this script doesn't ensure that your memory and core choices make sense -- that's up to you!"

    parser = argparse.ArgumentParser(description=description,epilog=epilog)

    parser.add_argument('cores', action='store',
                        default=None, help='how many cores to assign to each worker')

    parser.add_argument('memory', action='store', type=int,
                        default=None, help='total memory for each worker in GB')

    parser.add_argument('--log-directory', action='store', default='%s/spark-scratch/logs'%home_dir,
                        help='directory for worker and master log files', dest='logdir')

    parser.add_argument('--master-timeout', action='store', default=30, type=int, dest='time_out',
                        help='time to wait for master to report that it is up and running (in seconds)')

    args = parser.parse_args()

    if (args.memory is None) or (args.cores is None):
        parser.print_help()
        exit(0)
                              
    cores = args.cores
    mem = args.memory

    if mem is None: 
        mem = 2
    
    os.environ['SPARK_EXECUTOR_MEMORY'] = '%dG'%mem
    os.environ['SPARK_WORKER_MEMORY'] = '%dG'%(mem+1)

    os.environ['SPARK_SLAVES'] = slave_file
    os.environ['SPARK_LOG_DIR'] = args.logdir

    env = os.environ
    
    # Start the master
    master_command = "{spark_sbin}/start-master.sh".format(spark_sbin=spark_sbin)
    print master_command
    master_out = subprocess.check_output(master_command, env=env)

    master_log = master_out.split('logging to ')[1].rstrip()

    started = False
    start_time = time.time()
    while not started: 
        with open(master_log) as f: 
            log = f.read()
        try : 
            master_url, master_webui = re.findall('(spark://\S+:\d{4}|http://\S+:\d{4})', log)
            started = True
        except ValueError: 
            if time.time() - start_time < args.time_out:
                pass
            else:
                subprocess.call('{spark_sbin}/stop_master.sh'.format(spark_sbin=spark_sbin))
                raise RuntimeError('Spark master appears to not be starting -- check the logs at: %s'%master_log)

    print '['+bc.OKGREEN+'start_spark] '+bc.ENDC+'master running at %s'%master_url
    print '['+bc.OKGREEN+'start_spark] '+bc.ENDC+'master UI available at %s'%master_webui

    sys.stdout.flush()

    # Start the workers
    slaves_command = slaves_template.format(spark_sbin=spark_sbin, master=master_url, cores=cores)
    print slaves_command
    p = subprocess.Popen(shlex.split(slaves_command), env = env)
    p.wait()
