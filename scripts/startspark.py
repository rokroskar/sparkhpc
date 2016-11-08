#
#
# CLI for starting and running Spark standalone clusters on HPC resources
#
#

import click
from sparkhpc import sparklsf

@click.group()
def cli():
    pass

@cli.command()
@click.argument('ncores')
@click.option('--walltime', default="00:30", help="Walltime in HH:MM format")
@click.option('--jobname', default='spark', help='Name to use for the job')
@click.option('--template', default='./sparkjob.lsf.template', help='Job template path')
@click.option('--driver-memory', default='2G', help='Spark driver memory', envvar='SPARK_DRIVER_MEMORY')
@click.option('--executor-memory', default='2G', help='Spark executor memory', envvar='SPARK_EXECUTOR_MEMORY')
@click.option('--wait', default=0, help='Seconds to wait until job starts')
def submit(ncores, walltime, jobname, template, driver_memory, executor_memory, wait):
    """Submit the spark cluster as an LSF job"""

    sj = sparklsf.SparkJob(ncores=ncores, walltime=walltime, jobname=jobname, template=template,
                           driver_memory=driver_memory, executor_memory=executor_memory)
    sj.submit()

    if wait: 
        print 'Waiting for job to start'
        sj.wait_to_start()
        #print 'Job %s running with %d cores'%(sj.jobid, ncores)


@cli.command()
@click.argument('jobid', type=str) 
def connect_info(jobid):
    if sparklsf._job_started(jobid): 
        print 'master URL: %s'%sparklsf._master_url(jobid)[0]
        print 'Spark UI: %s'%sparklsf._master_ui(jobid)[0]
    else: 
        print 'Job not yet started'

if __name__ == "__main__":
    cli()



