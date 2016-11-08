#
# 
# Running spark clusters on batch scheduling systems
#
# Author: Rok Roskar, ETH Zuerich, 2016
#
#

import subprocess
import time
import re

class SparkJob(object): 

    def __init__(self, 
                jobname='spark', 
                ncores='4', 
                mem='2',
                walltime='00:30', 
                template='./sparkjob.lsf.template', 
                executor_memory=None, 
                driver_memory=None, 
                config_dir=None, 
                follow_up_script=""):
        self.ncores = ncores
        self.mem = mem
        self.walltime = walltime
        self.template = template
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.config_dir = config_dir
        self.jobname = jobname
        self.follow_up_script = follow_up_script
        self.jobid = None

    def submit(self): 
        """Write job file to current working directory and submit to LSF"""

        with open(self.template, 'r') as template_file: 
            template_str = template_file.read()

        job = template_str.format(walltime=self.walltime, 
                                  ncores=self.ncores, 
                                  mem=str(int(self.mem)*1000), 
                                  jobname=self.jobname, 
                                  follow_up_script=self.follow_up_script)

        with open('job', 'w') as jobfile: 
            jobfile.write(job)

        self.jobid = _submit_job('job')

    def wait_to_start(self, timeout=60):
        """Wait for the job to start or until timeout, whichever comes first"""
        timein = time.time()
        while(True): 
            if time.time() - timein > timeout: 
                print 'Job not started, but timeout reached!'
                break
            if _job_started(self.jobid): 
                break
            time.sleep(1)
        
    def master_url(self): 
        return _master_url(self.jobid)

    def master_ui(self): 
        return _master_ui(self.jobid)


def _job_started(jobid): 
    """Check whether the job is running already or not"""
    stat = subprocess.check_output(["bjobs", "-o", "stat", jobid]).split('\n')
    return stat[1] == 'RUN'

def _submit_job(jobfile): 
    """Submits the jobfile and returns the job ID"""
    job_submit = subprocess.Popen("bsub < %s"%(jobfile), shell=True, stdout=subprocess.PIPE)
    jobid = re.findall('Job <(\d+)>', job_submit.stdout.read())[0]
    return jobid

def _find_jobids(jobname): 
    """Return jobids that match jobname"""
    out = subprocess.check_output(["bjobs","-o","job_name jobid"])
    jobids = re.findall('%s (\d+)'%jobname, out)
    if len(jobid) == 0: 
        print 'Job %s not yet started'%jobname
        return -1
    else:
        return jobids

def _master_url(jobid):
    """Return the Spark master URL for this job"""

    if _job_started(jobid): 
        job_peek = subprocess.check_output(["bpeek", str(jobid)])
        master_url = re.findall('(spark://\S+:\d{4})', job_peek)
        return master_url
    else: 
        print 'Job not yet started'

def _master_ui(jobid):
    """Return the Spark web UI for this job"""

    if _job_started(jobid): 
        job_peek = subprocess.check_output(["bpeek", str(jobid)])
        master_ui = re.findall('(http://\S+:\d{4})', job_peek)
        return master_ui
    else: 
        print 'Job not yet started'
        


