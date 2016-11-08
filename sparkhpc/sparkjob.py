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
import signal 
import os
import json
import glob


def sparkjob_factory(scheduler): 
    """Return the correct class for the given scheduler"""

    if scheduler in _sparkjob_registry:
        return _sparkjob_registry[scheduler]
    else: 
        raise RuntimeError('Scheduler %s not supported'%scheduler)

class SparkJob(object): 
    """
    Generic SparkJob class

    To implement other schedulers, you must define: 

    1. class variables:
        * `peek_command` (command to get stdout of current job)
        * `submit_string` (command to submit a job to the scheduler)
        * `job_regex` (regex to get the job ID from return string of submit command)

    2. static methods
        * `job_started`
        * `find_jobids`
        * `current_clusters`

    See the LSFSparkJob class for an example.
    """

    def __init__(self, 
                jobid=None,
                jobname='spark', 
                ncores='4', 
                mem=2000,
                walltime='00:30', 
                template='./sparkjob.lsf.template', 
                executor_memory=None, 
                driver_memory=None, 
                config_dir=None, 
                follow_up_script=""):
        
        # try to load JSON data for the job
        if jobid is not None: 
            try: 
                with open(os.path.join(os.path.expanduser('~'), '.sparkhpc%s'%jobid)) as f:
                    self.prop_dict = json.load(f)
            except Exception as e: 
                raise(e)

        # save the properties in a dictionary
        self.prop_dict = {'ncores': ncores,
                          'mem': mem,
                          'walltime': walltime,
                          'template': template,
                          'executor_memory': executor_memory,
                          'driver_memory': driver_memory,
                          'config_dir': config_dir,
                          'jobname': jobname,
                          'follow_up_script': follow_up_script, 
                          'jobid': jobid
                          }

    def __getattr__(self, val): 
        if val in self.prop_dict: 
            return self.prop_dict[val]
        else: 
            raise AttributeError

    def dump_to_json(self):
        """Write the data to recreate this SparkJob to a JSON file"""
        filename = os.path.join(os.path.expanduser("~"), '.sparkhpc%s'%self.jobid)
        with open(filename, 'w') as fp:
            json.dump(self.prop_dict, fp)

    def wait_to_start(self, timeout=60):
        """Wait for the job to start or until timeout, whichever comes first"""
        timein = time.time()
        while(True): 
            if time.time() - timein > timeout: 
                print 'Job not started, but timeout reached!'
                break
            if self.job_started(self.jobid): 
                break
            time.sleep(1)

    @classmethod
    def master_url(cls, jobid): 
        """Retrieve the spark master address for jobid"""
        if cls.job_started(jobid): 
            job_peek = subprocess.check_output([cls.peek_command, str(jobid)])
            master_url = re.findall('(spark://\S+:\d{4})', job_peek)
            return master_url
        else: 
            print 'Job %s not yet started'%jobid


    @classmethod
    def master_ui(cls, jobid): 
        """Retrieve the web UI address for jobid"""
        if cls.job_started(jobid): 
            job_peek = subprocess.check_output([cls.peek_command, str(jobid)])
            master_ui = re.findall('(http://\S+:\d{4})', job_peek)
            return master_ui
        else: 
            print 'Job %s not yet started'%jobid

    def submit(self): 
        raise NotImplementedError

    def submit(self): 
        """Write job file to current working directory and submit to LSF"""
        with open(self.template, 'r') as template_file: 
            template_str = template_file.read()

        job = template_str.format(walltime=self.walltime, 
                                  ncores=self.ncores, 
                                  mem=self.mem, 
                                  jobname=self.jobname, 
                                  follow_up_script=self.follow_up_script)

        with open('job', 'w') as jobfile: 
            jobfile.write(job)

        self.jobid = self.submit_job('job')
        self.dump_to_json()

    @classmethod
    def submit_job(cls, jobfile): 
        """Submits the jobfile and returns the job ID"""
        job_submit = subprocess.Popen(cls.submit_string%jobfile, shell=True, stdout=subprocess.PIPE)
        jobid = re.findall(cls.job_regex, job_submit.stdout.read())[0]
        return jobid

    @classmethod
    def kill_cluster(cls, clusterid):
        sjs = cls.current_clusters()
        out = subprocess.check_output([cls.kill_command, '%s'%sjs[clusterid].jobid])
        print(out)

class LSFSparkJob(SparkJob):
    """Class for submitting spark jobs with the LSF scheduler"""
    peek_command = 'bpeek'
    submit_string = 'bsub < %s'
    job_regex = 'Job <(\d+)>'
    kill_command = 'bkill'

    @staticmethod
    def job_started(jobid): 
        """Check whether the job is running already or not"""
        stat = subprocess.check_output(["bjobs", "-o", "stat", jobid]).split('\n')
        return stat[1] == 'RUN'

    @staticmethod
    def find_jobids(jobname): 
        """Return jobids that match jobname"""
        out = subprocess.check_output(["bjobs","-o","job_name jobid"])
        jobids = re.findall('%s (\d+)'%jobname, out)
        if len(jobid) == 0: 
            print 'Job %s not yet started'%jobname
            return -1
        else:
            return jobids

    @staticmethod
    def current_clusters():
        """Determine which Spark clusters are currently running or in the queue"""
        sparkjob_files = glob.glob(os.path.join(os.path.expanduser('~'),'.sparkhpc*'))
        lsfjobs = subprocess.check_output(['bjobs', '-o', 'stat jobid'])
        jobids = set(map(lambda s: s.split()[1], lsfjobs.split('\n')[1:-1]))

        sjs = []
        for fname in sparkjob_files: 
            jobid = os.path.basename(fname)[9:]
            if jobid in jobids: 
                sjs.append(SparkJob(jobid=jobid))
        
        return sjs


_sparkjob_registry = {'lsf': LSFSparkJob}
