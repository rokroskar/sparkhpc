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
import shlex
import sys
import pkg_resources 

class bc:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def sparkjob_factory(scheduler): 
    """Return the correct class for the given scheduler"""

    if scheduler in _sparkjob_registry:
        return _sparkjob_registry[scheduler]
    else: 
        raise RuntimeError('Scheduler %s not supported'%scheduler)


class SparkJob(object): 
    """
    Generic SparkJob class

    To implement other schedulers, you must simply define some class variables: 

    * `_peek_command` (command to get stdout of current job)
    * `_submit_command` (command to submit a job to the scheduler)
    * `_job_regex` (regex to get the job ID from return string of submit command)
    * `_kill_command` (scheduler command to kill a job)
    * `_get_current_jobs` (scheduler command to return jobid, status, jobname one job per line)
    

    See the LSFSparkJob class for an example.
    """

    def __init__(self, 
                jobid=None,
                jobname='sparkcluster', 
                ncores='4', 
                walltime='00:30', 
                template=None, 
                memory='2000', 
                config_dir=None, 
                follow_up_script="",
                spark_home=None):
        
        # try to load JSON data for the job
        if jobid is not None: 
            jobid = str(jobid)
            try: 
                with open(os.path.join(os.path.expanduser('~'), '.sparkhpc%s'%jobid)) as f:
                    self.prop_dict = json.load(f)
            except Exception as e: 
                raise(e)

        # save the properties in a dictionary
        self.prop_dict = {'ncores': ncores,
                          'walltime': walltime,
                          'template': template,
                          'memory': memory,
                          'config_dir': config_dir,
                          'jobname': jobname,
                          'follow_up_script': follow_up_script, 
                          'jobid': jobid,
                          'status': None,
                          'spark_home': spark_home
                          }

    def __getattr__(self, val): 
        if val in self.prop_dict: 
            return self.prop_dict[val]
        else: 
            raise AttributeError

    def _dump_to_json(self):
        """Write the data to recreate this SparkJob to a JSON file"""
        filename = os.path.join(os.path.expanduser("~"), '.sparkhpc%s'%self.jobid)
        with open(filename, 'w') as fp:
            json.dump(self.prop_dict, fp)

    def wait_to_start(self, timeout=60):
        """Wait for the job to start or until timeout, whichever comes first"""
        if self.status is not 'submitted':
            self.submit()

        timein = time.time()
        while(True): 
            if self.job_started(): 
                break
            time.sleep(1)

    def master_url(self): 
        """Get the URL of the Spark master"""
        return self._master_url(self.jobid)

    @classmethod
    def _master_url(cls, jobid): 
        """Retrieve the spark master address for jobid"""
        if cls._job_started(jobid): 
            job_peek = subprocess.check_output([cls._peek_command, str(jobid)])
            master_url = re.findall('(spark://\S+:\d{4})', job_peek)
            if len(master_url) == 0: 
                raise RuntimeError('Unable to obtain information about Spark master -- are you sure it is running?')
            return master_url
        else: 
            print 'Job %s not yet started'%jobid


    def master_ui(self): 
        """Get the UI address of the Spark master"""
        return self._master_ui(self.jobid)

    @classmethod
    def _master_ui(cls, jobid): 
        """Retrieve the web UI address for jobid"""
        if cls._job_started(jobid): 
            job_peek = subprocess.check_output([cls._peek_command, str(jobid)])
            master_ui = re.findall('(http://\S+:\d{4})', job_peek)
            if len(master_ui) == 0: 
                raise RuntimeError('Unable to obtain information about Spark master -- are you sure it is running?')
            return master_ui
        else: 
            print 'Job %s not yet started'%jobid

    def submit(self): 
        """Write job file to current working directory and submit to the scheduler"""
        if self.template is None: 
            templates = {LSFSparkJob: 'sparkjob.lsf.template'}
            template_file = templates[self.__class__]
            template_str = pkg_resources.resource_string('sparkhpc', 'templates/%s'%template_file)
        else : 
            with open(self.template, 'r') as template_file: 
                template_str = template_file.read()

        job = template_str.format(walltime=self.walltime, 
                                  ncores=self.ncores, 
                                  memory=self.memory, 
                                  jobname=self.jobname, 
                                  spark_home=self.spark_home)

        with open('job', 'w') as jobfile: 
            jobfile.write(job)

        self.jobid = self._submit_job('job')
        self._dump_to_json()

    @classmethod
    def _submit_job(cls, jobfile): 
        """Submits the jobfile and returns the job ID"""
        job_submit = subprocess.Popen(cls._submit_command%jobfile, shell=True, stdout=subprocess.PIPE)
        jobid = re.findall(cls._job_regex, job_submit.stdout.read())[0]
        return jobid

    def stop(self): 
        """Stop the current job"""
        self._stop(self.jobid)

    @classmethod
    def _stop(cls, jobid):
        out = subprocess.check_output([cls._kill_command, jobid])
        print(out)

    def job_started(self): 
        """Check whether the job is running already or not"""
        return self._job_started(self.jobid)

    @classmethod 
    def _job_started(cls, jobid): 
        command = shlex.split(cls._get_current_jobs)
        command.append(str(jobid))
        stat = subprocess.check_output(command).split('\n')
        return stat[1].split()[1] == 'RUN'

    @classmethod
    def current_clusters(cls):
        """Determine which Spark clusters are currently running or in the queue"""
        command = shlex.split(cls._get_current_jobs)
        sparkjob_files = glob.glob(os.path.join(os.path.expanduser('~'),'.sparkhpc*'))
        lsfjobs = subprocess.check_output(command)
        jobids = set(map(lambda s: s.split()[2], lsfjobs.split('\n')[1:-1]))

        sjs = []
        for fname in sparkjob_files: 
            jobid = os.path.basename(fname)[9:]
            if jobid in jobids: 
                sjs.append(LSFSparkJob(jobid=jobid))
        
        return sjs

class LSFSparkJob(SparkJob):
    """Class for submitting spark jobs with the LSF scheduler"""
    _peek_command = 'bpeek'
    _submit_command = 'bsub < %s'
    _job_regex = 'Job <(\d+)>'
    _kill_command = 'bkill'
    _get_current_jobs = 'bjobs -o "job_name stat jobid"'



_sparkjob_registry = {'lsf': LSFSparkJob}

def start_cluster(memory, timeout=30, spark_home=None):
    """Start the spark cluster"""

    home_dir = os.environ['HOME']
    if spark_home is None: 
        spark_home = os.environ.get('SPARK_HOME','{home_dir}/spark'.format(home_dir=home_dir))
    spark_sbin = spark_home + '/sbin'

    os.environ['SPARK_EXECUTOR_MEMORY'] = '%s'%memory
    os.environ['SPARK_WORKER_MEMORY'] = '%s'%memory
    
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
            if time.time() - start_time < timeout:
                pass
            else:
                subprocess.call('{spark_sbin}/stop_master.sh'.format(spark_sbin=spark_sbin))
                raise RuntimeError('Spark master appears to not be starting -- check the logs at: %s'%master_log)

    print '['+bc.OKGREEN+'start_spark] '+bc.ENDC+'master running at %s'%master_url
    print '['+bc.OKGREEN+'start_spark] '+bc.ENDC+'master UI available at %s'%master_webui

    sys.stdout.flush()

    # Start the workers
    slaves_template ="mpirun {0}/start-slave.sh {1} -c 1"
    slaves_command = slaves_template.format(spark_sbin, master_url)
    print slaves_command
    p = subprocess.Popen(shlex.split(slaves_command), env = env)
    p.wait()
