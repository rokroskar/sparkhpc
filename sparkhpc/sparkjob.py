#
# 
# Running spark clusters on batch scheduling systems
#
# Author: Rok Roskar, ETH Zuerich, 2016
#
#
from __future__ import print_function
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
import logging
import signal

try: 
    get_ipython()
    IPYTHON=True
    from IPython.display import display, HTML
except NameError: 
    IPYTHON=False

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

home_dir = os.path.expanduser('~')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sparkhpc')

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

    table_header = """
                    <th>Job ID</th>
                    <th>Number of cores</th>
                    <th>Status</th>
                    <th>Spark UI</th>
                    <th>Spark URL</th>
                    """

    def __init__(self, 
                clusterid=None,
                jobid=None,
                ncores='4', 
                walltime='00:30',
                memory=2000, 
                jobname='sparkcluster',  
                template=None, 
                config_dir=None, 
                spark_home=None):
        """
        Creates a SparkJob
        
        Parameters
        ----------

        clusterid: int
            if a spark cluster is already running, initialize this SparkJob with its metadata
        jobid: int
            same as `clusterid` but using directly the scheduler job ID
        ncores: int
            number of cores to request
        walltime: string
            walltime in `HH:MM` format as a string
        memory: int
            memory to request per core in MB
        jobname: string
            name for the job - only used for the scheduler
        template: file path
            custom template to use for job submission
        config_dir: directory path
            path to spark configuration directory
        spark_home: 
            path to spark directory; default is the `SPARK_HOME` environment variable, 
            and if it is not set it defaults to `~/spark`

        Example usage
        -------------

            from sparkhpc.sparkjob import LSFSparkJob
            import findspark 
            findspark.init() # this sets up the paths required to find spark libraries
            import pyspark

            sj = LSFSparkJob(ncores=10)

            sj.wait_to_start()

            sc = pyspark.SparkContext(master=sj.master_url)

            sc.parallelize(...)
        """
        if clusterid is not None:
            sjs = self.current_clusters()
            if clusterid < len(sjs): 
                jobid = sjs[clusterid].jobid
            else: 
                logger.error('cluster %d does not exist'%clusterid)

        # try to load JSON data for the job
        if jobid is not None: 
            jobid = str(jobid)
            try: 
                with open(os.path.join(home_dir, '.sparkhpc%s'%jobid)) as f:
                    self.prop_dict = json.load(f)
            except Exception as e: 
                raise(e)

        else:
            if spark_home is None: 
                spark_home = os.environ.get('SPARK_HOME', os.path.join(os.path.expanduser('~'),'spark'))   
                if not os.path.exists(spark_home):
                    raise RuntimeError('Please make sure you either put spark in ~/spark or set the SPARK_HOME environment variable.')

            # save the properties in a dictionary
            self.prop_dict = {'ncores': ncores,
                              'walltime': walltime,
                              'template': template,
                              'memory': memory,
                              'config_dir': config_dir,
                              'jobname': jobname,
                              'jobid': jobid,
                              'status': None,
                              'spark_home': spark_home
                              }

        signal.signal(signal.SIGINT, self._sigint_handler)

    def _repr_html_(self): 
        table_header = "<tr>"+self.table_header+"</tr>"
        return table_header + self._to_string()


    def _to_string(self): 
        if self.job_started(): 
            self.prop_dict['status'] = 'running'

        if IPYTHON:
            row = """
                    <td>{jobid}</td>
                    <td>{ncores}</td>
                    <td>{status}</td>
                    <td><a target="_blank" href="{ui}">{ui}</a></td>
                    <td>{url}</td>
                  """
            
        else:
            row = "Job id: {jobid}\nNumber of cores: {ncores}\nStatus: {status}\nSpark UI: {ui}\nSpark URL: {url}"

        return row.format(jobid=self.jobid, ncores=self.ncores, status=self.status, ui=self.master_ui, url=self.master_url)


    def __getattr__(self, val): 
        if val in self.prop_dict: 
            return self.prop_dict[val]
        else: 
            raise AttributeError

    @property
    def master_url(self): 
        """Get the URL of the Spark master"""
        return self._master_url(self.jobid)


    @property
    def master_ui(self): 
        """Get the UI address of the Spark master"""
        return self._master_ui(self.jobid)


    def _dump_to_json(self):
        """Write the data to recreate this SparkJob to a JSON file"""

        filename = os.path.join(home_dir, '.sparkhpc%s'%self.jobid)
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


    @classmethod
    def _master_url(cls, jobid, timeout=60): 
        """Retrieve the spark master address for jobid"""

        if cls._job_started(jobid): 
            timein = time.time()
            while time.time() - timein < timeout:
                job_peek = subprocess.check_output([cls._peek_command, str(jobid)])
                master_url = re.findall('(spark://\S+:\d{4})', job_peek)
                if len(master_url) > 0: 
                    break
                else:
                    time.sleep(0.5)
        
            if len(master_url) == 0: 
                raise RuntimeError('Unable to obtain information about Spark master -- are you sure it is running?')
            else:
                return master_url[0]
        else: 
            return None

    @classmethod
    def _master_ui(cls, jobid, timeout=60): 
        """Retrieve the web UI address for jobid"""

        if cls._job_started(jobid): 
            timein = time.time()
            while time.time() - timein < timeout: 
                job_peek = subprocess.check_output([cls._peek_command, str(jobid)])
                master_ui = re.findall('(http://\S+:\d{4})', job_peek)
                if len(master_ui) > 0:
                    break
                else: 
                    time.sleep(0.5)
    
            if len(master_ui) == 0: 
                raise RuntimeError('Unable to obtain information about Spark master -- are you sure it is running?')
            else: 
                return master_ui[0]
        else:
            return None


    def submit(self): 
        """Write job file to current working directory and submit to the scheduler"""

        # check that the user has setup the java environment
        if 'JAVA_HOME' not in os.environ:
            raise RuntimeError('JAVA_HOME not set - please set it to the location of your java installation')

        if self.jobid is not None: 
            raise RuntimeError("This SparkJob instance has already submitted a job; you must create a separate instance for a new job")

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

        self.prop_dict['jobid'] = self._submit_job('job')
        self.prop_dict['status'] = 'submitted'
        self._dump_to_json()

        sjs = self.current_clusters()
        logger.info('Submitted cluster %d'%(len(sjs)-1))


    @classmethod
    def _submit_job(cls, jobfile): 
        """Submits the jobfile and returns the job ID"""

        job_submit = subprocess.Popen(cls._submit_command%jobfile, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try: 
            jobid = re.findall(cls._job_regex, job_submit.stdout.read())[0]
        except Exception as e: 
            logger.error('Job submission failed or jobid invalid')
            raise e
        return jobid


    def stop(self): 
        """Stop the current job"""
        self._stop(self.jobid)
        self.prop_dict['status'] = 'stopped'


    @classmethod
    def _stop(cls, jobid):
        out = subprocess.check_output([cls._kill_command, jobid], stderr=subprocess.STDOUT)
        logger.info(out)


    def job_started(self): 
        """Check whether the job is running already or not"""
        started = self._job_started(self.jobid)
        if started: 
            self.prop_dict['status'] = 'running'
        return started


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
        lsfjobs = subprocess.check_output(command, stderr=subprocess.STDOUT)
        jobids = set([s.split()[2] for s in lsfjobs.split('\n')[1:-1]])

        sjs = []
        for fname in sparkjob_files: 
            jobid = os.path.basename(fname)[9:]
            if jobid in jobids: 
                sjs.append(LSFSparkJob(jobid=jobid))
        
        return sjs

    
    def show_clusters(self): 
        sjs = self.current_clusters()

        if len(sjs) == 0: 
            print('No Spark clusters found')

        else:
            if IPYTHON:
                table_header = "<tr><td>ClusterID</td>"+self.table_header+"</tr>"
                table_rows = ""
                for i,sj in enumerate(sjs):
                    table_rows += "<tr>"+"<td>%s</td>"%i+sj._to_string()+"</tr>"
                display(HTML(table_header+table_rows))
            else: 
                for i,sj in enumerate(sjs): 
                    print('----- Cluster %d -----'%i)
                    print(self._to_string())

    def _sigint_handler(self, signal, frame): 
        """Handle ctrl-c from the user"""
        self.stop()
        sys.exit(0)



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

    if spark_home is None: 
        spark_home = os.environ.get('SPARK_HOME', os.path.join(home_dir,'spark'))
    spark_sbin = spark_home + '/sbin'

    os.environ['SPARK_EXECUTOR_MEMORY'] = '%s'%memory
    os.environ['SPARK_WORKER_MEMORY'] = '%s'%memory
    
    env = os.environ
    
    # Start the master
    master_command = os.path.join(spark_sbin, 'start-master.sh')
    logger.debug('master command: ' + master_command)
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

    logger.info('['+bc.OKGREEN+'start_spark] '+bc.ENDC+'master running at %s'%master_url)
    logger.info('['+bc.OKGREEN+'start_spark] '+bc.ENDC+'master UI available at %s'%master_webui)

    sys.stdout.flush()

    # Start the workers
    slaves_template ="mpirun {0}/start-slave.sh {1} -c 1"
    slaves_command = slaves_template.format(spark_sbin, master_url)
    logger.debug('slaves command: ' + slaves_command)
    p = subprocess.Popen(shlex.split(slaves_command), env = env)
    p.wait()


