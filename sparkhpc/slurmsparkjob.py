import os
import time
from .sparkjob import SparkJob
import re
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sparkhpc.slurmsparkjob')


class SLURMSparkJob(SparkJob):
    """
    Class for submitting spark jobs with the SLURM scheduler

    See the `SparkJob` class for keyword descriptions.

    """
    _submit_command = 'sbatch %s'
    _job_regex = "job (\d+)"
    _kill_command = 'scancel'
    _get_current_jobs = 'squeue -o "%.j %.T %.i" -j'

    def __init__(self, walltime='00:30', **kwargs): 
        h,m = map(lambda x: int(x), walltime.split(':'))

        super(SLURMSparkJob, self).__init__(**kwargs)

        self.prop_dict['walltime'] = m + 60*h

    def _peek(self):
        with open(os.path.join(self.workdir, 'sparkcluster-%s.log'%self.jobid)) as f: 
            job_peek = f.read()
        return job_peek    

    def _submit_job(cls, jobfile): 
        """Submits the jobfile and returns the job ID"""

        job_submit = subprocess.Popen(cls._submit_command%jobfile, shell=True, 
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try: 
            jobid = re.findall(cls._job_regex, job_submit.stdout.read())[0]
        except Exception as e: 
            logger.error('Job submission failed or jobid invalid')
            raise e
        return jobid
