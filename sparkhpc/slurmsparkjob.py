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
        h,m = [int(x) for x in walltime.split(':')]

        super(SLURMSparkJob, self).__init__(**kwargs)

        self.prop_dict['walltime'] = m + 60*h

    def _peek(self):
        with open(os.path.join(self.workdir, 'sparkcluster-%s.log'%self.jobid)) as f: 
            job_peek = f.read()
        return job_peek    

