#
# 
# Running spark clusters on batch scheduling systems
#
# Author: Rok Roskar, ETH Zuerich, 2016
#
#


class SparkJob(object): 

    def __init__(self, 
                jobname='spark', 
                ncores='4', 
                walltime='00:30', 
                template='./sparkjob.lsf.template', 
                executor_memory=None, 
                driver_memory=None, 
                config_dir=None, 
                follow_up_script=""):
        self.ncores = ncores
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

        with open(template, 'r') as template_file: 
            template = template_file.read()

        job = template.format(walltime=self.walltime, ncores=self.ncores, jobname=self.jobname, follow_up_script=self.follow_up_script)

        with open('job', 'w') as jobfile: 
            jobfile.write(job)

        self.jobid = _submit_job('job')

    def master_url(self):
        """Return the Spark master URL for this job"""

        if _job_started(jobid): 
            job_peek = subprocess.check_output(["bpeek", str(jobid)])
            master_url = re.findall('(spark://\S+:\d{4}', job_peek)


def _job_started(jobid): 
    """Check whether the job is running already or not"""
    stat = subprocess.Popen(["bjobs", "%s", "-o", "stat"]).split('\n')
    return stat[1] == 'RUN'

def _submit_job(jobfile): 
    """Submits the jobfile and returns the job ID"""
    job_submit = subprocess.Popen("bsub -n %s < %s"%(number_of_cores, jobfile), shell=True, stdout=subprocess.PIPE)
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




