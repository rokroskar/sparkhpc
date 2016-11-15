# sparkhpc: Spark on HPC clusters made easy

This package tries to greatly simplify deploying and managing [Apache Spark](http://spark.apache.org) clusters on HPC resources. 

## Usage

There are two options for using this library: from the command line or directly from python code. 

### Command line

#### Get usage info

```
$ sparkcluster
Usage: sparkcluster [OPTIONS] COMMAND [ARGS]...

Options:
  --scheduler [lsf]  Which scheduler to use
  --help             Show this message and exit.

Commands:
  info    Get info about currently running clusters
  launch  Launch the Spark master and workers within a...
  start   Start the spark cluster as a batch job
  stop    Kill a currently running cluster ('all' to...

$ sparkcluster submit --help
Usage: sparkcluster start [OPTIONS] NCORES

  Start the spark cluster as a batch job

Options:
  --walltime TEXT     Walltime in HH:MM format
  --jobname TEXT      Name to use for the job
  --template TEXT     Job template path
  --memory TEXT       Memory for each worker in MB
  --spark-home TEXT   Location of the Spark distribution
  --wait              Wait until the job starts
  --help              Show this message and exit.
```

#### Start a cluster
```
$ sparkcluster start 10
```

#### Get information about currently running clusters
```
$ sparkcluster info
----- Cluster 0 -----
Job 31454252 yet started

$ sparkcluster info
----- Cluster 0 -----
Number of cores: 10
master URL: spark://10.11.12.13:7077
Spark UI: http://10.11.12.13:8080
```

#### Stop running clusters
```
$ sparkcluster stop 0
Job <31463649> is being terminated
```

### Python code

```python
import sparkhpc
import pyspark

sj = sparkhpc.sparkjob.LSFSparkJob(ncores=10)

sj.wait_to_start()

sc = pyspark.SparkContext(master=sj.master_url())

sc.parallelize(...)
```

## Installation

```
$ python setup.py install
```

This will install the python package to your default package directory as well as the `sparkcluster` command-line script. 

### Job templates

A simple LSF job template is included in the distribution. If you want to use your own template, you can specify the path using the `--template` flag to `start`. See the [included template](sparkhpc/templates/sparkjob.lsf.template) for an example. Note that the variable names in curly braces, e.g. `{jobname}` will be used to inject runtime parameters. Currently you must specify `walltime`, `ncores`, `memory`, `jobname`, and `spark_home`. If you want to significantly alter the job submission, the best would be to subclass the relevant scheduler class (e.g. `LSFSparkCluster`) and override the `submit` method. 

## Using other schedulers

Currently only LSF is supported. However, adding support for other schedulers is rather straightforward (see the `LSFSparkCluster` implementation for an example). Please submit a pull request if you implement a new scheduler or get in touch if you need help!

To implement support for a new scheduler you should subclass `SparkCluster`. You must define the following *class* variables: 

* `_peek_command` (command to get stdout of current job)
* `_submit_command` (command to submit a job to the scheduler)
* `_job_regex` (regex to get the job ID from return string of submit command)
* `_kill_command` (scheduler command to kill a job)
* `_get_current_jobs` (scheduler command to return jobid, status, jobname one job per line)

Note that `_get_current_jobs` should return a custom formatted string where the output looks like this: 

```
JOB_NAME STAT JOBID
sparkcluster PEND 31610738
sparkcluster PEND 31610739
sparkcluster PEND 31610740
```


## Jupyter notebook

Running Spark applications, especially with python, is really nice from the comforts of a [Jupyter notebook](http://jupyter.org/).
This package includes the  `hpcnotebook` script, which  will setup and launch a secure, password-protected notebook for you.  

```
$ hpcnotebook
Usage: hpcnotebook [OPTIONS] COMMAND [ARGS]...

Options:
  --port INTEGER  Port for the notebook server
  --help          Show this message and exit.

Commands:
  launch  Launch the notebook
  setup   Setup the notebook
```

### Setup
Before launching the notebook, it needs to be configured. The script will first ask for a password for the notebook and generate a self-signed ssh
certificate - this is done to prevent other users of your cluster to stumble into your notebook by chance. 

### Launching
```
$ hpcnotebook launch
To access the notebook, inspect the output below for the port number, then point your browser to https://1.2.3.4:<port_number>
[TerminalIPythonApp] WARNING | Subcommand `ipython notebook` is deprecated and will be removed in future versions.
[TerminalIPythonApp] WARNING | You likely want to use `jupyter notebook` in the future
[I 15:43:12.022 NotebookApp] Serving notebooks from local directory: /cluster/home/roskarr
[I 15:43:12.022 NotebookApp] 0 active kernels
[I 15:43:12.022 NotebookApp] The Jupyter Notebook is running at: https://[all ip addresses on your system]:8889/
[I 15:43:12.022 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
```

So in this case, you could set up a port forward to host `1.2.3.4` and instruct your browser to connect to `https://1.2.3.4:8889`.

Inside the notebook, it is straightforward to set up the `SparkContext` using the `sparkhpc` package (see above).
