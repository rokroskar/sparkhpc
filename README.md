[![Documentation Status](https://readthedocs.org/projects/sparkhpc/badge/?version=latest)](http://sparkhpc.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://travis-ci.org/rokroskar/sparkhpc.svg?branch=master)](https://travis-ci.org/rokroskar/sparkhpc)
[![Test Coverage](https://codeclimate.com/github/rokroskar/sparkhpc/badges/coverage.svg)](https://codeclimate.com/github/rokroskar/sparkhpc/coverage)

# sparkhpc: Spark on HPC clusters made easy

This package tries to greatly simplify deploying and managing [Apache Spark](http://spark.apache.org) clusters on HPC resources. 

## Installation

### From [pypi](https://pypi.python.org)

```
$ pip install sparkhpc
```

### From source

```
$ python setup.py install
```

This will install the python package to your default package directory as well as the `sparkcluster` and `hpcnotebook` command-line scripts. 

## Usage

There are two options for using this library: from the command line or directly from python code. 

### Command line

#### Get usage info

```
Usage: sparkcluster [OPTIONS] COMMAND [ARGS]...

Options:
  --scheduler [lsf|slurm]  Which scheduler to use
  --help                   Show this message and exit.

Commands:
  info    Get info about currently running clusters
  launch  Launch the Spark master and workers within a...
  start   Start the spark cluster as a batch job
  stop    Kill a currently running cluster ('all' to...

$ sparkcluster start --help
Usage: sparkcluster start [OPTIONS] NCORES

  Start the spark cluster as a batch job

Options:
  --walltime TEXT                Walltime in HH:MM format
  --jobname TEXT                 Name to use for the job
  --template TEXT                Job template path
  --memory-per-executor INTEGER  Memory to reserve for each executor (i.e. the
                                 JVM) in MB
  --memory-per-core INTEGER      Memory per core to request from scheduler in
                                 MB
  --cores-per-executor INTEGER   Cores per executor
  --spark-home TEXT              Location of the Spark distribution
  --wait                         Wait until the job starts
  --help                         Show this message and exit.
```

#### Start a cluster
```
$ sparkcluster start 10
```

#### Get information about currently running clusters
```
$ sparkcluster info
----- Cluster 0 -----
Job 31454252 not yet started

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
from sparkhpc import sparkjob
import findspark 
findspark.init() # this sets up the paths required to find spark libraries
import pyspark

sj = sparkjob.sparkjob(ncores=10)

sj.wait_to_start()

sc = sj.start_spark()

sc.parallelize(...)
```

### Jupyter notebook

`sparkhpc` gives you nicely formatted info about your jobs and clusters in the jupyter notebook - see the [example notebook](./example.ipynb).

## Dependencies

### Python
* [click](http://click.pocoo.org/5/)
* [findspark](https://github.com/minrk/findspark) 

These are installable via `pip install`.

### System configuration
* Spark installation in `~/spark` OR wherever `SPARK_HOME` points to
* java distribution (set `JAVA_HOME`)
* `mpirun` in your path

### Job templates

Simple job templates for the currently supported schedulers are included in the distribution. If you want to use your own template, you can specify the path using the `--template` flag to `start`. See the [included templates](sparkhpc/templates) for an example. Note that the variable names in curly braces, e.g. `{jobname}` will be used to inject runtime parameters. Currently you must specify `walltime`, `ncores`, `memory`, `jobname`, and `spark_home`. If you want to significantly alter the job submission, the best would be to subclass the relevant scheduler class (e.g. `LSFSparkCluster`) and override the `submit` method. 

## Using other schedulers

The LSF and SLURM schedulers are currently supported. However, adding support for other schedulers is rather straightforward (see the `LSFSparkJob` and `SLURMSparkJob` implementations as examples). Please submit a pull request if you implement a new scheduler or get in touch if you need help!

To implement support for a new scheduler you should subclass `SparkCluster`. You must define the following *class* variables: 

* `_peek()` (function to get stdout of the current job)
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

Depending on the scheduler's behavior, you may need to override some of the other methods as well. 

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
On a computer cluster, you would normally either obtain an interactive job and issue the command below, or use this as a part of a batch submission script. 

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

In this case, you could set up a port forward to host `1.2.3.4` and instruct your browser to connect to `https://1.2.3.4:8889`.

Inside the notebook, it is straightforward to set up the `SparkContext` using the `sparkhpc` package (see above). 

## Contributing

Please submit an issue if you discover a bug or have a feature request! Pull requests also very welcome.
