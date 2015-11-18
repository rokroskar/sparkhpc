# Spark startup scripts

The scripts here started as templates for running Spark on HPC clusters using LSF. 

## Setting up the cluster environment

The `setup_spark.sh` script initializes some environment variables. This will obviously be cluster-specific and needs to be adapted. 

## Starting a standalone spark cluster on an HPC resource

The main component is the `start_spark_lsf.py` script, which spawns a Spark standalone cluster. It does so using `mpirun` because
this way the scheduler can keep track of the spark processes. The standard spark scripts use `ssh` which 
means that if your job terminates unexpectedly, your spark cluster results in a bunch of ghost 
processes that the scheduler can't control. Using the script here, the scheduler will most likely be able 
to clean up after you if the job fails or does not terminate gracefully (or you forget to shut down the 
spark cluster...)

## Spark jupyter notebook

Running Spark applications, especially with python, is really nice from the comforts of a [Jupyter notebook](http://jupyter.org/).
The `start_notebook.py` script will setup and launch a secure, password-protected notebook for you. It will first ask for a 
password for the notebook and generate a self-signed ssh certificate. To get some usage information just type

```
$ ./start_notebook.py
usage: start_notebook.py [-h] [--setup] [--launch] [--port PORT] [--spark]
                         [--spark_options SPARK_OPTIONS]
                         [--spark_conf SPARK_CONF]

Setup and launch a python notebook set up to serve a Spark session

optional arguments:
  -h, --help            show this help message and exit
  --setup               setup the notebook (does not launch)
  --launch              launch the notebook
  --port PORT           Port number for the notebook server
  --spark               launch the notebook with a spark backend
  --spark_options SPARK_OPTIONS
                        options to pass to spark
  --spark_conf SPARK_CONF
                        spark_configuration_directory
```

You have two options for launching the notebook:  
1. if you want to launch a spark context by hand and do all the configuration inside
your application, just launch the notebook using the `--launch` flag (this does nothing spark-specific, just starts the server). Because
this script is meant to be used while running an interactive session on an HPC cluster, it will also tell you the IP of the machine
where it is running to simplify any tunneling that needs to be done. 

2. if you'd like the notebook to also initialize a spark context and connect to e.g. YARN or some other master, you should specify 
the `--spark` option on the command line instead. This then also gives you the option of specifying other spark options via
`--spark_options` or a configuration directory via `--spark_conf`. 

The provided `notebook_job.lsf` is a template job submission script for LSF. It can run the notebook server as a batch job, you 
just need to note the host so you can connect to it. 



