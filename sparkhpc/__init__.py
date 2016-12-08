from __future__ import print_function
import os, sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from . import sparkjob

try : 
    def start_spark(master = 'local[*]', 
                    spark_conf='./spark_conf', 
                    executor_memory=None,
                    profiling=False, 
                    graphframes_package='graphframes:graphframes:0.2.0-spark2.0-s_2.11', 
                    extra_conf = None):
        """Launch a SparkContext 
        
        Inputs
        ------

        master : URL to spark master in the form 'spark://<master>:<port>'
        
        spark_conf : path to a spark configuration directory

        executor_memory : executor memory in java memory string format, e.g. '4G'

        profiling: whether to turn on python profiling or not

        graphframes_package : which graphframes to load
        """

        os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages {graphframes_package} pyspark-shell"\
                                            .format(graphframes_package=graphframes_package)
        
        os.environ['SPARK_CONF_DIR'] = os.path.realpath(spark_conf)

        os.environ['PYSPARK_PYTHON'] = sys.executable

        from pyspark import SparkContext, SparkConf

        conf = SparkConf()

        conf.set('spark.driver.maxResultSize', '0')

        if executor_memory is not None: 
            conf.set('spark.executor.memory', executor_memory)
        if profiling: 
            conf.set('spark.python.profile', 'true')
        else:
            conf.set('spark.python.profile', 'false')
        
        if extra_conf is not None: 
            for k,v in extra_conf.iteritems(): 
                conf.set(k,v)

        sc = SparkContext(master=master, conf=conf)

        return sc

    def get_sqc(sc):
        from pyspark.sql import SQLContext

        return SQLContext(sc)

except ImportError: 
    logger.warning('Could not import pyspark -- make sure SPARK_HOME is set')