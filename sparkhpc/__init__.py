from __future__ import print_function
import os, sys
import logging
from . import sparkjob
from . import lsfsparkjob
from . import slurmsparkjob
from .lsfsparkjob import LSFSparkJob
from .slurmsparkjob import SLURMSparkJob


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def show_clusters():
    sparkjob.sparkjob().show_clusters()