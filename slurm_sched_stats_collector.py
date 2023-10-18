#!/usr/bin/python3

"""
slurm_sched_stats_collector.py
A script that uses Popen to get the slurm scheduler statistics.
"""

import sys,os,json
import shlex,subprocess
import time
from os import path
import yaml

prefix = os.path.normpath(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
)
external = os.path.join(prefix, 'external')
sys.path = [prefix, external] + sys.path

from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily
from prometheus_client import start_http_server
