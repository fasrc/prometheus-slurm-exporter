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
from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily
from prometheus_client import start_http_server
