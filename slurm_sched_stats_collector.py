#!/usr/bin/python3.11

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
  os.path.join(os.path.abspath(os.path.dirname(__file__)))
)
external = os.path.join(prefix, 'external')
sys.path = [prefix, external] + sys.path

from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client.registry import Collector
from prometheus_client import start_http_server

class SlurmSchedStatsCollector(Collector):
  def __init__(self):
    pass
  def collect(self):
    try:
      proc = subprocess.Popen('sdiag', stdout=subprocess.PIPE, universal_newlines=True)
    except:
      return
    else:
      # Construct dictionary of stats
      sd = dict()
      pl = ""

      for line in proc.stdout:
        if "Remote" in line:
          break
        elif "Main" in line:
          pl = "m"
        elif "Backfilling" in line:
          pl = "b"
        elif ":" in line:
          line = line.replace(" ","").replace('\t',"").replace("(","").replace(")","")
          line = pl + line
          sd.update(dict(s.split(":", 1) for s in shlex.split(line) if ':' in s))

      # Slurmctld Stats
      sdiag = GaugeMetricFamily('sdiag', 'Stats from sdiag', labels=['field'])
     

      sdiag.add_metric(['server_thread_count'],sd['Serverthreadcount'])
      sdiag.add_metric(['agent_queue_size'],sd['Agentqueuesize'])
      sdiag.add_metric(['agent_queue_size'],sd['Agentqueuesize'])
  
      # Jobs Stats
      sdiag.add_metric(['jobs_submitted'],sd['Jobssubmitted'])
      sdiag.add_metric(['jobs_started'],sd['Jobsstarted'])
      sdiag.add_metric(['jobs_completed'],sd['Jobscompleted'])
      sdiag.add_metric(['jobs_canceled'],sd['Jobscanceled'])
      sdiag.add_metric(['jobs_failed'],sd['Jobsfailed'])
  
      # Main Scheduler Stats
      sdiag.add_metric(['main_last_cycle'],sd['mLastcycle'])
      sdiag.add_metric(['main_max_cycle'],sd['mMaxcycle'])
      sdiag.add_metric(['main_total_cycles'],sd['mTotalcycles'])
      sdiag.add_metric(['main_mean_cycle'],sd['mMeancycle'])
      sdiag.add_metric(['main_mean_depth_cycle'],sd['mMeandepthcycle'])
      sdiag.add_metric(['main_cycles_per_minute'],sd['mCyclesperminute'])  
      sdiag.add_metric(['main_last_queue_length'],sd['mLastqueuelength'])
  
      # Backfilling stats
      sdiag.add_metric(['bf_total_jobs_since_slurm_start'],sd['bTotalbackfilledjobssincelastslurmstart'])
      sdiag.add_metric(['bf_total_jobs_since_cycle_start'],sd['bTotalbackfilledjobssincelaststatscyclestart'])
      sdiag.add_metric(['bf_total_cycles'],sd['bTotalcycles'])
      sdiag.add_metric(['bf_last_cycle'],sd['bLastcycle'])
      sdiag.add_metric(['bf_max_cycle'],sd['bMaxcycle'])
      sdiag.add_metric(['bf_queue_length'],sd['bLastqueuelength'])
      sdiag.add_metric(['bf_mean_cycle'], (sd['bMeancycle'] if 'bMeancycle' in sd else 0))
      sdiag.add_metric(['bf_depth_mean'], (sd['bDepthMean'] if 'bDepthMean' in sd else 0))
      sdiag.add_metric(['bf_depth_mean_try'], (sd['bDepthMeantrydepth'] if 'bDepthMeantrydepth' in sd else 0))
      sdiag.add_metric(['bf_queue_length_mean'], (sd['bQueuelengthmean'] if 'bQueuelengthmean' in sd else 0))
      sdiag.add_metric(['bf_last_depth_cycle'],sd['bLastdepthcycle'])
      sdiag.add_metric(['bf_last_depth_cycle_try'],sd['bLastdepthcycletrysched'])
      yield sdiag

if __name__ == "__main__":
  start_http_server(9001)
  REGISTRY.register(SlurmSchedStatsCollector())
  while True: 
    # period between collection
    time.sleep(30)
