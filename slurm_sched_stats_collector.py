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
      sdiag = GaugeMetricFamily('sdiag', 'Stats from sdiag')
     

      sdiag.add_metric(['server_thread_count'],sd['Serverthreadcount'])
#      self.publish('agent_queue_size',sd['Agentqueuesize'])
#  
#      # Jobs Stats
#      self.publish('jobs_submitted',sd['Jobssubmitted'])
#      self.publish('jobs_started',sd['Jobsstarted'])
#      self.publish('jobs_completed',sd['Jobscompleted'])
#      self.publish('jobs_canceled',sd['Jobscanceled'])
#      self.publish('jobs_failed',sd['Jobsfailed'])
#  
#      # Main Scheduler Stats
#      self.publish('main_last_cycle',sd['mLastcycle'])
#      self.publish('main_max_cycle',sd['mMaxcycle'])
#      self.publish('main_total_cycles',sd['mTotalcycles'])
#      self.publish('main_mean_cycle',sd['mMeancycle'])
#      self.publish('main_mean_depth_cycle',sd['mMeandepthcycle'])
#      self.publish('main_cycles_per_minute',sd['mCyclesperminute'])  
#      self.publish('main_last_queue_length',sd['mLastqueuelength'])
#  
#      # Backfilling stats
#      self.publish('bf_total_jobs_since_slurm_start',sd['bTotalbackfilledjobssincelastslurmstart'])
#      self.publish('bf_total_jobs_since_cycle_start',sd['bTotalbackfilledjobssincelaststatscyclestart'])
#      self.publish('bf_total_cycles',sd['bTotalcycles'])
#      self.publish('bf_last_cycle',sd['bLastcycle'])
#      self.publish('bf_max_cycle',sd['bMaxcycle'])
#      self.publish('bf_queue_length',sd['bLastqueuelength'])
#      bMeancycle=''
#      bDepthMean=''
#      bDepthMeantrydepth=''
#      bQueuelengthmean=''
#      self.publish('bf_mean_cycle', (sd['bMeancycle'] if bMeancycle in sd['bMeancycle'] else 0))
#      self.publish('bf_depth_mean', (sd['bDepthMean'] if bDepthMean in sd['bDepthMean'] else 0))
#      self.publish('bf_depth_mean_try', (sd['bDepthMeantrydepth'] if bDepthMeantrydepth in sd['bDepthMeantrydepth'] else 0))
#      self.publish('bf_queue_length_mean', (sd['bQueuelengthmean'] if bQueuelengthmean in sd['bQueuelengthmean'] else 0))
#      self.publish('bf_last_depth_cycle',sd['bLastdepthcycle'])
#      self.publish('bf_last_depth_cycle_try',sd['bLastdepthcycletrysched'])

if __name__ == "__main__":
  start_http_server(9000)
  REGISTRY.register(SlurmSchedStatsCollector())
  while True: 
    # period between collection
    time.sleep(30)
