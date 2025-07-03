#!/usr/bin/python3.11

"""
slurm_sshare_collector.py
A script that gets slurm sshare statistics.
"""

import sys,os,json,subprocess
import time
from os import path

prefix = os.path.normpath(
  os.path.join(os.path.abspath(os.path.dirname(__file__)))
)
external = os.path.join(prefix, 'external')
sys.path = [prefix, external] + sys.path

from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client.registry import Collector
from prometheus_client import start_http_server

class SlurmSshareCollector(Collector):
  def __init__(self):
    pass
  def collect(self):
    try:
      # sshare command we will use to get the data
      proc = subprocess.Popen([
      'sshare',
      '-ahP', '--format=User,Account,RawShares,NormShares,RawUsage,NormUsage,Fairshare'
      ], stdout=subprocess.PIPE, universal_newlines=True)
    except:
      return
    else:
      sshare = GaugeMetricFamily('sshare', 'Stats from sshare', labels=['account','user','field'])
      for line in proc.stdout:
        (User, Account, RawShares, NormShares, RawUsage, NormUsage, Fairshare) = line.strip().split('|')
        Account=Account.replace(" ","")
        User=User.replace(" ","")
        # Need to deal with users that are set to parent for their Shares.
        if User == "" and Account:
          RawSharesAccount=RawShares
        if RawShares == 'parent':
          RawShares=RawSharesAccount
        if NormShares == "":
          NormShares=0
        if User and Account:
          sshare.add_metric([Account,User,'rawshares'],RawShares)
          sshare.add_metric([Account,User,'normshares'],NormShares)
          sshare.add_metric([Account,User,'rawusage'],RawUsage)
          sshare.add_metric([Account,User,'normusage'],NormUsage)
          sshare.add_metric([Account,User,'fairshare'],Fairshare)
      yield sshare

if __name__ == "__main__":
  start_http_server(9003)
  REGISTRY.register(SlurmSshareCollector())
  while True: 
    # period between collection
    time.sleep(55)
