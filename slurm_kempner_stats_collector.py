#!/usr/bin/python3

"""

A script to collect  statistics from Slurm.
"""

import subprocess
import time
import sys,os
from os import path

prefix = os.path.normpath(
  os.path.join(os.path.abspath(os.path.dirname(__file__)))
)
external = os.path.join(prefix, 'external')
sys.path = [prefix, external] + sys.path

from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server

class SlurmKempnerStatsCollector:
    def __init__(self):
        self.part_kemp = [ 'kempner',  'kempner_dev', 'kempner_h100', 'kempner_requeue' ]
        self.metric = {}

    def run_command(self, command):
        """Run a shell command and return its output."""
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
            output, _ = proc.communicate()
            return output.strip().splitlines()
        except subprocess.SubprocessError as e:
            print(f"Error executing command {command}: {e}")
            return []


    def collect(self):
        """Collect metrics and yield them to Prometheus."""
        k_partition = GaugeMetricFamily('k_partition', 'Stats for Kempner Partitions', labels=['field'])
       
        for part in self.part_kemp:
            showq_data = self.get_showq_data(part)
            self.process_showq_data(showq_data, part)
        for key, value in self.metric.items():
            k_partition.add_metric([key.lower()], value)
        jobt = 0
        for part in self.part_kemp:
            jobt  +=  int(self.metric[f"{part}-jt"])
   
        k_partition.add_metric(["job_total"], jobt)
        yield k_partition

    def get_showq_data(self, partition):
        """Collect data from showq for a specific partition."""
        command = ['/usr/local/bin/showq', '-s', '-p', partition]
        return self.run_command(command)
    
    def process_showq_data(self, lines, partition):
        """Process the collected showq data and add metrics."""
        for line in lines:
            summary = self.extract_summary(line)
            if "cores" and "gpus" in line:
                self.add_gpusummary_metrics(partition, summary)
            if "Active" and "Idle" in line:
                self.add_jobsummary_metrics(partition, summary)


    def extract_summary(self, line):
        """Extract and clean summary data from a line of showq output."""
        line = line.replace("(", " ").replace(")", " ")
        return line.split()
    
    
    def add_gpusummary_metrics(self, partition, summary):
        """Add metrics for GPU partitions."""
        self.metric[f"{partition}-cu"]= summary[4] # cpus used
        self.metric[f"{partition}-ct"]= summary[6] # cpus total
        self.metric[f"{partition}-gu"]= summary[11]
        self.metric[f"{partition}-gt"]= summary[13]
        self.metric[f"{partition}-nu"]= summary[18]
        self.metric[f"{partition}-nt"]= summary[20]
    

    def add_jobsummary_metrics(self, partition, summary):
        """Add metrics for GPU partitions."""
        self.metric[f"{partition}-jt"]= summary[2]
        self.metric[f"{partition}-ja"]= summary[5]
        self.metric[f"{partition}-ji"]= summary[8]
        self.metric[f"{partition}-jb"]= summary[11]

if __name__ == "__main__":
    start_http_server(9006)
    REGISTRY.register(SlurmKempnerStatsCollector())
    while True:
        time.sleep(30)

