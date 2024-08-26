#!/usr/bin/python3

"""
slurm_kempner_stats_collector.py
A script to collect  statistics from Slurm.
"""

import subprocess
import time
import os
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server

class SlurmKempnerStatsCollector:
    def __init__(self):
        self.kempner = GaugeMetricFamily('kempner', 'Stats for Kempner', labels=['field'])

    def run_command(self, command):
        """Run a shell command and return its output."""
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
            output, _ = proc.communicate()
            return output.strip().splitlines()
        except subprocess.SubprocessError as e:
            print(f"Error executing command {command}: {e}")
            return []

    def collect_slurm_data(self):
        """Collect Slurm job data."""
        command = [
            '/usr/bin/squeue',
            '-A kempner_alvarez_lab,kempner_ba_lab,kempner_barak_lab,kempner_bsabatini_lab,kempner_dam_lab,kempner_dev,kempner_devState,kempner_fellows,kempner_gershman_lab,kempner_grads,kempner_h100State,kempner_hms,kempner_kdbrantley_lab,kempner_konkle_lab,kempner_krajan_lab,kempner_lab,kempner_murphy_lab,kempner_mzitnik_lab,kempner_pehlevan_lab,kempner_pslade_lab,kempner_requeueState,kempner_sham_lab,kempner_sompolinsky_lab,kempnerState,kempner_undergrads,kempner_users',
            '--Format=RestartCnt,PendingTime,Partition',
            '--noheader'
        ]
        return self.run_command(command)

    def collect_showq_data(self, partition):
        """Collect data from showq for a specific partition."""
        command = ['/usr/local/bin/showq', '-s', '-p', partition]
        return self.run_command(command)

    def process_slurm_data(self, lines):
        """Process the collected Slurm job data."""
        rtot = ptot = jcnt = jkempner = 0
        partitions_of_interest = [ 
            'kempner',  'kempner_dev', 'kempner_h100', 'kempner_requeue'
        ]

        for line in lines:
            RestartCnt, PendingTime, Partition = line.split()
            rtot += int(RestartCnt)
            ptot += int(PendingTime)
            jcnt += 1
            if Partition in partitions_of_interest:
                jkempner += 1

        return rtot, ptot, jcnt, jkempner

    def collect(self):
        """Collect metrics and yield them to Prometheus."""
        # Collect and process Slurm job data
        slurm_data = self.collect_slurm_data()
        rtot, ptot, jcnt, jkempner = self.process_slurm_data(slurm_data)
       
        # Calculate averages and add metrics
        if jcnt > 0:
            self.kempner.add_metric(["restartave"], rtot / jcnt)
            self.kempner.add_metric(["pendingave"], ptot / jcnt)
        self.kempner.add_metric(["totkempnerjobs"], jcnt)
        self.kempner.add_metric(["kempnerpartjobs"], jkempner)

        # Collect data for specific partitions using showq
        partitions_of_interest = [ 
            'kempner',  'kempner_dev', 'kempner_h100', 'kempner_requeue'
        ]
        for partition in partitions_of_interest:
            showq_data = self.collect_showq_data(partition)
            self.process_showq_data(showq_data, partition)

        yield self.kempner

    def process_showq_data(self, lines, partition):
        """Process the collected showq data and add metrics."""
        for line in lines:
            if "cores" in line:
                summary = self.extract_summary(line)
                if "kempner" in partition:
                    self.add_gpu_metrics(summary)
                else:
                    self.add_compute_metrics(summary)
            elif "Idle" in line:
                summary = self.extract_summary(line)
                self.kempner.add_metric([f'{partition}'], summary[8])

    def extract_summary(self, line):
        """Extract and clean summary data from a line of showq output."""
        line = line.replace("(", " ").replace(")", " ")
        return line.split()

    def add_compute_metrics(self, summary):
        """Add metrics for compute partitions."""
        self.kempner.add_metric(["kccu"], summary[4])
        self.kempner.add_metric(["kcct"], summary[6])
        self.kempner.add_metric(["kcnu"], summary[18])
        self.kempner.add_metric(["kcnt"], summary[20])

    def add_gpu_metrics(self, summary):
        """Add metrics for GPU partitions."""
        self.kempner.add_metric(["kgcu"], summary[4])
        self.kempner.add_metric(["kgct"], summary[6])
        self.kempner.add_metric(["kggu"], summary[11])
        self.kempner.add_metric(["kggt"], summary[13])
        self.kempner.add_metric(["kgnu"], summary[18])
        self.kempner.add_metric(["kgnt"], summary[20])

if __name__ == "__main__":
    start_http_server(10002)
    REGISTRY.register(SlurmKempnerStatsCollector())
    while True:
        time.sleep(30)

