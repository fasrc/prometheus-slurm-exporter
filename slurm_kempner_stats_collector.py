#!/usr/bin/python3

"""

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
            '--account=kempner_alvarez_lab,kempner_ba_lab,kempner_barak_lab,kempner_bsabatini_lab,kempner_dam_lab,kempner_dev,kempner_devState,kempner_fellows,kempner_gershman_lab,kempner_grads,kempner_h100State,kempner_hms,kempner_kdbrantley_lab,kempner_konkle_lab,kempner_krajan_lab,kempner_lab,kempner_murphy_lab,kempner_mzitnik_lab,kempner_pehlevan_lab,kempner_pslade_lab,kempner_requeueState,kempner_sham_lab,kempner_sompolinsky_lab,kempnerState,kempner_undergrads,kempner_users',
            '--Format=RestartCnt,PendingTime,Partition',
            '--noheader'
        ]
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
            #print('showq_data')
            #print(showq_data)
            self.process_showq_data(showq_data, partition)

        yield self.kempner

    def collect_showq_data(self, partition):
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
        self.kempner.add_metric([f"{partition}-cpu-used"], summary[4]) # cpus used
        self.kempner.add_metric([f"{partition}-cpu-total"], summary[6]) # cpus total
        self.kempner.add_metric([f"{partition}-gpu-used"], summary[11])
        self.kempner.add_metric([f"{partition}-gpu-total"], summary[13])
        self.kempner.add_metric([f"{partition}-node-used"], summary[18])
        self.kempner.add_metric([f"{partition}-node-total"], summary[20])
    

    def add_jobsummary_metrics(self, partition, summary):
        """Add metrics for GPU partitions."""
        self.kempner.add_metric([f"{partition}-job-total"], summary[2])
        self.kempner.add_metric([f"{partition}-job-active"], summary[5])
        self.kempner.add_metric([f"{partition}-job-idle"], summary[8])
        self.kempner.add_metric([f"{partition}-job-blocked"], summary[11])


if __name__ == "__main__":
    start_http_server(10002)
    REGISTRY.register(SlurmKempnerStatsCollector())
    while True:
        time.sleep(30)
