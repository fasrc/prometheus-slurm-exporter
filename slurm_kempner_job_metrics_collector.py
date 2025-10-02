#!/usr/bin/python3.11

"""
slurm_kempner_job_metrics_collector.py
Prometheus exporter for detailed SLURM job metrics.

This script:
- Periodically collects granular SLURM job and node data using scontrol, squeue, sacct, and sinfo commands.
- Exposes these metrics in Prometheus format at http://localhost:9100/metrics.
- Filters for Kempner partition jobs and nodes only (configurable)

"""

import sys, os
import subprocess
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

class SlurmJobNodeCollector(Collector):
    def __init__(self):
        pass

    def collect(self):
        job_state = GaugeMetricFamily('slurm_job_state', 'SLURM job state (1=RUNNING, 0=other)', labels=['job_id', 'state'])
        scontrol = GaugeMetricFamily('slurm_scontrol_raw', 'Raw SLURM job information from scontrol show job', labels=['job_id', 'scontrol_data'])
        jobs_per_partition = GaugeMetricFamily('slurm_jobs_per_partition', 'Number of jobs per SLURM partition', labels=['partition'])
        node_status = GaugeMetricFamily('slurm_node_status', 'SLURM node status (1=up, 0=down)', labels=['node', 'state'])

        # Run squeue only once - to get job list and states 
        try:
            job_output = self.run_cmd(['timeout', '-s', '9', '60s', '/usr/bin/squeue', '--noheader', '--format=%i %T %P'])
            partition_counts = {}
            
            for line in job_output.splitlines():
                if line.strip():
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        job_id, state, partition = parts[0], parts[1], parts[2]
                        
                        # Only process Kempner partitions
                        if 'kempner' not in partition.lower():
                            continue

                        # Add job state 
                        value = 1 if state == "RUNNING" else 0
                        job_state.add_metric([job_id, state], value)
                        
                        partition_counts[partition] = partition_counts.get(partition, 0) + 1
                        
                        # Try scontrol for each job 
                        try:
                            scontrol_data = self.run_cmd(['timeout', '-s', '9', '60s', 'scontrol', '-o', 'show', 'job', job_id])
                            cleaned_data = scontrol_data.replace('\n', ' ').replace('"', '\\"')
                            scontrol.add_metric([job_id, cleaned_data], 1)
                        except Exception as e:
                            print(f"Error getting scontrol data for job {job_id}: {e}")
                            scontrol.add_metric([job_id, "ERROR: scontrol failed"], 0)

            # Add partition counts
            for partition, count in partition_counts.items():
                jobs_per_partition.add_metric([partition], count)
                
        except Exception as e:
            print(f"Error collecting job data: {e}")

        # Try to collect node status
        try:
            sinfo_output = self.run_cmd(['timeout', '-s', '9', '60s', '/usr/bin/sinfo', '-Nh', '-o', '%N %T %R'])
            for line in sinfo_output.splitlines():
                parts = line.strip().split()
                if len(parts) >= 3:
                    node, state, partition = parts[0], parts[1], parts[2]
                    
                    # Only process Kempner partitions
                    if 'kempner' not in partition.lower():
                        continue

                    value = 1 if state.upper() == "UP" else 0
                    node_status.add_metric([node, state], value)
        except Exception as e:
            print(f"Error collecting node data: {e}")

        # Always yield metrics (even if empty)
        yield job_state
        yield scontrol  
        yield jobs_per_partition
        yield node_status

    def run_cmd(self, cmd):
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()

if __name__ == "__main__":
    start_http_server(9100)
    REGISTRY.register(SlurmJobNodeCollector())
    
    # For testing: run once and keep server alive for data dump
    print("Kempner job metrics collector started. Metrics available at http://localhost:9100/metrics")
    print("For testing - keeping server alive for 1 hour to allow data collection")
    time.sleep(3600)  # Keep alive for 1 hour, then exit
    
    # For production: uncomment the lines below and comment out the testing section above
    # while True:
    #     time.sleep(600)  # Collect metrics every 10 minutes
