#!/usr/bin/python3.11

"""
slurm_kempner_job_metrics_collector.py
Prometheus exporter for detailed SLURM job metrics.

This script:
- Periodically collects granular SLURM data using:
        - 'sinfo' twice (get kempner partitions, get node status) 
        - 'sacct' once (get job data)
- Exposes these metrics in Prometheus format at http://localhost:9009/metrics.
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
        # Fallback hardcoded list in case dynamic discovery fails
        self.fallback_kempner_partitions = ['kempner', 'kempner_dev', 'kempner_h100', 'kempner_requeue']

    # Get all partitions with 'kempner'
    def get_kempner_partitions(self):
        try:
            output = self.run_cmd(['timeout', '-s', '9', '30s', '/usr/bin/sinfo', '-h', '-o', '%R'])
            partitions = []
            seen = set()
            
            for line in output.splitlines():
                partition = line.strip()
                if partition and 'kempner' in partition.lower() and partition not in seen:
                    partitions.append(partition)
                    seen.add(partition)
            
            if partitions:
                return ','.join(partitions)
            else:
                return ','.join(self.fallback_kempner_partitions)
                
        except Exception as e:
            print(f"Error discovering partitions, using fallback: {e}")
            return ','.join(self.fallback_kempner_partitions)

    def collect(self):
        job_state = GaugeMetricFamily('slurm_job_state', 'SLURM job state (1=RUNNING, 0=other)', labels=['job_id', 'state'])
        job_details = GaugeMetricFamily('slurm_job_details', 'Comprehensive SLURM job information from sacct', 
                                      labels=['job_id', 'user', 'partition', 'account', 'state', 'tres_cpu', 'tres_mem', 'tres_gres', 'start_time', 'end_time', 'elapsed', 'alloc_tres', 'node_list', 'ncpus'])
        jobs_per_partition = GaugeMetricFamily('slurm_jobs_per_partition', 'Number of jobs per SLURM partition', labels=['partition'])
        node_status = GaugeMetricFamily('slurm_node_status', 'SLURM node status (1=up, 0=down)', labels=['node', 'state'])

        kempner_partitions = self.get_kempner_partitions()

        # Get job data from sacct 
        try:
            job_output = self.run_cmd(['timeout', '-s', '9', '60s', '/usr/bin/sacct', 
                                     '--parsable2', '--noheader', '--allusers', '-X',
                                     '--partition=' + kempner_partitions,
                                     '--format=JobID,User,Partition,Account,State,AllocCPUS,ReqMem,ReqTRES,Start,End,Elapsed,AllocTRES,NodeList,NCPUs',
                                     '--starttime=today'])
            partition_counts = {}
            
            for line in job_output.splitlines():
                if line.strip():
                    parts = line.strip().split('|')  
                    if len(parts) >= 14:
                        job_id, user, partition, account, state, cpu, memory, tres, start_time, end_time, elapsed, alloc_tres, node_list, ncpus = parts[0:14]
                        
                        # Skip empty or invalid entries
                        if not job_id or not state or not partition:
                            continue

                        # Add job state
                        value = 1 if state == "RUNNING" else 0
                        job_state.add_metric([job_id, state], value)
                        
                        # Add job details 
                        job_details.add_metric([
                            job_id, user, partition, account, state, 
                            cpu or "0", memory or "0", tres or "none", 
                            start_time or "unknown", end_time or "unknown",
                            elapsed or "0", alloc_tres or "none", node_list or "unknown", ncpus or "0"
                        ], 1)
                        
                        partition_counts[partition] = partition_counts.get(partition, 0) + 1

            # Add partition counts
            for partition, count in partition_counts.items():
                jobs_per_partition.add_metric([partition], count)
                
        except Exception as e:
            print(f"Error collecting job data: {e}")

        # Set node status
        try:
            sinfo_output = self.run_cmd(['timeout', '-s', '9', '60s', '/usr/bin/sinfo', 
                                       '-Nh', '-o', '%N %T %R', 
                                       '-p', kempner_partitions])
            for line in sinfo_output.splitlines():
                parts = line.strip().split()
                if len(parts) >= 3:
                    node, state, partition = parts[0], parts[1], parts[2]
                    
                    value = 1 if state.upper() == "UP" else 0
                    node_status.add_metric([node, state], value)
        except Exception as e:
            print(f"Error collecting node data: {e}")

        # Always yield metrics (even if empty)
        yield job_state
        yield job_details  
        yield jobs_per_partition
        yield node_status

    def run_cmd(self, cmd):
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()

if __name__ == "__main__":
    start_http_server(9009)
    REGISTRY.register(SlurmJobNodeCollector())
    
    print("Kempner job metrics collector started. Metrics available at http://localhost:9009/metrics")
    
    # Collect metrics every 30 seconds 
    while True:
        time.sleep(30)