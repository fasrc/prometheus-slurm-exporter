#!/usr/bin/python3

"""
A script to get general Slurm cluster statistics.
"""

import sys,os
import subprocess
import shlex
import time
from os import path

prefix = os.path.normpath(
  os.path.join(os.path.abspath(os.path.dirname(__file__)))
)
external = os.path.join(prefix, 'external')
sys.path = [prefix, external] + sys.path

from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server

class SlurmClusterStatusCollector:
    def __init__(self):
        self.metrics = self.initialize_metrics()
        self.t2g = 93.25  # Translation from TRES to GFLOPs
        #Current TRES weights
        self.wcpu = {'genoa': 0.6, 'icelake': 1.15}
        self.wgpu = {'a100': 209.1, 'a100-mig': 29.9, 'h100': 546.9}

    def initialize_metrics(self):
        """Initialize all the metrics and counters."""
        metrics = {
"CPUTot":0, "CPULoad":0, "CPUAlloc":0, "RealMem":0, "MemAlloc":0, "MemLoad":0, "GPUTot":0, "GPUAlloc":0, "NodeTot":0, "IDLETot":0, "DOWNTot":0, "DRAINTot":0, "MIXEDTot":0, "ALLOCTot":0, "RESTot":0, "COMPTot":0, "PLANNEDTot":0, "IDLECPU":0, "MIXEDCPU":0, "ALLOCCPU":0, "COMPCPU":0, "RESCPU":0, "PLANNEDCPU":0, "DRAINCPU":0, "DOWNCPU":0, "IDLEMem":0, "MIXEDMem":0, "ALLOCMem":0, "COMPMem":0, "PLANNEDMem":0, "DRAINMem":0, "DOWNMem":0, "RESMem":0, "IDLEGPU":0, "MIXEDGPU":0, "ALLOCGPU":0, "COMPGPU":0, "DRAINGPU":0, "DOWNGPU":0, "RESGPU":0, "PLANNEDGPU":0, "PerAlloc":0 
        }
        cpu_gpu_types = ["genoa", "icelake", "a100", "a100-mig", "h100"]
        for ctype in cpu_gpu_types:
            metrics[f"tcpu_{ctype}"] = 0
            metrics[f"ucpu_{ctype}"] = 0
            metrics[f"tgpu_{ctype}"] = 0
            metrics[f"ugpu_{ctype}"] = 0
            metrics[f"umem_{ctype}"] = 0
        return metrics

    def run_command(self, command):
        """Run a command and return its output."""
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
            #for line in proc.stdout:
            #    print(line)
            return proc.stdout
        except subprocess.SubprocessError as e:
            print(f"Error running command {command}: {e}")
            return []

    def process_node_info(self, node, cfgtres, alloctres):
        """Update the metrics based on node, cfgTRES, and allocTRES."""
        numgpu = int(cfgtres.get('gres/gpu', 0))
        agpu = int(alloctres.get('gres/gpu', 0))
        self.metrics['NodeTot'] += 1
        self.metrics['CPUTot'] += int(node['CPUTot'])
        self.metrics['CPUAlloc'] += int(node['CPUAlloc'])
        self.metrics['RealMem'] += int(node['RealMemory'])
        self.metrics['MemAlloc'] += min(int(node['AllocMem']), int(node['RealMemory']))
        self.metrics['MemLoad'] += int(node['RealMemory']) - int(node['FreeMem']) if node['FreeMem'] != 'N/A' else 0
        self.metrics['CPULoad'] += float(node['CPULoad']) if node['CPULoad'] != 'N/A' else 0
        self.metrics['GPUTot'] += numgpu
        self.metrics['GPUAlloc'] += agpu
        for f in node['AvailableFeatures'].split(","):
            if f in self.wcpu:
                self.metrics[f"tcpu_{f}"] += int(node['CPUTot'])
                self.metrics[f"ucpu_{f}"] += int(node['CPUAlloc'])
                self.metrics[f"umem_{f}"] += int(node['CPUTot']) * int(node['AllocMem']) / int(node['RealMemory'])
            if f in self.wgpu:
                self.metrics[f"tgpu_{f}"] += numgpu
                self.metrics[f"ugpu_{f}"] += agpu

    def update_state_counters(self, node):
        """Update the counters based on the node's state."""
        state = node['State']
        for status in ["IDLE", "MIXED", "ALLOC", "RES", "COMP", "DRAIN", "DOWN"]:
            if status in state:
                self.metrics[f"{status}Tot"] += 1
                self.metrics[f"{status}CPU"] += int(node['CPUTot'])
                self.metrics[f"{status}Mem"] += int(node['RealMemory'])
                self.metrics[f"{status}GPU"] += int(self.metrics['GPUTot'])

    def calculate_totals(self):
        """Calculate totals and FLOPs for CPU, GPU, and memory."""
        tcputres = sum(float(self.wcpu[ctype]) * float(self.metrics[f"tcpu_{ctype}"]) for ctype in self.wcpu)
        tgputres = sum(float(self.wgpu[gtype]) * float(self.metrics[f"tgpu_{gtype}"]) for gtype in self.wgpu)
        ucputres = sum(float(self.wcpu[ctype]) * float(self.metrics[f"ucpu_{ctype}"]) for ctype in self.wcpu)
        ugputres = sum(float(self.wgpu[gtype]) * float(self.metrics[f"ugpu_{gtype}"]) for gtype in self.wgpu)

        self.metrics['tcputres'] = tcputres
        self.metrics['tgputres'] = tgputres
        self.metrics['ucputres'] = ucputres
        self.metrics['ugputres'] = ugputres
        self.metrics['tgflops'] = self.t2g * (tcputres + tgputres)
        self.metrics['ugflops'] = self.t2g * (ucputres + ugputres)

    def collect_metrics(self):
        """Collect all Slurm metrics."""
        for line in self.run_command(['scontrol', '-o', 'show', 'node']):
            if "kempner" in line:
                node, cfgtres, alloctres = self.parse_node(line)
                self.process_node_info(node, cfgtres, alloctres)
                self.update_state_counters(node)
        self.calculate_totals()

    def parse_node(self, line):
        """Parse a node line from scontrol output."""
        node = dict(s.split("=", 1) for s in shlex.split(line) if '=' in s)
        cfgtres = dict(s.split("=", 1) for s in shlex.split(node['CfgTRES'].replace(",", " ")) if '=' in s)
        alloctres = dict(s.split("=", 1) for s in shlex.split(node['AllocTRES'].replace(",", " ")) if '=' in s)
        return node, cfgtres, alloctres

    def collect(self):
        """Prometheus collector interface."""
        self.collect_metrics()
        k_lsload = GaugeMetricFamily('k_lsload', 'Aggregate Cluster Node Stats', labels=['field'])
        for key, value in self.metrics.items():
            k_lsload.add_metric([key], value)
        yield k_lsload

if __name__ == "__main__":
    start_http_server(9005)
    REGISTRY.register(SlurmClusterStatusCollector())
    while True:
        time.sleep(30)
