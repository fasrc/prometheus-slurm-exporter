#!/usr/bin/python3

"""
slurm_cluster_status_collector.py
A script to get general slurm cluster statistics.
"""

import sys,os,json,subprocess,shlex
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

class SlurmClusterStatusCollector(Collector):
  def __init__(self):
    pass
  def collect(self):
    try:
      proc = subprocess.Popen([
      'scontrol',
      '-o', 'show', 'node' 
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      return
    else:

      #Zero out counters
      CPUTot=0
      CPULoad=0
      CPUAlloc=0
      RealMem=0
      MemAlloc=0
      MemLoad=0
      GPUTot=0
      GPUAlloc=0
      NodeTot=0
      IDLETot=0
      DOWNTot=0
      DRAINTot=0
      MIXEDTot=0
      ALLOCTot=0
      RESTot=0
      COMPTot=0
      PLANNEDTot=0
      IDLECPU=0
      MIXEDCPU=0
      ALLOCCPU=0
      COMPCPU=0
      RESCPU=0
      PLANNEDCPU=0
      DRAINCPU=0
      DOWNCPU=0
      IDLEMem=0
      MIXEDMem=0
      ALLOCMem=0
      COMPMem=0
      PLANNEDMem=0
      DRAINMem=0
      DOWNMem=0
      RESMem=0
      IDLEGPU=0
      MIXEDGPU=0
      ALLOCGPU=0
      COMPGPU=0
      DRAINGPU=0
      DOWNGPU=0
      RESGPU=0
      PLANNEDGPU=0
      PerAlloc=0
      
      tcpu={'genoa': 0, 'icelake': 0}
      ucpu={'genoa': 0, 'icelake': 0}
      tgpu={'a100': 0, 'a100-mig': 0, 'h100': 0}
      ugpu={'a100': 0, 'a100-mig': 0, 'h100': 0}
      umem={'genoa': 0,  'icelake': 0}

      #Current translation from TRES to Double Precision GFLOps
      t2g=93.25

      #Current TRES weights
      wcpu={'genoa': 0.6, 'icelake': 1.15}
      wgpu={'a100': 209.1, 'a100-mig': 29.9, 'h100': 546.9}

      #Cycle through each node
      for line in proc.stdout:
          if "kempner" in line:
            #Turn node information into a hash
            node = dict(s.split("=", 1) for s in shlex.split(line) if '=' in s)

            #Break out TRES so we can get GPU info.
            cfgtres = dict(s.split("=", 1) for s in shlex.split(node['CfgTRES'].replace(",", " ")) if '=' in s)
            alloctres = dict(s.split("=", 1) for s in shlex.split(node['AllocTRES'].replace(",", " ")) if '=' in s)

            #Test for GPU
            if 'gres/gpu' in cfgtres:
              numgpu=int(cfgtres['gres/gpu'])
              if 'gres/gpu' in alloctres:
                agpu=int(alloctres['gres/gpu'])
              else:
                agpu=0
            else:
              numgpu=0
              agpu=0

            #Cataloging all the different CPU's and GPU's
            for f in node['AvailableFeatures'].split(","):
              if f in tcpu:
                tcpu[f]=tcpu[f]+int(node['CPUTot'])
                ucpu[f]=ucpu[f]+int(node['CPUAlloc'])
                umem[f]=umem[f]+float(node['CPUTot'])*float(node['AllocMem'])/float(node['RealMemory'])
                cflops=t2g*float(wcpu[f])*int(node['CPUAlloc'])
              if f in tgpu:
                tgpu[f]=tgpu[f]+numgpu
                ugpu[f]=ugpu[f]+agpu
                gflops=t2g*float(wgpu[f])*agpu

            #Counters.
            NodeTot=NodeTot+1
            CPUTot=CPUTot+int(node['CPUTot'])
            CPUAlloc=CPUAlloc+int(node['CPUAlloc'])
            if node['CPULoad'] != 'N/A':
              CPULoad=CPULoad+float(node['CPULoad'])
            RealMem=RealMem+int(node['RealMemory'])
            MemAlloc=MemAlloc+min(int(node['AllocMem']),int(node['RealMemory']))
            #Slurm only lists actual free memory so we have to back calculate how much is actually used.
            if node['FreeMem'] != 'N/A':
              MemLoad=MemLoad+(int(node['RealMemory'])-int(node['FreeMem']))

            GPUTot=GPUTot+numgpu
            GPUAlloc=GPUAlloc+agpu

            #Count how many nodes are in each state
            if node['State'] == 'IDLE' or node['State'] == 'IDLE+COMPLETING' or node['State'] == 'IDLE+POWER' or node['State'] == 'IDLE#':
              IDLETot=IDLETot+1
              IDLECPU=IDLECPU+int(node['CPUTot'])
              IDLEMem=IDLEMem+int(node['RealMemory'])
              IDLEGPU=IDLEGPU+numgpu
            if node['State'] == 'MIXED' or node['State'] == 'MIXED+COMPLETING' or node['State'] == 'MIXED#':
              MIXEDTot=MIXEDTot+1
              MIXEDCPU=MIXEDCPU+int(node['CPUTot'])
              MIXEDMem=MIXEDMem+int(node['RealMemory'])
              MIXEDGPU=MIXEDGPU+numgpu
            if node['State'] == 'ALLOCATED' or node['State'] == 'ALLOCATED+COMPLETING':
              ALLOCTot=ALLOCTot+1
              ALLOCCPU=ALLOCCPU+int(node['CPUTot'])
              ALLOCMem=ALLOCMem+int(node['RealMemory'])
              ALLOCGPU=ALLOCGPU+numgpu
            if node['State'] == 'IDLE+PLANNED' or node['State'] == 'MIXED+PLANNED':
              PLANNEDTot=PLANNEDTot+1
              PLANNEDCPU=PLANNEDCPU+int(node['CPUTot'])
              PLANNEDMem=PLANNEDMem+int(node['RealMemory'])
              PLANNEDGPU=PLANNEDGPU+numgpu
            if "RESERVED" in node['State']:
              RESTot=RESTot+1
              RESCPU=RESCPU+int(node['CPUTot'])
              RESMem=RESMem+int(node['RealMemory'])
              RESGPU=RESGPU+numgpu
            if "COMPLETING" in node['State']:
              COMPTot=COMPTot+1
              COMPCPU=COMPCPU+int(node['CPUTot'])
              COMPMem=COMPMem+int(node['RealMemory'])
              COMPGPU=COMPGPU+numgpu
            if "DRAIN" in node['State'] and node['State'] != 'IDLE+DRAIN' and node['State'] != 'DOWN+DRAIN':
              DRAINTot=DRAINTot+1
              DRAINCPU=DRAINCPU+int(node['CPUTot'])
              DRAINMem=DRAINMem+int(node['RealMemory'])
              DRAINGPU=DRAINGPU+numgpu
            if "DOWN" in node['State'] or node['State'] == 'IDLE+DRAIN':
              DOWNTot=DOWNTot+1
              DOWNCPU=DOWNCPU+int(node['CPUTot'])
              DOWNMem=DOWNMem+int(node['RealMemory'])
              DOWNGPU=DOWNGPU+numgpu

            #Calculate percent occupation of all nodes.  Some nodes may have few cores used but all their memory allocated.
            #Thus the node is fully used even though it is not labelled Alloc.  This metric is an attempt to count this properly.
            #Similarly if all the GPU's on a gpu node are used it is fully utilized even though CPU and Mem may still be available.
            PerAlloc=PerAlloc+max(float(node['CPUAlloc'])/float(node['CPUTot']),min(float(node['AllocMem']),float(node['RealMemory']))/float(node['RealMemory']),float(agpu)/max(1,float(numgpu)))

      #Calculate Total TRES and Total FLOps
      #This is Harvard specific for the weightings.  Update to match what you need.
      tcputres= float(wcpu['genoa'])*float(tcpu['genoa'])+float(wcpu['icelake'])*float(tcpu['icelake'])
      tmemtres=tcputres
      tgputres=float(wgpu['a100'])*float(tgpu['a100'])+float(wgpu['a100-mig'])*float(tgpu['a100-mig'])+float(wgpu['h100'])*float(tgpu['h100'])
      ucputres=float(wcpu['genoa'])*float(ucpu['genoa'])+float(wcpu['icelake'])*float(ucpu['icelake'])
      umemtres=float(wcpu['genoa'])*float(umem['genoa'])+float(wcpu['icelake'])*float(umem['icelake'])
      ugputres=float(wgpu['a100'])*float(ugpu['a100'])+float(wgpu['a100-mig'])*float(ugpu['a100-mig'])+float(wgpu['h100'])*float(ugpu['h100'])

      ttres=tcputres+tmemtres+tgputres
      utres=ucputres+umemtres+ugputres

      tcgflops=t2g*tcputres
      ucgflops=t2g*ucputres
      tggflops=t2g*tgputres
      uggflops=t2g*ugputres

      tgflops=tcgflops+tggflops
      ugflops=ucgflops+uggflops

      #Ship it.
      k_lsload = GaugeMetricFamily('k_lsload', 'Aggregate Cluster Node Stats', labels=['field'])
      k_lsload.add_metric(["nodetot"],NodeTot)
      k_lsload.add_metric(["cputot"],CPUTot)
      k_lsload.add_metric(["cpualloc"],CPUAlloc)
      k_lsload.add_metric(["cpuload"],CPULoad)
      k_lsload.add_metric(["realmem"],RealMem)
      k_lsload.add_metric(["memalloc"],MemAlloc)
      k_lsload.add_metric(["memload"],MemLoad)
      k_lsload.add_metric(["gputot"],GPUTot)
      k_lsload.add_metric(["gpualloc"],GPUAlloc)
      k_lsload.add_metric(["idletot"],IDLETot)
      k_lsload.add_metric(["downtot"],DOWNTot)
      k_lsload.add_metric(["draintot"],DRAINTot)
      k_lsload.add_metric(["mixedtot"],MIXEDTot)
      k_lsload.add_metric(["alloctot"],ALLOCTot)
      k_lsload.add_metric(["comptot"],COMPTot)
      k_lsload.add_metric(["restot"],RESTot)
      k_lsload.add_metric(["plannedtot"],PLANNEDTot)
      k_lsload.add_metric(["idlecpu"],IDLECPU)
      k_lsload.add_metric(["downcpu"],DOWNCPU)
      k_lsload.add_metric(["draincpu"],DRAINCPU)
      k_lsload.add_metric(["mixedcpu"],MIXEDCPU)
      k_lsload.add_metric(["alloccpu"],ALLOCCPU)
      k_lsload.add_metric(["compcpu"],COMPCPU)
      k_lsload.add_metric(["rescpu"],RESCPU)
      k_lsload.add_metric(["plannedcpu"],PLANNEDCPU)
      k_lsload.add_metric(["idlemem"],IDLEMem)
      k_lsload.add_metric(["downmem"],DOWNMem)
      k_lsload.add_metric(["drainmem"],DRAINMem)
      k_lsload.add_metric(["mixedmem"],MIXEDMem)
      k_lsload.add_metric(["allocmem"],ALLOCMem)
      k_lsload.add_metric(["compmem"],COMPMem)
      k_lsload.add_metric(["resmem"],RESMem)
      k_lsload.add_metric(["plannedmem"],PLANNEDMem)
      k_lsload.add_metric(["idlegpu"],IDLEGPU)
      k_lsload.add_metric(["downgpu"],DOWNGPU)
      k_lsload.add_metric(["draingpu"],DRAINGPU)
      k_lsload.add_metric(["mixedgpu"],MIXEDGPU)
      k_lsload.add_metric(["allocgpu"],ALLOCGPU)
      k_lsload.add_metric(["compgpu"],COMPGPU)
      k_lsload.add_metric(["resgpu"],RESGPU)
      k_lsload.add_metric(["plannedgpu"],PLANNEDGPU)
      k_lsload.add_metric(["peralloc"],PerAlloc)
      k_lsload.add_metric(["tcpugenoa"],tcpu['genoa'])
      k_lsload.add_metric(["tcpuicelake"],tcpu['icelake'])
      k_lsload.add_metric(["tgpua100"],tgpu['a100'])
      k_lsload.add_metric(["tgpua100mig"],tgpu['a100-mig'])
      k_lsload.add_metric(["tgpuh100"],tgpu['h100'])
      k_lsload.add_metric(["ucpugenoa"],ucpu['genoa'])
      k_lsload.add_metric(["ucpuicelake"],ucpu['icelake'])
      k_lsload.add_metric(["ugpua100"],ugpu['a100'])
      k_lsload.add_metric(["ugpua100mig"],ugpu['a100-mig'])
      k_lsload.add_metric(["ugpuh100"],ugpu['h100'])
      k_lsload.add_metric(["umemgenoa"],umem['genoa'])
      k_lsload.add_metric(["umemicelake"],umem['icelake'])
      k_lsload.add_metric(["tcputres"],tcputres)
      k_lsload.add_metric(["tgputres"],tgputres)
      k_lsload.add_metric(["tmemtres"],tmemtres)
      k_lsload.add_metric(["ucputres"],ucputres)
      k_lsload.add_metric(["ugputres"],ugputres)
      k_lsload.add_metric(["umemtres"],umemtres)
      k_lsload.add_metric(["ttres"],ttres)
      k_lsload.add_metric(["utres"],utres)
      k_lsload.add_metric(["tcgflops"],tcgflops)
      k_lsload.add_metric(["tggflops"],tggflops)
      k_lsload.add_metric(["ucgflops"],ucgflops)
      k_lsload.add_metric(["uggflops"],uggflops)
      k_lsload.add_metric(["tgflops"],tgflops)
      k_lsload.add_metric(["ugflops"],ugflops)
      yield k_lsload

if __name__ == "__main__":
  start_http_server(10000)
  REGISTRY.register(SlurmClusterStatusCollector())
  while True: 
    # period between collection
    time.sleep(30)

