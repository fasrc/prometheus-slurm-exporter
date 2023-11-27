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

      tcpu={'haswell': 0, 'broadwell': 0, 'skylake': 0, 'milan': 0, 'rome': 0, 'cascadelake': 0, 'icelake': 0}
      ucpu={'haswell': 0, 'broadwell': 0, 'skylake': 0, 'milan': 0, 'rome': 0, 'cascadelake': 0, 'icelake': 0}
      tgpu={'titanx': 0, 'rtx2080ti': 0, 'v100': 0, 'a40': 0, 'a100': 0, 'a100-mig': 0}
      ugpu={'titanx': 0, 'rtx2080ti': 0, 'v100': 0, 'a40': 0, 'a100': 0, 'a100-mig': 0}
      umem={'haswell': 0, 'broadwell': 0, 'skylake': 0, 'milan': 0, 'rome': 0, 'cascadelake': 0, 'icelake': 0}

      #Cycle through each node
      for line in proc.stdout:
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
          if f in tgpu:
            tgpu[f]=tgpu[f]+numgpu
            ugpu[f]=ugpu[f]+agpu


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
      tcputres=0.4*float(tcpu['haswell']+tcpu['broadwell'])+0.5*float(tcpu['skylake'])+0.5*float(tcpu['milan'])+0.8*float(tcpu['rome'])+1.0*float(tcpu['cascadelake'])+1.15*float(tcpu['icelake'])
      tmemtres=tcputres
      tgputres=2.2*float(tgpu['titanx'])+75.0*float(tgpu['v100']+tgpu['rtx2080ti'])+10.0*float(tgpu['a40'])+209.1*float(tgpu['a100'])+29.9*float(tgpu['a100-mig'])
      ucputres=0.4*float(ucpu['haswell']+ucpu['broadwell'])+0.5*float(ucpu['skylake'])+0.5*float(ucpu['milan'])+0.8*float(ucpu['rome'])+1.0*float(ucpu['cascadelake'])+1.15*float(ucpu['icelake'])
      umemtres=0.4*float(umem['haswell']+umem['broadwell'])+0.5*float(umem['skylake'])+0.5*float(umem['milan'])+0.8*float(umem['rome'])+1.0*float(umem['cascadelake'])+1.15*float(umem['icelake'])
      ugputres=2.2*float(ugpu['titanx'])+75.0*float(ugpu['v100']+ugpu['rtx2080ti'])+10.0*float(ugpu['a40'])+209.1*float(ugpu['a100'])+29.9*float(ugpu['a100-mig'])

      ttres=tcputres+tmemtres+tgputres
      utres=ucputres+umemtres+ugputres
      #Current translation from TRES to Double Precision GFLOps
      t2g=93.25

      tcgflops=t2g*tcputres
      ucgflops=t2g*ucputres
      tggflops=t2g*tgputres
      uggflops=t2g*ugputres

      tgflops=tcgflops+tggflops
      ugflops=ucgflops+uggflops

      #Ship it.
      lsload = GaugeMetricFamily('lsload', 'Aggregate Cluster Node Stats', labels=['field'])
      lsload.add_metric(["nodetot"],NodeTot)
      lsload.add_metric(["cputot"],CPUTot)
      lsload.add_metric(["cpualloc"],CPUAlloc)
      lsload.add_metric(["cpuload"],CPULoad)
      lsload.add_metric(["realmem"],RealMem)
      lsload.add_metric(["memalloc"],MemAlloc)
      lsload.add_metric(["memload"],MemLoad)
      lsload.add_metric(["gputot"],GPUTot)
      lsload.add_metric(["gpualloc"],GPUAlloc)
      lsload.add_metric(["idletot"],IDLETot)
      lsload.add_metric(["downtot"],DOWNTot)
      lsload.add_metric(["draintot"],DRAINTot)
      lsload.add_metric(["mixedtot"],MIXEDTot)
      lsload.add_metric(["alloctot"],ALLOCTot)
      lsload.add_metric(["comptot"],COMPTot)
      lsload.add_metric(["restot"],RESTot)
      lsload.add_metric(["plannedtot"],PLANNEDTot)
      lsload.add_metric(["idlecpu"],IDLECPU)
      lsload.add_metric(["downcpu"],DOWNCPU)
      lsload.add_metric(["draincpu"],DRAINCPU)
      lsload.add_metric(["mixedcpu"],MIXEDCPU)
      lsload.add_metric(["alloccpu"],ALLOCCPU)
      lsload.add_metric(["compcpu"],COMPCPU)
      lsload.add_metric(["rescpu"],RESCPU)
      lsload.add_metric(["plannedcpu"],PLANNEDCPU)
      lsload.add_metric(["idlemem"],IDLEMem)
      lsload.add_metric(["downmem"],DOWNMem)
      lsload.add_metric(["drainmem"],DRAINMem)
      lsload.add_metric(["mixedmem"],MIXEDMem)
      lsload.add_metric(["allocmem"],ALLOCMem)
      lsload.add_metric(["compmem"],COMPMem)
      lsload.add_metric(["resmem"],RESMem)
      lsload.add_metric(["plannedmem"],PLANNEDMem)
      lsload.add_metric(["idlegpu"],IDLEGPU)
      lsload.add_metric(["downgpu"],DOWNGPU)
      lsload.add_metric(["draingpu"],DRAINGPU)
      lsload.add_metric(["mixedgpu"],MIXEDGPU)
      lsload.add_metric(["allocgpu"],ALLOCGPU)
      lsload.add_metric(["compgpu"],COMPGPU)
      lsload.add_metric(["resgpu"],RESGPU)
      lsload.add_metric(["plannedgpu"],PLANNEDGPU)
      lsload.add_metric(["peralloc"],PerAlloc)
      lsload.add_metric(["tcpuhaswell"],tcpu['haswell'])
      lsload.add_metric(["tcpubroadwell"],tcpu['broadwell'])
      lsload.add_metric(["tcpuskylake"],tcpu['skylake'])
      lsload.add_metric(["tcpumilan"],tcpu['milan'])
      lsload.add_metric(["tcpurome"],tcpu['rome'])
      lsload.add_metric(["tcpucascadelake"],tcpu['cascadelake'])
      lsload.add_metric(["tcpuicelake"],tcpu['icelake'])
      lsload.add_metric(["tgputitanx"],tgpu['titanx'])
      lsload.add_metric(["tgpuv100"],tgpu['v100'])
      lsload.add_metric(["tgpurtx2080ti"],tgpu['rtx2080ti'])
      lsload.add_metric(["tgpua40"],tgpu['a40'])
      lsload.add_metric(["tgpua100"],tgpu['a100'])
      lsload.add_metric(["tgpua100mig"],tgpu['a100-mig'])
      lsload.add_metric(["ucpuhaswell"],ucpu['haswell'])
      lsload.add_metric(["ucpubroadwell"],ucpu['broadwell'])
      lsload.add_metric(["ucpuskylake"],ucpu['skylake'])
      lsload.add_metric(["ucpumilan"],ucpu['milan'])
      lsload.add_metric(["ucpurome"],ucpu['rome'])
      lsload.add_metric(["ucpucascadelake"],ucpu['cascadelake'])
      lsload.add_metric(["ucpuicelake"],ucpu['icelake'])
      lsload.add_metric(["ugputitanx"],ugpu['titanx'])
      lsload.add_metric(["ugpuv100"],ugpu['v100'])
      lsload.add_metric(["ugpurtx2080ti"],ugpu['rtx2080ti'])
      lsload.add_metric(["ugpua40"],ugpu['a40'])
      lsload.add_metric(["ugpua100"],ugpu['a100'])
      lsload.add_metric(["ugpua100mig"],ugpu['a100-mig'])
      lsload.add_metric(["umemhaswell"],umem['haswell'])
      lsload.add_metric(["umembroadwell"],umem['broadwell'])
      lsload.add_metric(["umemskylake"],umem['skylake'])
      lsload.add_metric(["umemmilan"],umem['milan'])
      lsload.add_metric(["umemrome"],umem['rome'])
      lsload.add_metric(["umemcascadelake"],umem['cascadelake'])
      lsload.add_metric(["umemicelake"],umem['icelake'])
      lsload.add_metric(["tcputres"],tcputres)
      lsload.add_metric(["tgputres"],tgputres)
      lsload.add_metric(["tmemtres"],tmemtres)
      lsload.add_metric(["ucputres"],ucputres)
      lsload.add_metric(["ugputres"],ugputres)
      lsload.add_metric(["umemtres"],umemtres)
      lsload.add_metric(["ttres"],ttres)
      lsload.add_metric(["utres"],utres)
      lsload.add_metric(["tcgflops"],tcgflops)
      lsload.add_metric(["tggflops"],tggflops)
      lsload.add_metric(["ucgflops"],ucgflops)
      lsload.add_metric(["uggflops"],uggflops)
      lsload.add_metric(["tgflops"],tgflops)
      lsload.add_metric(["ugflops"],ugflops)
      yield lsload

if __name__ == "__main__":
  start_http_server(9002)
  REGISTRY.register(SlurmClusterStatusCollector())
  while True: 
    # period between collection
    time.sleep(30)
