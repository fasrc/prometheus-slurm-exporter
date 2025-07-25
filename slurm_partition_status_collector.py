#!/usr/bin/python3.11

"""
slurm_partition_status_collector.py
A script that gets slurm partition statistics.
"""

import sys,os,subprocess,shlex
import time

prefix = os.path.normpath(
  os.path.join(os.path.abspath(os.path.dirname(__file__)))
)
external = os.path.join(prefix, 'external')
sys.path = [prefix, external] + sys.path

from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client.registry import Collector
from prometheus_client import start_http_server

class SlurmPartStatusCollector(Collector):
  def __init__(self):
    pass
  def collect(self):
    # Get partition information
    pprioritytier={}
    pcpu={}
    pmem={}
    pgpu={}
    pnode={}
    ptresweightcpu={}
    ptresweightmem={}
    ptresweightgpu={}
    presnode={}
    prescpu={}
    presmem={}
    presgpu={}
    pdownnode={}
    pdowncpu={}
    pdownmem={}
    pdowngpu={}
    ppwdnode={}
    ppwdcpu={}
    ppwdmem={}
    ppwdgpu={}
    pruncpu={}
    prunmem={}
    prungpu={}
    pruncnt={}
    prunusercnt={}
    prunacctcnt={}
    ppenduser={}
    ppendacct={}
    ppendcnt={}
    ppendusercnt={}
    ppendacctcnt={}
    prunuser={}
    prunacct={}
    pcpuuser={}
    pmemuser={}
    pgpuuser={}
    pcpuacct={}
    pmemacct={}
    pgpuacct={}
    prestarts={}
    pocc={}
    phcpu={}
    phmem={}
    phgpu={}
    plcpu={}
    plmem={}
    plgpu={}

    try:
      proc = subprocess.Popen([
      'scontrol',
      '-o', 'show', 'partition'
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      print("Exception")
    else:
      for line in proc.stdout:
        #Sanitize input
        line = line.replace("'", "\\'")

        #Turn partition information into a hash
        partition = dict(s.split("=", 1) for s in shlex.split(line) if '=' in s)

        #Get what Partition PriorityTier this partition is.
        pprioritytier[partition["PartitionName"]] = int(partition["PriorityTier"])

        #Get the total size of a partition
        tres = dict(s.split("=", 1) for s in shlex.split(partition['TRES'].replace(",", " ")) if '=' in s)
        pcpu[partition["PartitionName"]] = int(tres['cpu'])

        if 'G' in tres['mem']:
          pmem[partition["PartitionName"]] = float(tres['mem'].strip('G'))
        else:
          pmem[partition["PartitionName"]] = float(tres['mem'].strip('M'))/1024

        pnode[partition["PartitionName"]] = int(tres['node'])
        if 'gres/gpu' in tres:
          pgpu[partition["PartitionName"]] = int(tres['gres/gpu'])
        else:
          pgpu[partition["PartitionName"]] = 0

        #Get the TRESBillingWeights for a partition
        tresweight = dict(s.split("=", 1) for s in shlex.split(partition['TRESBillingWeights'].replace(",", " ")) if '=' in s)
        ptresweightcpu[partition["PartitionName"]] = float(tresweight['CPU'])
        ptresweightmem[partition["PartitionName"]] = float(tresweight['Mem'].strip('G'))
        if 'Gres/gpu' in tresweight:
          ptresweightgpu[partition["PartitionName"]] = float(tresweight['Gres/gpu'])
        else:
          ptresweightgpu[partition["PartitionName"]] = 0.0

        #Counters
        presnode[partition["PartitionName"]] = 0
        prescpu[partition["PartitionName"]] = 0
        presmem[partition["PartitionName"]] = 0.0
        presgpu[partition["PartitionName"]] = 0
        pdownnode[partition["PartitionName"]] = 0
        pdowncpu[partition["PartitionName"]] = 0
        pdownmem[partition["PartitionName"]] = 0.0
        pdowngpu[partition["PartitionName"]] = 0
        ppwdnode[partition["PartitionName"]] = 0
        ppwdcpu[partition["PartitionName"]] = 0
        ppwdmem[partition["PartitionName"]] = 0.0
        ppwdgpu[partition["PartitionName"]] = 0
        ppendcnt[partition["PartitionName"]] = 0
        ppendusercnt[partition["PartitionName"]] = 0
        ppendacctcnt[partition["PartitionName"]] = 0
        pruncpu[partition["PartitionName"]] = 0
        prunmem[partition["PartitionName"]] = 0.0
        prungpu[partition["PartitionName"]] = 0
        pruncnt[partition["PartitionName"]] = 0
        prunusercnt[partition["PartitionName"]] = 0
        prunacctcnt[partition["PartitionName"]] = 0
        prestarts[partition["PartitionName"]] = 0
        pocc[partition["PartitionName"]] = 0.0
        phcpu[partition["PartitionName"]] = 0
        phmem[partition["PartitionName"]] = 0.0
        phgpu[partition["PartitionName"]] = 0
        plcpu[partition["PartitionName"]] = 0
        plmem[partition["PartitionName"]] = 0.0
        plgpu[partition["PartitionName"]] = 0

    #Get node information
    ncpu={}
    nmem={}
    ngpu={}
    npartition={}
    npcpu={}
    npmem={}
    npgpu={}

    try:
      proc = subprocess.Popen([
      'scontrol',
      '-o', 'show', 'node'
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      print("Exception")
    else:
      for line in proc.stdout:
        #Sanitize input
        line = line.replace("'", "\\'")

        #Turn partition information into a hash
        node = dict(s.split("=", 1) for s in shlex.split(line) if '=' in s)

        #Get the configured TRES for a node
        cfgtres = dict(s.split("=", 1) for s in shlex.split(node['CfgTRES'].replace(",", " ")) if '=' in s)
        ncpu[node["NodeName"]] = int(cfgtres["cpu"])
        nmem[node["NodeName"]] = float(cfgtres["mem"].strip("M"))/1024
        if 'gres/gpu' in cfgtres:
          ngpu[node["NodeName"]] = int(cfgtres['gres/gpu'])
        else:
          ngpu[node["NodeName"]] = 0

        #Get the partitions that hit a node
        npartition[node["NodeName"]] = node["Partitions"].split(',')

        #Flag nodes by state
        state = node["State"].split('+')

        stateres = 0
        statedown = 0
        statepwd = 0

        for s in state:
          if s == "RESERVED":
            stateres = 1
          if s == "DOWN" or s == "DRAIN":
            statedown = 1
          if s == "POWERED_DOWN":
            statepwd = 1

        if stateres == 1:
          for part in npartition[node["NodeName"]]:
            presnode[part] = presnode[part]+1
            prescpu[part] = prescpu[part]+ncpu[node["NodeName"]]
            presmem[part] = presmem[part]+nmem[node["NodeName"]]
            presgpu[part] = presgpu[part]+ngpu[node["NodeName"]]
        if statedown == 1:
          for part in npartition[node["NodeName"]]:
            pdownnode[part] = pdownnode[part]+1
            pdowncpu[part] = pdowncpu[part]+ncpu[node["NodeName"]]
            pdownmem[part] = pdownmem[part]+nmem[node["NodeName"]]
            pdowngpu[part] = pdowngpu[part]+ngpu[node["NodeName"]]
        if statepwd == 1:
          for part in npartition[node["NodeName"]]:
            ppwdnode[part] = ppwdnode[part]+1
            ppwdcpu[part] = ppwdcpu[part]+ncpu[node["NodeName"]]
            ppwdmem[part] = ppwdmem[part]+nmem[node["NodeName"]]
            ppwdgpu[part] = ppwdgpu[part]+ngpu[node["NodeName"]]


        #Initializing Counters
        for part in npartition[node["NodeName"]]:
          try:
            npcpu[node["NodeName"]][part] = 0
            npmem[node["NodeName"]][part] = 0
            npgpu[node["NodeName"]][part] = 0
          except:
            npcpu[node["NodeName"]]={part: 0}
            npmem[node["NodeName"]]={part: 0.0}
            npgpu[node["NodeName"]]={part: 0}

    #Get job information
    try:
      proc = subprocess.Popen([
      'scontrol',
      '-od', 'show', 'job'
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      print("Exception")
    else:
      for line in proc.stdout:
        #Sanitize input
        line = line.replace("'", "\\'")

        #Turn partition information into a hash
        job = dict(s.split("=", 1) for s in shlex.split(line) if '=' in s)

        #Get user and account info
        #If the data is empty then there are no jobs to count.
        try:
          user = job["UserId"]
        except:
          break

        acct = job["Account"]

        #Get the partitions for a job
        jobpart = job["Partition"].split(',')

        #Count how many job restarts per partition
        if "CronJob" not in job:
          for part in jobpart:
            prestarts[part] = prestarts[part] + int(job["Restarts"])

        #Count how many pending jobs per partition, user, and account
        if "PENDING" in job["JobState"]:

          #Count array elements as jobs
          if "ArrayTaskId" in job:
            taskid = job["ArrayTaskId"].strip(".")
            taskid = taskid.split(',')

            jobcnt = 0

            for t in taskid:
              t = t.split('%',1)[0]
              t = t.split(':',1)[0]
              t = t.strip(".")
              if "-" in t:
                ts = t.split('-')

                if not ts[1]:
                  ts[1] = 0

                jobcnt = max(int(ts[1])-int(ts[0]),1) + 1 + jobcnt
              else:
                jobcnt = jobcnt + 1
          else:
            jobcnt = 1

          for part in jobpart:
            ppendcnt[part] = ppendcnt[part]+jobcnt
            try:
              try:
                ppenduser[part][user] = ppenduser[part][user]+jobcnt
              except:
                ppenduser[part][user] = jobcnt
                ppendusercnt[part]=ppendusercnt[part]+1
            except:
              ppenduser[part]={user: jobcnt}
              ppendusercnt[part]=ppendusercnt[part]+1

            try:
              try:
                ppendacct[part][acct] = ppendacct[part][acct]+jobcnt
              except:
                ppendacct[part][acct] = jobcnt
                ppendacctcnt[part]=ppendacctcnt[part]+1
            except:
              ppendacct[part]={acct: jobcnt}
              ppendacctcnt[part]=ppendacctcnt[part]+1

        #Grab stats on Running jobs
        if "RUNNING" in job["JobState"]:
          #Strictly speaking there should only be one partition per job but to make this easy we will just have a meaningless for loop
          for part in jobpart:
            #Get overall stats for job
            alloctres = dict(s.split("=", 1) for s in shlex.split(job['AllocTRES'].replace(",", " ")) if '=' in s)
            cpu = int(alloctres["cpu"])
            if "G" in alloctres["mem"]:
              mem = float(alloctres["mem"].strip("G"))
            elif "M" in alloctres["mem"]:
              mem = float(alloctres["mem"].strip("M"))/1024

            if 'gres/gpu' in alloctres:
              gpu = int(alloctres['gres/gpu'])
            else:
              gpu = 0

            #logging cpu, memory, and gpu per partition
            pruncpu[part] = pruncpu[part] + cpu
            prunmem[part] = prunmem[part] + mem
            prungpu[part] = prungpu[part] + gpu
            pruncnt[part] = pruncnt[part] + 1

            #Counting running jobs per user and account as well as logging cpu, memory, and gpu usage.
            try:
              try:
                prunuser[part][user] = prunuser[part][user]+1
                pcpuuser[part][user] = pcpuuser[part][user]+cpu
                pmemuser[part][user] = pmemuser[part][user]+mem
                pgpuuser[part][user] = pgpuuser[part][user]+gpu
              except:
                prunuser[part][user] = 1
                pcpuuser[part][user] = cpu
                pmemuser[part][user] = mem
                pgpuuser[part][user] = gpu
                prunusercnt[part]=prunusercnt[part]+1
            except:
              prunuser[part]={user: 1}
              pcpuuser[part]={user: cpu}
              pmemuser[part]={user: mem}
              pgpuuser[part]={user: gpu}
              prunusercnt[part]=prunusercnt[part]+1

            try:
              try:
                prunacct[part][acct] = prunacct[part][acct]+1
                pcpuacct[part][acct] = pcpuacct[part][acct]+cpu
                pmemacct[part][acct] = pmemacct[part][acct]+mem
                pgpuacct[part][acct] = pgpuacct[part][acct]+gpu
              except:
                prunacct[part][acct] = 1
                pcpuacct[part][acct] = cpu
                pmemacct[part][acct] = mem
                pgpuacct[part][acct] = gpu
                prunacctcnt[part]=prunacctcnt[part]+1
            except:
              prunacct[part]={acct: 1}
              pcpuacct[part]={acct: cpu}
              pmemacct[part]={acct: mem}
              pgpuacct[part]={acct: gpu}
              prunacctcnt[part]=prunacctcnt[part]+1

            #Grabbing node specific information
            #First we need to reparse the scontrol data
            jobsplit = line.split('  ')
            for i in jobsplit:
              if "NumNodes=" not in i:
                if "Nodes=" in i:
                  nodestat = dict(s.split("=", 1) for s in shlex.split(i) if '=' in s)

                  #Getting CPU count
                  cpuid = nodestat["CPU_IDs"].split(',')

                  cpucnt = 0
                  for c in cpuid:
                    if "-" in c:
                      cs = c.split('-')
                      cpucnt = int(cs[1]) - int(cs[0]) + 1 + cpucnt
                    else:
                      cpucnt = cpucnt + 1

                  #Getting GPU count
                  if "gpu" in nodestat["GRES"]:
                    ggres = nodestat["GRES"].split(':')
                    gpucnt = int(ggres[2].strip('(IDX'))
                  else:
                    gpucnt = 0

                  #Recording data
                  #Testing if single nodename or nodelist
                  try:
                    npcpu[nodestat["Nodes"]][part] = npcpu[nodestat["Nodes"]][part] + cpucnt
                    npmem[nodestat["Nodes"]][part] = npmem[nodestat["Nodes"]][part] + float(nodestat["Mem"])/1024
                    npgpu[nodestat["Nodes"]][part] = npgpu[nodestat["Nodes"]][part] + gpucnt
                  except:
                    #Splitting nodelist into node names
                    proc2 = subprocess.Popen([
                    'scontrol',
                    'show', 'hostnames', nodestat["Nodes"]
                    ], stdout=subprocess.PIPE,
                    universal_newlines=True)

                    for n in proc2.stdout:
                      n=n.strip("\n")
                      #This is to cover cases where nodes were moved to a different partition but the jobs from the old partition still exist
                      #In this case we will just drop the data and move on as this is a temporary state.
                      try:
                        npcpu[n][part] = npcpu[n][part] + cpucnt
                        npmem[n][part] = npmem[n][part] + float(nodestat["Mem"])/1024
                        npgpu[n][part] = npgpu[n][part] + gpucnt
                      except:
                        #Do not do anything
                        continue

    #Doing node based sums
    for n in ncpu:
      #Calculating inverses (1/value)
      incpu = 1/float(ncpu[n])
      inmem = 1/nmem[n]
      ingpu = 1/max(float(ngpu[n]),1)

      for p in npartition[n]:
        #Calculation occupation
        pocc[p] = pocc[p] + max(float(npcpu[n][p])*incpu,npmem[n][p]*inmem,float(npgpu[n][p])*ingpu)

        #Calculating usage in partitions that are higher and lower priority than the current partition
        #Grabbing current priority
        cprio = pprioritytier[p]

        #Look at the other partitions and sum based on relative priority
        for sp in npartition[n]:
          if sp != p:
            if pprioritytier[sp] < cprio:
              plcpu[p] = plcpu[p] + npcpu[n][sp]
              plmem[p] = plmem[p] + npmem[n][sp]
              plgpu[p] = plgpu[p] + npgpu[n][sp]
            else:
              phcpu[p] = phcpu[p] + npcpu[n][sp]
              phmem[p] = phmem[p] + npmem[n][sp]
              phgpu[p] = phgpu[p] + npgpu[n][sp]

    #Export data
    spart = GaugeMetricFamily('spart', 'Partition stats', labels=['partition','user','account','field'])

    #Current translation from TRES to Double Precision GFLOps
    t2g=93.25
   
    for p in pcpu:
      #General partition stats
      spart.add_metric([p,'','','cpu'],pcpu[p])
      spart.add_metric([p,'','','mem'],pmem[p])
      spart.add_metric([p,'','','gpu'],pgpu[p])
      spart.add_metric([p,'','','node'],pnode[p])
      spart.add_metric([p,'','','rescpu'],prescpu[p])
      spart.add_metric([p,'','','resmem'],presmem[p])
      spart.add_metric([p,'','','resgpu'],presgpu[p])
      spart.add_metric([p,'','','resnode'],presnode[p])
      spart.add_metric([p,'','','downcpu'],pdowncpu[p])
      spart.add_metric([p,'','','downmem'],pdownmem[p])
      spart.add_metric([p,'','','downgpu'],pdowngpu[p])
      spart.add_metric([p,'','','downnode'],pdownnode[p])
      spart.add_metric([p,'','','pwdcpu'],ppwdcpu[p])
      spart.add_metric([p,'','','pwdmem'],ppwdmem[p])
      spart.add_metric([p,'','','pwdgpu'],ppwdgpu[p])
      spart.add_metric([p,'','','pwdnode'],ppwdnode[p])
      spart.add_metric([p,'','','perdown'],float(pdownnode[p])/max(float(pnode[p]),1.0))
      spart.add_metric([p,'','','perres'],float(presnode[p])/max(float(pnode[p]),1.0))
      spart.add_metric([p,'','','runcpu'],pruncpu[p])
      spart.add_metric([p,'','','runmem'],prunmem[p])
      spart.add_metric([p,'','','rungpu'],prungpu[p])
      spart.add_metric([p,'','','occ'],pocc[p])
      spart.add_metric([p,'','','perocc'],pocc[p]/max(float(pnode[p]),1.0))
      spart.add_metric([p,'','','hcpu'],phcpu[p])
      spart.add_metric([p,'','','hmem'],phmem[p])
      spart.add_metric([p,'','','hgpu'],phgpu[p])
      spart.add_metric([p,'','','lcpu'],plcpu[p])
      spart.add_metric([p,'','','lmem'],plmem[p])
      spart.add_metric([p,'','','lgpu'],plgpu[p])
      spart.add_metric([p,'','','pendcnt'],ppendcnt[p])
      spart.add_metric([p,'','','pendusercnt'],ppendusercnt[p])
      spart.add_metric([p,'','','pendacctcnt'],ppendacctcnt[p])
      spart.add_metric([p,'','','runcnt'],pruncnt[p])
      spart.add_metric([p,'','','runusercnt'],prunusercnt[p])
      spart.add_metric([p,'','','runacctcnt'],prunacctcnt[p])
      spart.add_metric([p,'','','restarts'],prestarts[p])

      #TRES and FLOPS calculation
      trescpu = ptresweightcpu[p]*float(pcpu[p])
      tresmem = ptresweightmem[p]*pmem[p]
      tresgpu = ptresweightgpu[p]*float(pgpu[p])
      trestot = trescpu+tresmem+tresgpu
      tresruncpu = ptresweightcpu[p]*float(pruncpu[p])
      tresrunmem = ptresweightmem[p]*prunmem[p]
      tresrungpu = ptresweightgpu[p]*float(prungpu[p])
      tresruntot = tresruncpu+tresrunmem+tresrungpu

      flopscpu = t2g*trescpu
      flopsgpu = t2g*tresgpu
      flopstot = flopscpu+flopsgpu
      flopsruncpu = t2g*tresruncpu
      flopsrungpu = t2g*tresrungpu
      flopsruntot = flopsruncpu+flopsrungpu

      spart.add_metric([p,'','','trescpu'],trescpu)
      spart.add_metric([p,'','','tresmem'],tresmem)
      spart.add_metric([p,'','','tresgpu'],tresgpu)
      spart.add_metric([p,'','','trestot'],trestot)
      spart.add_metric([p,'','','tresruncpu'],tresruncpu)
      spart.add_metric([p,'','','tresrunmem'],tresrunmem)
      spart.add_metric([p,'','','tresrungpu'],tresrungpu)
      spart.add_metric([p,'','','tresruntot'],tresruntot)
      spart.add_metric([p,'','','flopscpu'],flopscpu)
      spart.add_metric([p,'','','flopsgpu'],flopsgpu)
      spart.add_metric([p,'','','flopstot'],flopstot)
      spart.add_metric([p,'','','flopsruncpu'],flopsruncpu)
      spart.add_metric([p,'','','flopsrungpu'],flopsrungpu)
      spart.add_metric([p,'','','flopsruntot'],flopsruntot)

      #Per User Data
      try:
        for u in ppenduser[p]:
          spart.add_metric([p,u,'','penduser'],ppenduser[p][u])
      except:
        spart.add_metric([p,'root(0)','','penduser'],0)

      try:
        for u in prunuser[p]:
          spart.add_metric([p,u,'','runuser'],prunuser[p][u])
          spart.add_metric([p,u,'','cpuuser'],pcpuuser[p][u])
          spart.add_metric([p,u,'','memuser'],pmemuser[p][u])
          spart.add_metric([p,u,'','gpuuser'],pgpuuser[p][u])

          tresruncpu = ptresweightcpu[p]*float(pcpuuser[p][u])
          tresrunmem = ptresweightmem[p]*pmemuser[p][u]
          tresrungpu = ptresweightgpu[p]*float(pgpuuser[p][u])
          tresruntot = tresruncpu+tresrunmem+tresrungpu

          spart.add_metric([p,u,'','tresuser'],tresruntot)
      except:
        spart.add_metric([p,'root(0)','','runuser'],0)
        spart.add_metric([p,'root(0)','','cpuuser'],0)
        spart.add_metric([p,'root(0)','','memuser'],0)
        spart.add_metric([p,'root(0)','','gpuuser'],0)
        spart.add_metric([p,'root(0)','','tresuser'],0)

      #Per Account Data
      try:
        for a in ppendacct[p]:
          spart.add_metric([p,'',a,'pendacct'],ppendacct[p][a])
      except:
        spart.add_metric([p,'','root','pendacct'],0)

      try:
        for a in prunacct[p]:
          spart.add_metric([p,'',a,'runacct'],prunacct[p][a])
          spart.add_metric([p,'',a,'cpuacct'],pcpuacct[p][a])
          spart.add_metric([p,'',a,'memacct'],pmemacct[p][a])
          spart.add_metric([p,'',a,'gpuacct'],pgpuacct[p][a])

          tresruncpu = ptresweightcpu[p]*float(pcpuacct[p][a])
          tresrunmem = ptresweightmem[p]*pmemacct[p][a]
          tresrungpu = ptresweightgpu[p]*float(pgpuacct[p][a])
          tresruntot = tresruncpu+tresrunmem+tresrungpu

          spart.add_metric([p,'',a,'tresacct'],tresruntot)
      except:
        spart.add_metric([p,'','root','runacct'],0)
        spart.add_metric([p,'','root','cpuacct'],0)
        spart.add_metric([p,'','root','memacct'],0)
        spart.add_metric([p,'','root','gpuacct'],0)
        spart.add_metric([p,'','root','tresacct'],0)

    yield spart

if __name__ == "__main__":
  start_http_server(9008)
  REGISTRY.register(SlurmPartStatusCollector())
  while True: 
    # period between collection
    time.sleep(55)
