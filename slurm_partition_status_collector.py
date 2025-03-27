#!/usr/bin/python3.11

import sys,os,subprocess,shlex

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
    pruncpu={}
    prunmem={}
    prungpu={}
    pruncnt={}
    ppenduser={}
    ppendacct={}
    ppendcnt={}
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
        ppendcnt[partition["PartitionName"]] = 0
        pruncpu[partition["PartitionName"]] = 0
        prunmem[partition["PartitionName"]] = 0.0
        prungpu[partition["PartitionName"]] = 0
        pruncnt[partition["PartitionName"]] = 0
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
        if "RESERVED" in node["State"]:
          for part in npartition[node["NodeName"]]:
            presnode[part] = presnode[part]+1
            prescpu[part] = prescpu[part]+ncpu[node["NodeName"]]
            presmem[part] = presmem[part]+nmem[node["NodeName"]]
            presgpu[part] = presgpu[part]+ngpu[node["NodeName"]]
        if "DRAIN" in node["State"] or "DOWN" in node["State"]:
          for part in npartition[node["NodeName"]]:
            pdownnode[part] = pdownnode[part]+1
            pdowncpu[part] = pdowncpu[part]+ncpu[node["NodeName"]]
            pdownmem[part] = pdownmem[part]+nmem[node["NodeName"]]
            pdowngpu[part] = pdowngpu[part]+ngpu[node["NodeName"]]

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
        user = job["UserId"]
        acct = job["Account"]

        #Get the partitions for a job
        jobpart = job["Partition"].split(',')

        #Get overall stats for job
        reqtres = dict(s.split("=", 1) for s in shlex.split(job['ReqTRES'].replace(",", " ")) if '=' in s)
        cpu = int(reqtres["cpu"])
        if "G" in reqtres["mem"]:
          mem = float(reqtres["mem"].strip("G"))
        elif "M" in reqtres["mem"]:
          mem = float(reqtres["mem"].strip("M"))/1024

        if 'gres/gpu' in reqtres:
          gpu = int(reqtres['gres/gpu'])
        else:
          gpu = 0

        #Count how many job restarts per partition
        if "CronJob" not in job:
          for part in jobpart:
            prestarts[part] = prestarts[part] + int(job["Restarts"])

        #Count how many pending jobs per partition, user, and account
        if "PENDING" in job["JobState"]:
          for part in jobpart:
            ppendcnt[part] = ppendcnt[part]+1
            try:
              try:
                ppenduser[part][user] = ppenduser[part][user]+1
              except:
                ppenduser[part][user] = 1
            except:
              ppenduser[part]={user: 1}

            try:
              try:
                ppendacct[part][acct] = ppendacct[part][acct]+1
              except:
                ppendacct[part][acct] = 1
            except:
              ppendacct[part]={acct: 1}

        #Grab stats on Running jobs
        if "RUNNING" in job["JobState"]:
          #Strictly speaking there should only be one partition per job but to make this easy we will just have a meaningless for loop
          for part in jobpart:
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
            except:
              prunuser[part]={user: 1}
              pcpuuser[part]={user: cpu}
              pmemuser[part]={user: mem}
              pgpuuser[part]={user: gpu}

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
            except:
              prunacct[part]={acct: 1}
              pcpuacct[part]={acct: cpu}
              pmemacct[part]={acct: mem}
              pgpuacct[part]={acct: gpu}

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
                      npcpu[n][part] = npcpu[n][part] + cpucnt
                      npmem[n][part] = npmem[n][part] + float(nodestat["Mem"])/1024
                      npgpu[n][part] = npgpu[n][part] + gpucnt

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
