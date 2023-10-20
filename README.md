# prometheus-slurm-exporter
[Prometheus](https://prometheus.io/) Exporter for Slurm. Uses the [prometheus python implementation](https://github.com/prometheus/client_python).

## Description

These collectors are intended to be used with prometheus to ship stats.  Each collector collects data on a different aspect of slurm.  Feel free to add or update these collectors to suit your needs.

### SlurmSchedStatsCollector

This collector is a prometheus version of this:

http://giovannitorres.me/graphing-sdiag-with-graphite.html

This collector will collect [sdiag](http://slurm.schedmd.com/sdiag.html "sdiag") stats allowing you to chart your scheduler performance over time.

### SlurmSshareCollector

This collector grabs the current [sshare](http://slurm.schedmd.com/sshare.html "sshare") data for users.  This assumes that you are using a two tier simple [fairshare](http://slurm.schedmd.com/priority_multifactor.html "Multifactor Priority") system of accounts and users of those accounts.

### SlurmClusterStatusCollector

This collector pulls the current [state](http://slurm.schedmd.com/scontrol.html "scontrol") of all the nodes in the cluster and then computes overall stats of the cluster such as number of nodes down, number of nodes in use, etc.

### SlurmSeasStats

This collector pulls stats for Harvard SEAS.