# prometheus-slurm-exporter
[Prometheus](https://prometheus.io/) Exporter for Slurm. Uses the [prometheus python implementation](https://github.com/prometheus/client_python).

## Description

These collectors are intended to be used with prometheus to ship stats.  Each collector collects data on a different aspect of slurm.  Feel free to add or update these collectors to suit your needs.

### SlurmJobNodeCollector

This collector monitors individual SLURM job states and node status. It tracks job states (RUNNING, PENDING, etc.) and node availability (UP/DOWN), providing detailed visibility into job execution and cluster health at the individual job and node level.

### SlurmSchedStatsCollector

This collector is a prometheus version of this:

http://giovannitorres.me/graphing-sdiag-with-graphite.html

This collector will collect [sdiag](http://slurm.schedmd.com/sdiag.html "sdiag") stats allowing you to chart your scheduler performance over time.

### SlurmSshareCollector

This collector grabs the current [sshare](http://slurm.schedmd.com/sshare.html "sshare") data for users.  This assumes that you are using a two tier simple [fairshare](http://slurm.schedmd.com/priority_multifactor.html "Multifactor Priority") system of accounts and users of those accounts.

### SlurmClusterStatusCollector

This collector pulls the current [state](http://slurm.schedmd.com/scontrol.html "scontrol") of all the nodes in the cluster and then computes overall stats of the cluster such as number of nodes down, number of nodes in use, etc.

### SlurmPartitionStatusCollection

This collector reconstructs current partition state using several slurm commands to give a comprehensive summary of partition usage, allowing visibility into partition evolution over time.

### SlurmSeasStatsCollector

This collector pulls stats for Harvard SEAS.

### SlurmKempnerStatsCollector

This collector pulls stats for the Kempner Institute.

### SlurmKempnerSacctCollector

This collector pulls stats for the Kempner Institute

### SlurmKempnerNodeStatusCollector

This collector pulls stats for the Kempner Institute.

## Usage

Simply build the rpm via `rpmbuild -ba prometheus-slurm-exporter.spec` and install then use systemd to run the various unit files to get the exporters running.


## Dashboards

You can example dashboards for the various collectors in the `dashboards` directory.
