#!/usr/bin/env python
import os
import sys
from os.path import dirname, join

#progfile = "build/TestMF"
progfile = "build/TestLR"
hostfile = "scripts/5node"

script_path = os.path.realpath(__file__)
proj_dir = dirname(dirname(script_path))
print "proj_dir:", proj_dir
hostfile_path = join(proj_dir, hostfile)
prog_path = join(proj_dir, progfile)
print "hostfile_path:%s, prog_path:%s" % (hostfile_path, prog_path)


ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    )

params = {
    "config_file" : hostfile_path,
    "hdfs_namenode" : "proj10",
    "hdfs_namenode_port" : 9000,
    "num_workers_per_node" : 5,
    #"input" : "hdfs:///datasets/ml/netflix_small.txt",
    "input" : "hdfs:///datasets/classification/kdd12",
    "master_port" : 23743,
    "n_loaders_per_node" : 12,
}

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=1 "
  "GLOG_minloglevel=0 "
  )


clear_cmd = "ls " + hostfile_path + " > /dev/null; ls " + prog_path + " > /dev/null; "
# use the following to allow core dumping
# clear_cmd = "ls " + hostfile_path + " > /dev/null; ls " + prog_path + " > /dev/null; ulimit -c unlimited; "
with open(hostfile_path, "r") as f:
  hostlist = []  
  hostlines = f.read().splitlines()
  for line in hostlines:
    if not line.startswith("#"):
      hostlist.append(line.split(":"))
      
  for [node_id, host, port] in hostlist:
    print "node_id:%s, host:%s, port:%s" %(node_id, host, port)
    cmd = ssh_cmd + host + " "  # Start ssh command
    cmd += "\""  # Remote command starts
    cmd += clear_cmd
    # Command to run program
    cmd += env_params + " " + prog_path
    cmd += " --my_id=" + node_id
    cmd += "".join([" --%s=%s" % (k,v) for k,v in params.items()])
  
    cmd += "\""  # Remote Command ends
    cmd += " &"
    print cmd
    os.system(cmd)
        


