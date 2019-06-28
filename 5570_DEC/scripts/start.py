#!/usr/bin/env python

import os
num_of_nodes = 5
cmd = ""
port = 13900
master_port = 49000
for i in range(0, num_of_nodes):
    cmd = cmd + "ssh 1155077005@proj" + str(10 - i) + " "
    cmd = cmd + "/data/opt/tmp/1155077005/CSCI5570/build/./TestSimple "
    #+ \
    #    str(10 - i) + " " + str(num_of_nodes) + " " + \
    #    str(port) + " " + str(master_port + i) + "&"
    print cmd
    os.system(cmd)
    cmd = ""
while (True):
    pass
# print cmd
