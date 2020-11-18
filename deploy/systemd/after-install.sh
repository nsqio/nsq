#!/bin/bash
## creating a user and the folders that we require to run the vac jobs
useradd cc -s /bin/bash -p '*'
# reown to user cc
mkdir -p /home/cc/
chown -R cc:cc /home/cc
mkdir -p /opt/nsq_to_file_storage
mkdir -p /opt/tmp_nsq_to_file
chown -R cc:cc /opt/
systemctl daemon-reload
