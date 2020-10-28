#!/bin/bash
## creating a user and the folders that we require to run the vac jobs
useradd cc -s /bin/bash -p '*'
# reown to user cc
mkdir -p /home/cc/
chown -R cc:cc /home/cc
mkdir -p /opt/nsq_to_file_storage
mkdir -p /opt/tmp_nsq_to_file
chown -R cc:cc /opt/nsq-1.2.0.linux-amd64.go1.12.9
systemctl daemon-reload
