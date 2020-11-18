#!/bin/bash
## creating a user and the folders that we require to run the vac jobs
useradd cc -s /bin/bash -p '*'

echo "copy service files to /etc/systemd/system"
cp /opt/nsq-latest/bin/*.service /etc/systemd/system

# reown to user cc
mkdir -p /home/cc/
chown -R cc:cc /home/cc
mkdir -p /opt/nsq_to_file_storage
mkdir -p /opt/tmp_nsq_to_file
chown -R cc:cc /opt/
systemctl daemon-reload
