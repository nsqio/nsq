#!/bin/bash
id -u cc &>/dev/null || useradd cc -s /bin/bash -p '*'
mkdir -p /opt/nsq_to_file_storage
mkdir -p /opt/tmp_nsq_to_file
chown -R cc:cc /opt/nsq_to_file_storage
chown -R cc:cc /opt/tmp_nsq_to_file
