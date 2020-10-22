#!/bin/bash
id -u cc &>/dev/null || useradd cc -s /bin/bash -p '*'
if [ ! -d "/opt/nsq_to_file_storage" ]
then
  mkdir -p /opt/nsq_to_file_storage
  chown -R cc:cc /opt/nsq_to_file_storage
fi
if [ ! -d "/opt/tmp_nsq_to_file" ]
then
  mkdir -p /opt/tmp_nsq_to_file
  chown -R cc:cc /opt/tmp_nsq_to_file
fi
