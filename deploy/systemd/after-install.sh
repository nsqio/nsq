#!/bin/bash
chown -R cc:cc /opt/nsq-latest
systemctl daemon-reload
systemctl start nsqd
systemctl enable nsqd