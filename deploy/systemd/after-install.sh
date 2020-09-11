#!/bin/bash
chown -R cc:cc /opt/nsq-latest
systemctl daemon-reloade
systemctl start nsqd
systemctl enable nsqd