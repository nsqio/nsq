#!/usr/bin/env python3

#
# This script bootstraps an NSQ cluster in EC2 and runs benchmarks.
#
# Requires python3 and the following packages:
#   - boto3
#   - paramiko
#   - tornado
#
# AWS authentication is delegated entirely to the boto3 environment, see:
#
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
#
# EC2 instances are launched into EC2 Classic, expecting a 'default' security group
# that allows allows SSH (port 22) from 0.0.0.0/0 and an EC2 key pair created
# (named 'default', but configurable via --ssh-key-name).
#

import sys
import logging
import time
import datetime
import socket
import warnings
import hashlib

import boto3
import paramiko.client
import paramiko.ssh_exception
import tornado.options


def ssh_connect_with_retries(host, retries=3, timeout=30):
    for i in range(retries):
        try:
            ssh_client = paramiko.client.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.client.WarningPolicy())
            ssh_client.connect(host, username='ubuntu', timeout=timeout)
            return ssh_client
        except (socket.error, paramiko.ssh_exception.SSHException):
            if i == retries - 1:
                raise
        logging.warning('... re-trying to connect to %s:%d in 15s', host, 22)
        time.sleep(15)


def ssh_cmd_async(ssh_client, cmd):
    transport = ssh_client.get_transport()
    chan = transport.open_session()
    chan.exec_command(cmd)
    return chan


def ssh_cmd(ssh_client, cmd, timeout=2):
    transport = ssh_client.get_transport()
    chan = transport.open_session()
    chan.settimeout(timeout)
    chan.exec_command(cmd)

    stdout = b''
    stderr = b''
    while True:
        if chan.recv_ready():
            stdout += chan.recv(4096)
            continue
        if chan.recv_stderr_ready():
            stderr += chan.recv_stderr(4096)
            continue
        if chan.exit_status_ready():
            exit_status = chan.recv_exit_status()
            break
        time.sleep(0.1)

    if exit_status != 0:
        raise Exception('%r' % stderr)

    return stdout, stderr


def get_session():
    return boto3.session.Session(region_name=tornado.options.options.region)


def _bootstrap(addr):
    commit = tornado.options.options.commit
    golang_version = tornado.options.options.golang_version
    ssh_client = ssh_connect_with_retries(addr)
    for cmd in [
            'wget https://storage.googleapis.com/golang/go%s.linux-amd64.tar.gz' % golang_version,
            'sudo -S tar -C /usr/local -xzf go%s.linux-amd64.tar.gz' % golang_version,
            'sudo -S apt-get update',
            'sudo -S apt-get -y install git mercurial',
            'mkdir -p go/src/github.com/nsqio',
            'cd go/src/github.com/nsqio && git clone https://github.com/nsqio/nsq',
            'cd go/src/github.com/nsqio/nsq && git checkout %s' % commit,
            'cd go/src/github.com/nsqio/nsq/apps/nsqd && GO111MODULE=on /usr/local/go/bin/go build',
            'cd go/src/github.com/nsqio/nsq/bench/bench_writer && GO111MODULE=on /usr/local/go/bin/go build',
            'cd go/src/github.com/nsqio/nsq/bench/bench_reader && GO111MODULE=on /usr/local/go/bin/go build',
            'sudo -S mkdir -p /mnt/nsq',
            'sudo -S chmod 777 /mnt/nsq']:
        ssh_cmd(ssh_client, cmd, timeout=10)


def bootstrap():
    session = get_session()

    ec2 = session.resource('ec2')

    total_count = tornado.options.options.nsqd_count + tornado.options.options.worker_count
    logging.info('launching %d instances', total_count)
    instances = ec2.create_instances(
        ImageId=tornado.options.options.ami,
        MinCount=total_count,
        MaxCount=total_count,
        KeyName=tornado.options.options.ssh_key_name,
        InstanceType=tornado.options.options.instance_type,
        SecurityGroups=['default'])

    logging.info('waiting for instances to launch...')

    while any(i.state['Name'] != 'running' for i in instances):
        waiting_for = [i.id for i in instances if i.state['Name'] != 'running']
        logging.info('... sleeping for 5s (waiting for %s)', ', '.join(waiting_for))
        time.sleep(5)
        for instance in instances:
            instance.load()

    for instance in instances:
        if not instance.tags:
            instance.create_tags(Tags=[{'Key': 'nsq_bench', 'Value': '1'}])

    try:
        c = 0
        for i in instances:
            c += 1
            logging.info('(%d) bootstrapping %s (%s)', c, i.public_dns_name, i.id)
            _bootstrap(i.public_dns_name)
    except Exception:
        logging.exception('bootstrap failed')
        decomm()


def run():
    instances = _find_instances()

    logging.info('launching nsqd on %d host(s)', tornado.options.options.nsqd_count)

    nsqd_chans = []
    nsqd_hosts = instances[:tornado.options.options.nsqd_count]
    for instance in nsqd_hosts:
        try:
            ssh_client = ssh_connect_with_retries(instance.public_dns_name)
            for cmd in [
                    'sudo -S pkill -f nsqd',
                    'sudo -S rm -f /mnt/nsq/*.dat',
                    'GOMAXPROCS=32 ./go/src/github.com/nsqio/nsq/apps/nsqd/nsqd \
                        --data-path=/mnt/nsq \
                        --mem-queue-size=10000000 \
                        --max-rdy-count=%s' % (tornado.options.options.rdy)]:
                nsqd_chans.append((ssh_client, ssh_cmd_async(ssh_client, cmd)))
        except Exception:
            logging.exception('failed')

    nsqd_tcp_addrs = [i.public_dns_name for i in nsqd_hosts]

    dt = datetime.datetime.utcnow()
    deadline = dt + datetime.timedelta(seconds=30)

    logging.info('launching %d producer(s) on %d host(s)',
                 tornado.options.options.nsqd_count * tornado.options.options.worker_count,
                 tornado.options.options.worker_count)

    worker_chans = []

    producer_hosts = instances[tornado.options.options.nsqd_count:]
    for instance in producer_hosts:
        for nsqd_tcp_addr in nsqd_tcp_addrs:
            topic = hashlib.md5(instance.public_dns_name.encode('utf-8')).hexdigest()
            try:
                ssh_client = ssh_connect_with_retries(instance.public_dns_name)
                for cmd in [
                        'GOMAXPROCS=2 \
                            ./go/src/github.com/nsqio/nsq/bench/bench_writer/bench_writer \
                            --topic=%s --nsqd-tcp-address=%s:4150 --deadline=\'%s\' --size=%d' % (
                            topic, nsqd_tcp_addr, deadline.strftime('%Y-%m-%d %H:%M:%S'),
                            tornado.options.options.msg_size)]:
                    worker_chans.append((ssh_client, ssh_cmd_async(ssh_client, cmd)))
            except Exception:
                logging.exception('failed')

    if tornado.options.options.mode == 'pubsub':
        logging.info('launching %d consumer(s) on %d host(s)',
                     tornado.options.options.nsqd_count * tornado.options.options.worker_count,
                     tornado.options.options.worker_count)

        consumer_hosts = instances[tornado.options.options.nsqd_count:]
        for instance in consumer_hosts:
            for nsqd_tcp_addr in nsqd_tcp_addrs:
                topic = hashlib.md5(instance.public_dns_name.encode('utf-8')).hexdigest()
                try:
                    ssh_client = ssh_connect_with_retries(instance.public_dns_name)
                    for cmd in [
                            'GOMAXPROCS=8 \
                                ./go/src/github.com/nsqio/nsq/bench/bench_reader/bench_reader \
                                --topic=%s --nsqd-tcp-address=%s:4150 --deadline=\'%s\' --size=%d \
                                --rdy=%d' % (
                                topic, nsqd_tcp_addr, deadline.strftime('%Y-%m-%d %H:%M:%S'),
                                tornado.options.options.msg_size, tornado.options.options.rdy)]:
                        worker_chans.append((ssh_client, ssh_cmd_async(ssh_client, cmd)))
                except Exception:
                    logging.exception('failed')

    stats = {
        'bench_reader': {
            'durations': [],
            'mbytes': [],
            'ops': []
        },
        'bench_writer': {
            'durations': [],
            'mbytes': [],
            'ops': []
        }
    }
    while worker_chans:
        for ssh_client, chan in worker_chans[:]:
            if chan.recv_ready():
                sys.stdout.write(chan.recv(4096))
                sys.stdout.flush()
                continue
            if chan.recv_stderr_ready():
                line = chan.recv_stderr(4096).decode('utf-8')
                if 'duration:' in line:
                    kind = line.split(' ')[0][1:-1]
                    parts = line.rsplit('duration:')[1].split('-')
                    stats[kind]['durations'].append(float(parts[0].strip()[:-1]))
                    stats[kind]['mbytes'].append(float(parts[1].strip()[:-4]))
                    stats[kind]['ops'].append(float(parts[2].strip()[:-5]))
                sys.stdout.write(line)
                sys.stdout.flush()
                continue
            if chan.exit_status_ready():
                worker_chans.remove((ssh_client, chan))
        time.sleep(0.1)

    for kind, data in stats.items():
        if not data['durations']:
            continue

        max_duration = max(data['durations'])
        total_mb = sum(data['mbytes'])
        total_ops = sum(data['ops'])

        logging.info('[%s] %fs - %fmb/s - %fops/s - %fus/op',
                     kind, max_duration, total_mb, total_ops,
                     max_duration / total_ops * 1000 * 1000)

    for ssh_client, chan in nsqd_chans:
        chan.close()


def _find_instances():
    session = get_session()
    ec2 = session.resource('ec2')
    return [i for i in ec2.instances.all() if
            i.state['Name'] == 'running' and any(t['Key'] == 'nsq_bench' for t in i.tags)]


def decomm():
    instances = _find_instances()
    logging.info('terminating instances %s' % ','.join(i.id for i in instances))
    for instance in instances:
        instance.terminate()


if __name__ == '__main__':
    tornado.options.define('region', type=str, default='us-east-1',
                           help='EC2 region to launch instances')
    tornado.options.define('nsqd_count', type=int, default=3,
                           help='how many nsqd instances to launch')
    tornado.options.define('worker_count', type=int, default=3,
                           help='how many worker instances to launch')
    # ubuntu 18.04 HVM instance store us-east-1
    tornado.options.define('ami', type=str, default='ami-0938f2289b3fa3f5b',
                           help='AMI ID')
    tornado.options.define('ssh_key_name', type=str, default='default',
                           help='SSH key name')
    tornado.options.define('instance_type', type=str, default='c3.2xlarge',
                           help='EC2 instance type')
    tornado.options.define('msg_size', type=int, default=200,
                           help='size of message')
    tornado.options.define('rdy', type=int, default=10000,
                           help='RDY count to use for bench_reader')
    tornado.options.define('mode', type=str, default='pubsub',
                           help='the benchmark mode (pub, pubsub)')
    tornado.options.define('commit', type=str, default='master',
                           help='the git commit')
    tornado.options.define('golang_version', type=str, default='1.14.3',
                           help='the go version')
    tornado.options.parse_command_line()

    logging.getLogger('paramiko').setLevel(logging.WARNING)
    warnings.simplefilter('ignore')

    cmd_name = sys.argv[-1]
    cmd_map = {
        'bootstrap': bootstrap,
        'run': run,
        'decomm': decomm
    }
    cmd = cmd_map.get(cmd_name, bootstrap)

    sys.exit(cmd())
