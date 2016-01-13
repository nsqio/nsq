#!/usr/bin/env python
import sys
import logging
import time
import datetime
import socket
import warnings
import hashlib

import boto.ec2
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

    stdout = ''
    stderr = ''
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


def connect_to_ec2():
    return boto.ec2.connect_to_region(
        tornado.options.options.region,
        aws_access_key_id=tornado.options.options.access_key,
        aws_secret_access_key=tornado.options.options.secret_key)


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
            'sudo -S curl -s -o /usr/local/bin/gpm \
                https://raw.githubusercontent.com/pote/gpm/v1.2.3/bin/gpm',
            'sudo -S chmod +x /usr/local/bin/gpm',
            'cd go/src/github.com/nsqio/nsq && \
                GOPATH=/home/ubuntu/go PATH=$PATH:/usr/local/go/bin gpm install',
            'cd go/src/github.com/nsqio/nsq/apps/nsqd && \
                GOPATH=/home/ubuntu/go /usr/local/go/bin/go build',
            'cd go/src/github.com/nsqio/nsq/bench/bench_writer && \
                GOPATH=/home/ubuntu/go /usr/local/go/bin/go build',
            'cd go/src/github.com/nsqio/nsq/bench/bench_reader && \
                GOPATH=/home/ubuntu/go /usr/local/go/bin/go build',
            'sudo -S mkdir -p /mnt/nsq',
            'sudo -S chmod 777 /mnt/nsq']:
        ssh_cmd(ssh_client, cmd, timeout=10)


def bootstrap():
    conn = connect_to_ec2()

    total_count = tornado.options.options.nsqd_count + tornado.options.options.worker_count
    logging.info('launching %d instances', total_count)
    run = conn.run_instances(
        tornado.options.options.ami,
        min_count=total_count,
        max_count=total_count,
        key_name=tornado.options.options.ssh_key_name,
        instance_type=tornado.options.options.instance_type,
        security_groups=['default'])

    logging.info('waiting for instances to launch...')

    while all(i.state != 'running' for i in run.instances):
        waiting_for = [i.id for i in run.instances if i.state != 'running']
        logging.info('... sleeping for 5s (waiting for %s)', ', '.join(waiting_for))
        time.sleep(5)
        for instance in run.instances:
            instance.update()

    for instance in run.instances:
        if not instance.tags:
            conn.create_tags([instance.id], {'nsq_bench': '1'})

    hosts = [(i.id, i.public_dns_name) for i in run.instances]

    try:
        c = 0
        for id, addr in hosts:
            c += 1
            logging.info('(%d) bootstrapping %s (%s)', c, addr, id)
            _bootstrap(addr)
    except Exception:
        logging.exception('bootstrap failed')
        decomm()


def run():
    hosts = _find_hosts()

    logging.info('launching nsqd on %d host(s)', tornado.options.options.nsqd_count)

    nsqd_chans = []
    nsqd_hosts = hosts[:tornado.options.options.nsqd_count]
    for id, addr in nsqd_hosts:
        try:
            ssh_client = ssh_connect_with_retries(addr)
            for cmd in [
                    'sudo -S pkill -f nsqd',
                    'sudo -S rm -f /mnt/nsq/*.dat',
                    'GOMAXPROCS=32 ./go/src/github.com/nsqio/nsq/apps/nsqd/nsqd \
                        --data-path=/mnt/nsq --mem-queue-size=10000000 --max-rdy-count=%s' % (
                        tornado.options.options.rdy
                        )]:
                nsqd_chans.append((ssh_client, ssh_cmd_async(ssh_client, cmd)))
        except Exception:
            logging.exception('failed')

    nsqd_tcp_addrs = [h[1] for h in nsqd_hosts]

    dt = datetime.datetime.utcnow()
    deadline = dt + datetime.timedelta(seconds=30)

    logging.info('launching %d producer(s) on %d host(s)',
                 tornado.options.options.nsqd_count * tornado.options.options.worker_count,
                 tornado.options.options.worker_count)

    worker_chans = []

    producer_hosts = hosts[tornado.options.options.nsqd_count:]
    for id, addr in producer_hosts:
        for nsqd_tcp_addr in nsqd_tcp_addrs:
            topic = hashlib.md5(addr).hexdigest()
            try:
                ssh_client = ssh_connect_with_retries(addr)
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

        consumer_hosts = hosts[tornado.options.options.nsqd_count:]
        for id, addr in consumer_hosts:
            for nsqd_tcp_addr in nsqd_tcp_addrs:
                topic = hashlib.md5(addr).hexdigest()
                try:
                    ssh_client = ssh_connect_with_retries(addr)
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
                line = chan.recv_stderr(4096)
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


def _find_hosts():
    conn = connect_to_ec2()
    reservations = conn.get_all_instances()
    instances = [inst for res in reservations for inst in res.instances]

    hosts = []
    for instance in instances:
        if not instance.tags or instance.state != 'running':
            continue
        if 'nsq_bench' in instance.tags:
            hosts.append((instance.id, instance.public_dns_name))

    return hosts


def decomm():
    conn = connect_to_ec2()
    hosts = _find_hosts()
    host_ids = [h[0] for h in hosts]
    logging.info('terminating instances %s' % ','.join(host_ids))
    conn.terminate_instances(host_ids)


if __name__ == '__main__':
    tornado.options.define('region', type=str, default='us-east-1',
                           help='EC2 region to launch instances')
    tornado.options.define('nsqd_count', type=int, default=3,
                           help='how many nsqd instances to launch')
    tornado.options.define('worker_count', type=int, default=3,
                           help='how many worker instances to launch')
    tornado.options.define('access_key', type=str, default='',
                           help='AWS account access key')
    tornado.options.define('secret_key', type=str, default='',
                           help='AWS account secret key')
    tornado.options.define('ami', type=str, default='ami-48fd2120',
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
    tornado.options.define('golang_version', type=str, default='1.5.1',
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
