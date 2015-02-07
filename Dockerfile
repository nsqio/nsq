FROM scratch

ADD dist/docker/bin/ /

VOLUME /data
VOLUME /etc/ssl/certs
