FROM scratch

ADD dist/docker/bin/ /

VOLUME /etc/ssl/certs
