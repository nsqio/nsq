FROM busybox

ADD dist/docker/bin/ /

EXPOSE 4150 4151 4160 4161 4170 4171

VOLUME /data
VOLUME /etc/ssl/certs
