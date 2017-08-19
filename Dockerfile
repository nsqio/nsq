FROM alpine:3.6

EXPOSE 4150 4151 4160 4161 4170 4171

VOLUME /data
VOLUME /etc/ssl/certs

COPY dist/docker/bin/ /usr/local/bin/
RUN ln -s /usr/local/bin/*nsq* / \
    && ln -s /usr/local/bin/*nsq* /bin/
