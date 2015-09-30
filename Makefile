PREFIX=/usr/local
DESTDIR=
GOFLAGS=
BINDIR=${PREFIX}/bin

NSQD_SRCS = $(wildcard apps/nsqd/*.go nsqd/*.go nsq/*.go internal/*/*.go)
NSQLOOKUPD_SRCS = $(wildcard apps/nsqlookupd/*.go nsqlookupd/*.go nsq/*.go internal/*/*.go)
NSQADMIN_SRCS = $(wildcard apps/nsqadmin/*.go nsqadmin/*.go nsqadmin/templates/*.go internal/*/*.go)
NSQ_PUBSUB_SRCS = $(wildcard apps/nsq_pubsub/*.go nsq/*.go internal/*/*.go)
NSQ_TO_NSQ_SRCS = $(wildcard apps/nsq_to_nsq/*.go nsq/*.go internal/*/*.go)
NSQ_TO_FILE_SRCS = $(wildcard apps/nsq_to_file/*.go nsq/*.go internal/*/*.go)
NSQ_TO_HTTP_SRCS = $(wildcard apps/nsq_to_http/*.go nsq/*.go internal/*/*.go)
NSQ_TAIL_SRCS = $(wildcard apps/nsq_tail/*.go nsq/*.go internal/*/*.go)
NSQ_STAT_SRCS = $(wildcard apps/nsq_stat/*.go internal/*/*.go)
TO_NSQ_SRCS = $(wildcard apps/to_nsq/*.go internal/*/*.go)

APPS = nsqd nsqlookupd nsqadmin nsq_pubsub nsq_to_nsq nsq_to_file nsq_to_http nsq_tail nsq_stat to_nsq
BLDDIR = build

all: $(APPS)

$(BLDDIR)/%:
	@mkdir -p $(dir $@)
	go build ${GOFLAGS} -o $@ ./apps/$*

$(APPS): %: $(BLDDIR)/%

$(BLDDIR)/nsqd:        $(NSQD_SRCS)
$(BLDDIR)/nsqlookupd:  $(NSQLOOKUPD_SRCS)
$(BLDDIR)/nsqadmin:    $(NSQADMIN_SRCS)
$(BLDDIR)/nsq_pubsub:  $(NSQ_PUBSUB_SRCS)
$(BLDDIR)/nsq_to_nsq:  $(NSQ_TO_NSQ_SRCS)
$(BLDDIR)/nsq_to_file: $(NSQ_TO_FILE_SRCS)
$(BLDDIR)/nsq_to_http: $(NSQ_TO_HTTP_SRCS)
$(BLDDIR)/nsq_tail:    $(NSQ_TAIL_SRCS)
$(BLDDIR)/nsq_stat:    $(NSQ_STAT_SRCS)
$(BLDDIR)/to_nsq:      $(TO_NSQ_SRCS)

clean:
	rm -fr $(BLDDIR)

.PHONY: install clean all
.PHONY: $(APPS)

install: $(APPS)
	install -m 755 -d ${DESTDIR}${BINDIR}
	install -m 755 $(BLDDIR)/nsqlookupd  ${DESTDIR}${BINDIR}/nsqlookupd
	install -m 755 $(BLDDIR)/nsqd        ${DESTDIR}${BINDIR}/nsqd
	install -m 755 $(BLDDIR)/nsqadmin    ${DESTDIR}${BINDIR}/nsqadmin
	install -m 755 $(BLDDIR)/nsq_pubsub  ${DESTDIR}${BINDIR}/nsq_pubsub
	install -m 755 $(BLDDIR)/nsq_to_nsq  ${DESTDIR}${BINDIR}/nsq_to_nsq
	install -m 755 $(BLDDIR)/nsq_to_file ${DESTDIR}${BINDIR}/nsq_to_file
	install -m 755 $(BLDDIR)/nsq_to_http ${DESTDIR}${BINDIR}/nsq_to_http
	install -m 755 $(BLDDIR)/nsq_tail    ${DESTDIR}${BINDIR}/nsq_tail
	install -m 755 $(BLDDIR)/nsq_stat    ${DESTDIR}${BINDIR}/nsq_stat
	install -m 755 $(BLDDIR)/to_nsq      ${DESTDIR}${BINDIR}/to_nsq
