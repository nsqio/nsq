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
	go build ${GOFLAGS} -o $(abspath $@) ./$*

$(BINARIES): %: $(BLDDIR)/%
$(APPS): %: $(BLDDIR)/apps/%

$(BLDDIR)/apps/nsqd: $(NSQD_SRCS)
$(BLDDIR)/apps/nsqlookupd: $(NSQLOOKUPD_SRCS)
$(BLDDIR)/apps/nsqadmin: $(NSQADMIN_SRCS)
$(BLDDIR)/apps/nsq_pubsub: $(NSQ_PUBSUB_SRCS)
$(BLDDIR)/apps/nsq_to_nsq: $(NSQ_TO_NSQ_SRCS)
$(BLDDIR)/apps/nsq_to_file: $(NSQ_TO_FILE_SRCS)
$(BLDDIR)/apps/nsq_to_http: $(NSQ_TO_HTTP_SRCS)
$(BLDDIR)/apps/nsq_tail: $(NSQ_TAIL_SRCS)
$(BLDDIR)/apps/nsq_stat: $(NSQ_STAT_SRCS)
$(BLDDIR)/apps/to_nsq: $(TO_NSQ_SRCS)

clean:
	rm -fr $(BLDDIR)

.PHONY: install clean all
.PHONY: $(BINARIES)
.PHONY: $(APPS)

install: $(BINARIES) $(EXAMPLES)
	install -m 755 -d ${DESTDIR}${BINDIR}
	install -m 755 $(BLDDIR)/apps/nsqlookupd ${DESTDIR}${BINDIR}/nsqlookupd
	install -m 755 $(BLDDIR)/apps/nsqd ${DESTDIR}${BINDIR}/nsqd
	install -m 755 $(BLDDIR)/apps/nsqadmin ${DESTDIR}${BINDIR}/nsqadmin
	install -m 755 $(BLDDIR)/apps/nsq_pubsub ${DESTDIR}${BINDIR}/nsq_pubsub
	install -m 755 $(BLDDIR)/apps/nsq_to_nsq ${DESTDIR}${BINDIR}/nsq_to_nsq
	install -m 755 $(BLDDIR)/apps/nsq_to_file ${DESTDIR}${BINDIR}/nsq_to_file
	install -m 755 $(BLDDIR)/apps/nsq_to_http ${DESTDIR}${BINDIR}/nsq_to_http
	install -m 755 $(BLDDIR)/apps/nsq_tail ${DESTDIR}${BINDIR}/nsq_tail
	install -m 755 $(BLDDIR)/apps/nsq_stat ${DESTDIR}${BINDIR}/nsq_stat
	install -m 755 $(BLDDIR)/apps/to_nsq ${DESTDIR}${BINDIR}/to_nsq
