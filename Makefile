PREFIX=/usr/local
DESTDIR=
BLDFLAGS=
BINDIR=${PREFIX}/bin

BLDDIR = build
EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

APPS = nsqd nsqlookupd nsqadmin nsq_pubsub nsq_to_nsq nsq_to_file nsq_to_http nsq_tail nsq_stat to_nsq
all: $(APPS)

$(BLDDIR)/nsqd:        $(wildcard apps/nsqd/*.go       nsqd/*.go       nsq/*.go internal/*/*.go)
$(BLDDIR)/nsqlookupd:  $(wildcard apps/nsqlookupd/*.go nsqlookupd/*.go nsq/*.go internal/*/*.go)
$(BLDDIR)/nsqadmin:    $(wildcard apps/nsqadmin/*.go   nsqadmin/*.go nsqadmin/templates/*.go internal/*/*.go)
$(BLDDIR)/nsq_pubsub:  $(wildcard apps/nsq_pubsub/*.go  nsq/*.go internal/*/*.go)
$(BLDDIR)/nsq_to_nsq:  $(wildcard apps/nsq_to_nsq/*.go  nsq/*.go internal/*/*.go)
$(BLDDIR)/nsq_to_file: $(wildcard apps/nsq_to_file/*.go nsq/*.go internal/*/*.go)
$(BLDDIR)/nsq_to_http: $(wildcard apps/nsq_to_http/*.go nsq/*.go internal/*/*.go)
$(BLDDIR)/nsq_tail:    $(wildcard apps/nsq_tail/*.go    nsq/*.go internal/*/*.go)
$(BLDDIR)/nsq_stat:    $(wildcard apps/nsq_stat/*.go             internal/*/*.go)
$(BLDDIR)/to_nsq:      $(wildcard apps/to_nsq/*.go               internal/*/*.go)

$(BLDDIR)/%:
	@mkdir -p $(dir $@)
	go build ${BLDFLAGS} -o $@ ./apps/$*

$(APPS): %: $(BLDDIR)/%

clean:
	rm -fr $(BLDDIR)

.PHONY: install clean all
.PHONY: $(APPS)

install: $(APPS)
	install -m 755 -d ${DESTDIR}${BINDIR}
	for APP in $^ ; do install -m 755 ${BLDDIR}/$$APP ${DESTDIR}${BINDIR}/$$APP${EXT} ; done
