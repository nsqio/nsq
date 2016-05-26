%define name nsq
%define version 0.3.8
%define release 1
%define path usr/local
%define group Database/Applications
%define __os_install_post %{nil}

Summary:    nsq
Name:       %{name}
Version:    %{version}
Release:    %{release}
Group:      %{group}
Packager:   Matt Reiferson <mreiferson@gmail.com>
License:    Apache
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}
AutoReqProv: no
# we just assume you have go installed. You may or may not have an RPM to depend on.
# BuildRequires: go

%description 
NSQ - A realtime distributed messaging platform
https://github.com/nsqio/nsq

%prep
mkdir -p $RPM_BUILD_DIR/%{name}-%{version}-%{release}
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}
git clone git@github.com:nsqio/nsq.git

%build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq
make PREFIX=/%{path}

%install
export DONT_STRIP=1
rm -rf $RPM_BUILD_ROOT
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq
make PREFIX=/${path} DESTDIR=$RPM_BUILD_ROOT install

%files
/%{path}/bin/nsqadmin
/%{path}/bin/nsqd
/%{path}/bin/nsqlookupd
/%{path}/bin/nsq_pubsub
/%{path}/bin/nsq_to_file
/%{path}/bin/nsq_to_http
/%{path}/bin/nsq_to_nsq
/%{path}/bin/nsq_tail
/%{path}/bin/nsq_stat
/%{path}/bin/to_nsq
