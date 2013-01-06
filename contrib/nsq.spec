%define name nsq
%define version 0.2.12
%define release 21
%define path usr/local
%define group Database/Applications
%define __os_install_post %{nil}

Summary:    nsq
Name:       %{name}
Version:    %{version}
Release:    %{release}
Group:      %{group}
Packager:   Matt Reiferson <mattr@bit.ly>
License:    Apache
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}
AutoReqProv: no
# we just assume you have go installed. You may or may not have an RPM to depend on.
# BuildRequires: go

%description 
nsq - realtime distributed message processing at scale
https://github.com/bitly/nsq

%prep
mkdir -p $RPM_BUILD_DIR/%{name}-%{version}-%{release}
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}
git clone git@github.com:bitly/nsq.git
# cd nsq
# git checkout %{commit}

%build
# source GOROOT, GOPATH, GOARCH, etc
# . /etc/profile.d/go.sh
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq/nsqd
go build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq/nsqlookupd
go build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq/nsqadmin
go build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq/examples/nsq_to_file
go build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq/examples/nsq_pubsub
go build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq/examples/nsq_to_http
go build

%install
export DONT_STRIP=1
export gopath=$RPM_BUILD_DIR/%{name}-%{version}-%{release}
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/%{path}/bin
mkdir -p $RPM_BUILD_ROOT/%{path}/share/nsqadmin/templates
mkdir -p ${RPM_BUILD_ROOT}${GOPATH}
cp $gopath/nsq/nsqd/nsqd $RPM_BUILD_ROOT/%{path}/bin
cp $gopath/nsq/nsqlookupd/nsqlookupd $RPM_BUILD_ROOT/%{path}/bin
cp $gopath/nsq/nsqadmin/nsqadmin $RPM_BUILD_ROOT/%{path}/bin
cp $gopath/nsq/examples/nsq_to_file/nsq_to_file $RPM_BUILD_ROOT/%{path}/bin
cp $gopath/nsq/examples/nsq_pubsub/nsq_pubsub $RPM_BUILD_ROOT/%{path}/bin
cp $gopath/nsq/examples/nsq_to_http/nsq_to_http $RPM_BUILD_ROOT/%{path}/bin
cp -R $gopath/nsq/nsqadmin/templates $RPM_BUILD_ROOT/%{path}/share/nsqadmin
cp -R $gopath/src ${RPM_BUILD_ROOT}${GOPATH}
cp -R $gopath/pkg ${RPM_BUILD_ROOT}${GOPATH}

%files
/%{path}/bin/nsqadmin
/%{path}/bin/nsqd
/%{path}/bin/nsqlookupd
/%{path}/bin/nsq_to_file
/%{path}/bin/nsq_pubsub
/%{path}/bin/nsq_to_http
/%{path}/share/nsqadmin/templates
