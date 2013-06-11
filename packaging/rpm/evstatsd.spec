Name		: evstatsd
Version		: 0.1
Release		: 1.20130610gitXXXXXXXX
Summary		: libevent-based statsd implementation
Group		: Applications/Internet

Source0		: %{name}-%{version}.tar.gz
URL		: https://github.com/bodgit/evstatsd
License		: BSD
Packager	: Matt Dainty <matt@bodgit-n-scarper.com>

BuildRoot	: %{_tmppath}/%{name}-%{version}-root
%if %{?el5:1}0
BuildRequires	: cmake28 >= 2.8.8
%elseif %{?el6:1}0
BuildRequires	: cmake28 >= 2.8.8
%else
BuildRequires	: cmake >= 2.8.8
%endif
BuildRequires	: libevent-devel >= 2
BuildRequires	: bison

%description
libevent-based statsd implementation

%prep
%setup -q

%build
%if %{?el5:1}0
%cmake28 .
%elseif %{?el6:1}0
%cmake28 .
%else
%cmake .
%endif
make %{?_smp_mflags}

%install
%{__rm} -rf %{buildroot}
make install DESTDIR=%{buildroot}
%{__mkdir_p} %{buildroot}%{_sysconfdir}/sysconfig
%{__mkdir_p} %{buildroot}%{_sysconfdir}/rc.d/init.d
for i in statsd ; do
	%{__install} -m 0644 packaging/rpm/${i}.sysconfig \
		%{buildroot}%{_sysconfdir}/sysconfig/${i}
	%{__install} -m 0755 packaging/rpm/${i}.init \
		%{buildroot}%{_sysconfdir}/rc.d/init.d/${i}
done

%posttrans
/sbin/service statsd condrestart >/dev/null 2>&1 || :

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root)
%config %attr(0640,-,-) %{_sysconfdir}/statsd.conf
%config %{_sysconfdir}/sysconfig/statsd
%{_sysconfdir}/rc.d/init.d/statsd
%{_sbindir}/statsd
%doc %{_mandir}/man5/statsd.conf.5*
%doc %{_mandir}/man8/statsd.8*

%changelog
* Mon Jun 10 2013 Matt Dainty <matt@bodgit-n-scarper.com> 0.1-1.20130610gitXXXXXXXX
- Initial version 0.1-1.20130610gitXXXXXXXX.
