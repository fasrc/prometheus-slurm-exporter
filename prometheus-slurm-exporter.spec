Name:           prometheus-slurm-exporter
Version:        1.08
Release:        1%{?dist}
Summary:        Prometheus Exporter for Slurm

License:        See LICENSE file in gitrepo.
URL:            https://github.com/fasrc/prometheus-slurm-exporter

%description
Prometheus Exporter for Slurm. Uses the prometheus python implementation.

%prep

%build
rm -rf prometheus-slurm-exporter
git clone https://github.com/fasrc/prometheus-slurm-exporter.git
cd prometheus-slurm-exporter
rm -rf .git
rm -rf .gitignore
./bootstrap.sh

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p %{buildroot}/opt/prometheus-slurm-exporter
rsync -av %{_topdir}/BUILD/prometheus-slurm-exporter/ %{buildroot}/opt/prometheus-slurm-exporter/

install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exporter/systemd/prometheus-slurm-exporter-lsload.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-lsload.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exporter/systemd/prometheus-slurm-exporter-sdiag.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-sdiag.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exporter/systemd/prometheus-slurm-exporter-sshare.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-sshare.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exporter/systemd/prometheus-slurm-exporter-seas.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-seas.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exporter/systemd/prometheus-slurm-exporter-kempner.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-kempner.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exporter/systemd/prometheus-slurm-exporter-klsload.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-klsload.service

%files
%defattr(-,root,root,-)
/opt/prometheus-slurm-exporter/*
%{_unitdir}/prometheus-slurm-exporter-lsload.service
%{_unitdir}/prometheus-slurm-exporter-sdiag.service
%{_unitdir}/prometheus-slurm-exporter-sshare.service
%{_unitdir}/prometheus-slurm-exporter-seas.service
%{_unitdir}/prometheus-slurm-exporter-kempner.service
%{_unitdir}/prometheus-slurm-exporter-klsload.service

%changelog
* Tue Aug 27 2024 Paul Edmon <pedmon@cfa.harvard.edu>
- Adding kempner exporters.
* Fri Dec 1 2023 Paul Edmon <pedmon@cfa.harvard.edu>
- Fixing bug.
* Tue Nov 28 2023 Paul Edmon <pedmon@cfa.harvard.edu>
- Added power monitoring.
* Fri Oct 20 2023 Paul Edmon <pedmon@cfa.harvard.edu>
- Initial version.
