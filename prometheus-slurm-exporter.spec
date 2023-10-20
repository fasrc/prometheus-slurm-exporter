Name:           prometheus-slurm-exporter
Version:        1.0
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
./bootstrap.sh

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p %{buildroot}/opt/prometheus-slurm-exporter
rsync -av %{_topdir}/BUILD/prometheus-slurm-exporter/ %{buildroot}/opt/prometheus-slurm-exporter/

install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exportersystemd/prometheus-slurm-exporter-lsload.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-lsload.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exportersystemd/prometheus-slurm-exporter-sdiag.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-sdiag.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exportersystemd/prometheus-slurm-exporter-sshare.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-sshare.service
install -D -m644 %{_topdir}/BUILD/prometheus-slurm-exportersystemd/prometheus-slurm-exporter-seas.service %{buildroot}/%{_unitdir}/prometheus-slurm-exporter-seas.service

%files
%defattr(-,root,root,-)
/opt/prometheus-slurm-exporter/*
%{_unitdir}/prometheus-slurm-exporter-lsload.service
%{_unitdir}/prometheus-slurm-exporter-sdiag.service
%{_unitdir}/prometheus-slurm-exporter-sshare.service
%{_unitdir}/prometheus-slurm-exporter-seas.service

%post lsload
%systemd_post prometheus-slurm-exporter-lsload.service
%preun lsload
%systemd_preun prometheus-slurm-exporter-lsload.service
%postun lsload
%systemd_postun_with_restart prometheus-slurm-exporter-lsload.service

%post sdiag
%systemd_post prometheus-slurm-exporter-sdiag.service
%preun sdiag
%systemd_preun prometheus-slurm-exporter-sdiag.service
%postun sdiag
%systemd_postun_with_restart prometheus-slurm-exporter-sdiag.service

%post sshare
%systemd_post prometheus-slurm-exporter-sshare.service
%preun sshare
%systemd_preun prometheus-slurm-exporter-sshare.service
%postun sshare
%systemd_postun_with_restart prometheus-slurm-exporter-sshare.service

%post seas
%systemd_post prometheus-slurm-exporter-seas.service
%preun seas
%systemd_preun prometheus-slurm-exporter-seas.service
%postun seas
%systemd_postun_with_restart prometheus-slurm-exporter-seas.service

%changelog
* Fri Oct 20 2023 Paul Edmon <pedmon@cfa.harvard.edu>
- Initial version.
