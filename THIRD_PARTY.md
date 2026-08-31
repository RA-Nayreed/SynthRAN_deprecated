# Third-Party Dependencies

SynthRAN original code is licensed under Apache-2.0. External projects remain separate dependencies and are not relicensed by this repository.

The immutable identifiers below mirror `dependencies.lock.yml`. The lock file is the machine-readable source of truth.

| Dependency | Purpose | Locked version | Reuse | License status |
|---|---|---|---|---|
| `sopnode/5g_ansible` | SLICES 5G deployment | `a0149fc0dde39e2872945a0f3c91e804ece52d4f` | External detached checkout | No top-level license found in the reviewed tree; do not copy or publish derivative source without clarification |
| `contiki-ng/contiki-ng` | Firmware, RPL/6LoWPAN, Cooja | release 5.1 at `2b87baf3ebdde3c8e37ca791d2bc84bfd76c49a4` | External detached checkout and out-of-tree application | BSD-3-Clause unless a source file states otherwise |
| `RA-Nayreed/Amber` | 6G Ambient IoT discrete-event source model | `08dd6bd445e607ad3accf4e9a2dff51a499ebdf9` | External detached checkout at `.deps/amber`; SynthRAN-specific adapters remain in SynthRAN | BSD-3-Clause |
| `sopnode/open5gs-k8s` | Transitive Open5GS Kubernetes deployment | `e53601e5209425867413d45d3d01ed9a1b696de7` | Referenced through the `5g_ansible` adapter | MIT license present in the pinned tree |
| `turletti/srsran-helm` | Transitive srsRAN Helm deployment | `8dfb9890d127734cdcd6eee9df8c5d09b1a8076a` | Referenced through the `5g_ansible` adapter | License not yet asserted; inspect before copying or modifying upstream source |
| `eclipse-mosquitto` | Edge and central MQTT brokers | `2.1.2-alpine@sha256:6f8d8a947c506f8a2290ec65cd4bd2bc7cb4d43fb5f6271f861cb013e2ef9797` | Container image | EPL-2.0 OR EDL-1.0; retain image notices |
| `niloysh/open5gs` | Golden-path Open5GS NFs and SMF | `v2.6.4-aio@sha256:b41e5919f28edb1467b7c189302d7460d05e74cf5fb65f19126b5851334cb3ce` and `v2.7.0@sha256:eb8b23589c724ba2e18b783c544dbcd56f02da29f818d4cf1974853b53aee329` | Container images | License not yet asserted; retain image notices |
| `r2labuser/open5gs-amf-patched` | Golden-path patched AMF | `v2.7.0@sha256:13818df32958781f910a61a20df8b3c856ae5ef33d941517b959885182bd4295` | Container image | License not yet asserted; retain image notices |
| `r2labuser/mongodb` | Golden-path subscriber database | `4.4.4-debian-10-r0@sha256:95abfb776bb4e6ee34f7b5b1c811f978d132136035deacdb7143f798f0343a31` | Container image | License not yet asserted; retain image notices |
| `r2labuser/srsran-gnb-zmq-csi` | Golden-path RFSIM gNB | `v1.0.0.21@sha256:89ceaebc6adddb9900b3cae01316fadea8660aeff6ce60e1b335ba0a9d0ff9cd` | Container image | License not yet asserted; retain image notices |
| `ziyad-mabrouk/srsue` | Golden-path srsUE gateway | `v1.0@sha256:c1c9eb2119e48d1f5f9120c71cd539c606fca6c242a2e430409269c121610bb2` | Container image | License not yet asserted; retain image notices |
| BusyBox | Golden-path readiness/log helpers | `1.32.0@sha256:31a54a0cf86d7354788a8265f60ae6acb4b348a67efbcf7c1007dd3cf7af05ab` and `1.36@sha256:b7f3d86d6e84fc17718c48bcde1450807faa2d56704205c697b4bd5df7b9e29f` | Container images | GPL-2.0-only |
| Miniforge3 | Conda distribution used by CI and recommended locally | `26.3.2-2`, Linux x86-64 installer `sha256:42260ffe3830fb953d5eee1bbb32229ff06aa7c3833c1ed7a9a0420a95685d94` | External environment bootstrap | Installer code is BSD-3-Clause; installed packages retain their own licenses |
| Python | SynthRAN runtime | `3.12.13` | Conda package from `conda-forge` | PSF-2.0 |
| OpenJDK | Java runtime required by Cooja | `21.0.9` | Conda package from `conda-forge` | GPL-2.0-with-classpath-exception |
| Git | Detached dependency synchronization and repository hooks | `2.51.0` | Conda package from `conda-forge` | GPL-2.0-only |
| Ansible Core | Golden-path controller | `2.20.5` | Conda package from `conda-forge` | GPL-3.0-or-later |
| SimPy | Amber discrete-event runtime | `4.1.1` | Conda package from `conda-forge` | MIT |
| NumPy | Amber numerical runtime | `2.3.2` | Conda package from `conda-forge` | BSD-3-Clause |
| pandas | Amber trace and result processing | `2.3.2` | Conda package from `conda-forge` | BSD-3-Clause |
| Matplotlib | Amber upstream plotting import dependency | `3.10.5` | Conda package from `conda-forge` | PSF-based license; retain package notices |
| openpyxl | Amber energy-trace workbook reader | `3.1.5` | Conda package from `conda-forge` | MIT |
| `kubernetes.core` | Golden-path Kubernetes and Helm modules | `6.5.0` | Ansible Galaxy collection | GPL-3.0-or-later |
| `community.general` | Kernel module management required by locked upstream preparation | `13.0.1` | Ansible Galaxy collection | GPL-3.0-or-later; individual files may use documented compatible licenses |
| `ansible.posix` | Mount management required by locked upstream preparation | `2.2.2` | Ansible Galaxy collection | GPL-3.0-or-later |
| Helm | Golden-path Kubernetes package deployment on the RAN node | `3.18.4`, Linux AMD64 archive `sha256:f8180838c23d7c7d797b208861fecb591d9ce1690d8704ed1e4cb8e2add966c1` | Installed by the preparation playbook | Apache-2.0 |
| yq | Locked Helm values transformation on the RAN node | `4.45.1`, Linux AMD64 `sha256:654d2943ca1d3be2024089eb4f270f4070f491a0610481d128509b2834870049` | Installed by the preparation playbook | MIT |
| Kubernetes Python client | Ansible Kubernetes modules on prepared nodes | `32.0.1` | Installed in `/opt/synthran-venv` and version-checked by live doctor | Apache-2.0 |
| Open5GS Python bootstrap packages | Subscriber generation and MongoDB insertion on prepared nodes | `dnspython==2.3.0`, `pymongo==4.5.0`, `python-dateutil==2.8.2`, `ruamel.yaml==0.18.5`, `six==1.16.0` | Installed in `/opt/synthran-venv` and version-checked by live doctor | ISC; Apache-2.0; Apache-2.0 OR BSD-3-Clause; MIT; MIT, respectively |
| Eclipse Paho MQTT Python | Collector MQTT client | `2.1.0` | Conda package from `conda-forge` | EPL-2.0 OR EDL-1.0 |
| Apache PyArrow | Parquet conversion | `21.0.0` | Conda package from `conda-forge` | Apache-2.0 |
| PyPA Setuptools | Python build backend | `83.0.0` | Conda package from `conda-forge` | MIT |
| `actions/checkout` | CI repository checkout | commit `d23441a48e516b6c34aea4fa41551a30e30af803` | GitHub Action | MIT |
| `conda-incubator/setup-miniconda` | CI Conda/Miniforge environment bootstrap | v4 at commit `8ee1f361103df19b6f8c8655fd3967a8ecb162d5` | GitHub Action | MIT |
| `gitleaks/gitleaks-action` | CI secret scanning | commit `e0c47f4f8be36e29cdc102c57e68cb5cbf0e8d1e` | GitHub Action | MIT |

## Maintenance rules

1. Resolve mutable tags or branches to immutable commits or image digests before use.
2. Change only one dependency at a time.
3. Record the source ref, resolved identifier, date, license review, and compatibility result.
4. Preserve all upstream notices when an upstream artifact is redistributed.
5. Do not copy from a dependency whose redistribution terms are unclear.
6. Regenerate the SBOM when distributable images or packages are introduced.

`environment.yml` and the Conda section of `dependencies.lock.yml` lock direct package versions only. Conda resolves transitive packages and platform builds when the environment is created. Platform-specific artifact lock files must be generated and reviewed before an environment is described as fully reproducible.

This file is provenance documentation, not legal advice. Unasserted license status is a release blocker for copied or redistributed upstream material, not for merely linking to a separately obtained repository.
