# Third-Party Dependencies

SynthRAN original code is licensed under Apache-2.0. External projects remain separate dependencies and are not relicensed by this repository.

`dependencies.lock.yml` is the machine-readable source of truth for dependencies SynthRAN consumes directly. Infrastructure internals consumed only by 5g-Ansible are intentionally not duplicated here; their provenance and licenses must be reviewed in the pinned 5g-Ansible repository.

| Dependency | Purpose | Locked identity | Reuse | License status |
|---|---|---|---|---|
| `RA-Nayreed/5g-Ansible` | SLICES provider context, 5G infrastructure authority, and structured deployment progress | `4799f4d93577840a56255fef1a1eb7fb6017b87f` | External detached checkout at `.deps/5g_ansible-r2lab`; invoked only through `bin/fiveg` | `NOASSERTION` in the SynthRAN lock; do not copy or redistribute upstream source without reviewing its own repository |
| `RA-Nayreed/Amber` | Ambient-IoT discrete-event source model | `08dd6bd445e607ad3accf4e9a2dff51a499ebdf9` | External detached checkout at `.deps/amber`; SynthRAN experiment adapters remain local | BSD-3-Clause |
| `eclipse-mosquitto` | Experiment-owned MQTT broker | `2.1.2-alpine@sha256:6f8d8a947c506f8a2290ec65cd4bd2bc7cb4d43fb5f6271f861cb013e2ef9797` | Container image | EPL-2.0 OR EDL-1.0; retain image notices |
| iperf3 | Controlled research load generation | source `3.21`, `sha256:656e4405ebd620121de7ceca3eaf43a88f79ea1b857d041a6a0b1314801acdd8` | Source-locked research tool | BSD-3-Clause |
| Miniforge3 | Conda distribution used by CI and recommended locally | `26.3.2-2`, Linux x86-64 installer `sha256:42260ffe3830fb953d5eee1bbb32229ff06aa7c3833c1ed7a9a0420a95685d94` | External environment bootstrap | BSD-3-Clause for installer code; installed packages retain their own licenses |
| Python | SynthRAN runtime | `3.12.13` | Conda package from `conda-forge` | PSF-2.0 |
| Git | Immutable dependency synchronization and repository hooks | `2.51.0` | Conda package from `conda-forge` | GPL-2.0-only |
| Setuptools | Python build backend | `83.0.0` | Conda package from `conda-forge` | MIT |
| Eclipse Paho MQTT Python | MQTT collector/publisher client | `2.1.0` | Conda package from `conda-forge` | EPL-2.0 OR EDL-1.0 |
| Apache PyArrow | Deterministic Parquet conversion | `21.0.0` | Conda package from `conda-forge` | Apache-2.0 |
| Ansible Core | Runtime required by the pinned 5g-Ansible machine checkout | `2.20.5` | Conda package from `conda-forge`; SynthRAN does not own upstream playbooks/collections | GPL-3.0-or-later |
| SimPy | Amber discrete-event runtime | `4.1.1` | Conda package from `conda-forge` | MIT |
| NumPy | Amber numerical runtime | `2.3.2` | Conda package from `conda-forge` | BSD-3-Clause |
| pandas | Amber trace and result processing | `2.3.2` | Conda package from `conda-forge` | BSD-3-Clause |
| Matplotlib | Amber plotting/import dependency | `3.10.5` | Conda package from `conda-forge` | PSF-based license; retain package notices |
| openpyxl | Amber energy-trace workbook reader | `3.1.5` | Conda package from `conda-forge` | MIT |
| `actions/checkout` | CI repository checkout | `d23441a48e516b6c34aea4fa41551a30e30af803` | GitHub Action | MIT |
| `conda-incubator/setup-miniconda` | CI Conda/Miniforge bootstrap | `8ee1f361103df19b6f8c8655fd3967a8ecb162d5` | GitHub Action | BSD-3-Clause |
| `gitleaks/gitleaks-action` | CI history secret scanning | `e0c47f4f8be36e29cdc102c57e68cb5cbf0e8d1e` | GitHub Action | MIT |

## Upstream-owned infrastructure dependencies

5g-Ansible may itself consume Open5GS, srsRAN, Kubernetes/Helm tooling, Ansible collections, physical-radio images, Python bootstrap packages, or other deployment dependencies. Those are **not SynthRAN dependencies merely because SynthRAN invokes 5g-Ansible**. SynthRAN neither pins nor overlays them. Review the exact pinned 5g-Ansible commit when their provenance or redistribution terms matter.

The same ownership rule applies to provider tooling and deployment progress. The controller environment must make the commands required by the pinned 5g-Ansible machine interface available, but SynthRAN does not implement, vendor, parse, or reinterpret their deployment logic or Ansible output. SynthRAN consumes the versioned upstream machine result and `fiveg/event/v1` progress records.

## Maintenance rules

1. Resolve mutable tags or branches to immutable commits, source hashes, or image digests before direct use.
2. Add a dependency to the SynthRAN lock only when SynthRAN consumes it directly.
3. For a 5g-Ansible-internal change, update and validate 5g-Ansible first, then pin the reviewed commit in SynthRAN.
4. Preserve upstream notices when an upstream artifact is redistributed.
5. Do not copy from a dependency whose redistribution terms are unclear.
6. Update this document whenever a direct dependency identity or license review changes.

`environment.yml` and the Conda section of `dependencies.lock.yml` lock direct package versions only. Conda resolves transitive packages and platform builds when the environment is created. Platform-specific artifact lock files must be generated and reviewed before an environment is described as fully artifact-reproducible.

This file is provenance documentation, not legal advice. An unasserted license status is a release blocker for copied or redistributed upstream material, not for invoking a separately obtained dependency through its documented interface.
