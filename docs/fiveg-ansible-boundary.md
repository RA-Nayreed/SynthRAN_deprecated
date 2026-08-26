# 5g-Ansible ownership boundary

SynthRAN uses the dependency-lock entry for `fiveg_ansible` as the implementation source for 5G deployment mechanics whenever the pinned upstream behavior is compatible with SynthRAN's authority and evidence contract.

## Rule

SynthRAN owns experiment identity, provider/lease/allocation authority, exact resource ownership, acceptance ordering, evidence, resumability, deterministic workloads, and exact cleanup. 5g-Ansible owns deployment mechanics and R2Lab device mechanics where those mechanics can be invoked without broadening the selected resource set or weakening SSH trust.

The R2Lab transport follows the topology used by 5g-Ansible: the operator reaches `faraday.inria.fr`, and Faraday performs provider/RRU/UE operations. SynthRAN centralizes physical-path OpenSSH policy in `synthran.utils.ssh` and isolates calls from ambient `~/.ssh/config` with `-F /dev/null`. Host-key checking remains strict.

## Cross-check against pinned 5g-Ansible

The cross-check was performed against the exact `fiveg_ansible` commit in `dependencies.lock.yml`, including `playbooks/deploy.yml`, `playbooks/deploy_r2lab.yml`, `roles/r2lab/*`, `roles/5g/open5gs/*`, and `roles/5g/srsRAN/*`.

| Capability | Owner after refactor | Reason |
| --- | --- | --- |
| Open5GS configuration/deployment mechanics | 5g-Ansible, invoked through the pinned checkout | Upstream already owns the Open5GS roles. SynthRAN supplies locked images, selected subscriber, authority gates, and postconditions. |
| srsRAN N300/N320 RF values and chart source | pinned upstream srsRAN/5g-Ansible dependencies | RF configuration is not reimplemented in SynthRAN. |
| R2Lab UE setup/connect/stop mechanics | 5g-Ansible role copies | Upstream modem logic is reused. The isolated copy is strict-SSH hardened before execution because the pinned roles contain `StrictHostKeyChecking=no` and, in setup, `UserKnownHostsFile=/dev/null`. |
| R2Lab RRU power mechanics | provider commands through the Faraday topology | The upstream RRU role is only `rhubarbe pdu on/status`; SynthRAN retains the same provider primitives because it additionally requires current lease authority and exact observed post-state before accepting a mutation. |
| R2Lab global cleanup | SynthRAN exact cleanup | Upstream `roles/r2lab/cleanup` runs `all-off`, which is incompatible with selected-resource ownership. It must never be called from the canonical SynthRAN lifecycle. |
| srsRAN gNB deploy/start lifecycle | SynthRAN lifecycle wrapper over pinned chart/config | Upstream `5g/srsRAN/deploy` uninstalls an existing release and immediately starts a replacement. SynthRAN requires zero-replica staging, run labels/annotations, resumability, stable N2 evidence, and exact scale-to-zero recovery. |
| Kubernetes/Open5GS observations | SynthRAN | These are evidence/postcondition checks, not alternative deployment mechanics. |
| SLICES reservation/allocation and R2Lab lease | SynthRAN | 5g-Ansible does not own SynthRAN's experiment authority model. |
| deterministic IoT workload and evidence | SynthRAN | Experiment semantics are outside 5g-Ansible. |

## SSH contract

All SynthRAN-owned SSH/SCP construction on the physical R2Lab path uses `synthran.utils.ssh`.

Required properties:

- `BatchMode=yes`
- bounded `ConnectTimeout`
- `StrictHostKeyChecking=yes`
- explicit `UserKnownHostsFile` when a run supplies one
- `IdentitiesOnly=yes` with an explicit R2Lab identity when configured
- `-F /dev/null` for deterministic routing independent of operator SSH configuration

Forbidden in SynthRAN-owned physical-path commands and hardened upstream role copies:

- `StrictHostKeyChecking=no`
- `accept-new`
- `UserKnownHostsFile=/dev/null`
- `GlobalKnownHostsFile=/dev/null`

The explicit SLICES known-hosts file is used for sopnode SSH. Faraday-to-UE role copies use `/home/<slice>/.ssh/known_hosts`, matching the R2Lab jump-host topology while preserving host-key verification.

## Source-tree rules

- No second R2Lab resource lifecycle is allowed beside `synthran/r2lab/resources.py`.
- `synthran/r2lab/controller.py` is transport only.
- No runtime monkeypatch is allowed for SSH configuration.
- Upstream role transformations happen only in isolated temporary copies and are fail-closed against source drift.
- Run directories contain evidence and small generated inputs, not duplicated dependency checkouts.
- New cross-domain transport/environment helpers belong under `synthran/utils/` rather than being copied into backend modules.
