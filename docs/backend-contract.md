# Platform experiment contract

SynthRAN does not implement independent deployment backends. `rfsim` and `r2lab` are platform choices passed to 5g-Ansible. 5g-Ansible owns platform-specific infrastructure and exposes the resulting deployment through its manifest and generated inventory.

## Public contract

```text
synthran run --radio rfsim ...
synthran run --radio r2lab ...
```

The common SynthRAN contract begins **after** upstream deployment:

1. consume the exact upstream manifest and inventory;
2. observe the required live path;
3. execute the selected workload/measurement;
4. persist scientific and transport evidence;
5. remove only experiment-owned state;
6. re-observe the upstream deployment.

A successful `fiveg up` is deployment evidence, not experiment acceptance. SynthRAN acceptance remains evidence-based.

## Deployment authority

The sole deployment boundary is the pinned 5g-Ansible machine API. SynthRAN must not independently:

- reserve/release provider or R2Lab resources;
- allocate or prepare POS nodes;
- install Kubernetes or 5G software;
- render core/RAN/RU/UE deployment configuration;
- patch upstream 5G Deployments to make a workload work;
- start/restart/repair a gNB or UE runtime;
- maintain a second support matrix.

Topology validation belongs upstream. If 5g-Ansible accepts a native `fiveg/deployment/v1` request and returns a ready deployment, SynthRAN may consume it.

## Common experiment semantics

Every accepted workload run must have:

- one immutable run ID;
- exact upstream deployment provenance;
- fresh live path observation;
- immutable IoT source/profile/seed parameters;
- deterministic telemetry semantics;
- workload-specific transport proof;
- persisted JSONL/Parquet/evidence artifacts;
- exact experiment-local cleanup;
- post-cleanup upstream-path reproof.

Network readiness and workload transport are separate claims. SynthRAN never upgrades one into the other implicitly.

## Platform-specific observation

| Concern | RFSIM | Physical R2Lab |
| --- | --- | --- |
| Deployment owner | 5g-Ansible | 5g-Ansible |
| UE facts | generated inventory + live UE pod | generated inventory physical UE entry |
| Experiment interface | `tun_srsue1` | `wwan0` for current modem experiment |
| Transport proof | PDU-bound transient UE relay + counters | route through physical UE + counters |
| Experiment mutation | run-owned processes, optional exact `/32` route, central MQTT | run-owned forwards/ingress, central MQTT |
| Infrastructure repair | forbidden | forbidden |

These are experiment acceptance differences, not separate deployment frameworks.

## RFSIM rules

The current Amber RFSIM experiment requires the persisted and live PDU identity to agree. The experiment may add an exact `/32` route only when needed and only with `ip route add`; it must never replace an existing route. It launches a transient relay inside the existing UE container and removes it after the run. No Deployment rollout is permitted.

## Physical rules

Physical UE identity and SSH parameters come only from the generated upstream inventory. The current physical experiment proves the destination route through `wwan0`, sends the Amber stream through that UE, and requires the physical interface TX counter to increase. It does not configure the modem, radio, gNB, lease, or allocation.

## Authority and resume

For an experiment, current live observation outranks persisted SynthRAN evidence. Persisted evidence establishes provenance but never authorizes a new infrastructure mutation.

A resumed experiment must verify that the supplied upstream deployment identity and current observed path still match the persisted scientific intent. Unknown or ambiguous path state fails closed.

## Research measurements

Controlled RFSIM research measurements may create bounded measurement state such as an exact target route and a run-owned iperf3 server. They must clean that state and re-prove the same UE/PDU identity. Calibration follows the same contract.

Physical controlled-load campaign parity is not claimed merely because physical Amber delivery works. It requires separate accepted measurement evidence.

## Adding another platform or topology

A new 5G topology belongs in 5g-Ansible. SynthRAN should need changes only when a new experiment requires different observation, transport, measurement, or acceptance semantics. It must not gain another deployment command family or infrastructure support matrix.
