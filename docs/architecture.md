# Architecture

SynthRAN is an experiment-control and evidence layer around external provider, radio, 5G, IoT, and measurement systems. The public interface is backend-neutral; hardware-specific mechanics stay below the run boundary.

## System boundary

```text
                         synthran
                            |
          +-----------------+-----------------+
          |                                   |
    --radio rfsim                       --radio r2lab
          |                                   |
 SLICES compute resources              active R2Lab lease
 Open5GS + srsRAN/RFSIM                 exact N3xx + UE claim
 srsUE packet endpoint                  Open5GS + N3xx gNB
          |                              physical UE/PDU
          +-----------------+-----------------+
                            |
                 deterministic IoT workload
                            |
                    accepted evidence
                            |
                 JSONL + Parquet + logs
```

There is one installed executable and one lifecycle command: `synthran run`. RFSIM and R2Lab are implementations selected by `--radio`; they are not independent applications.

## Public command boundary

The supported top-level commands are:

```text
run
 doctor
inspect
logs
stop
research
deps
dev
```

Only `run` performs the complete experiment lifecycle. `doctor` is read-only, `inspect` reads capabilities/evidence, `logs` reads the persisted event stream, and `stop` releases exact resources owned by a run. Research and repository-maintenance commands are separate because they operate on accepted evidence or source state rather than constructing another network lifecycle.

Backend-specific resource preparation, Open5GS reconciliation, gNB staging, UE activation, PDU proof, and cleanup functions are Python implementation boundaries. They are deliberately not separate public command groups.

## Run orchestration

A run executes the following logical sequence:

```text
provider context
-> resource authority
-> 5G foundation
-> radio/gNB path
-> UE/PDU/user-plane proof
-> deterministic workload
-> acceptance
-> exact cleanup
```

The concrete work differs by backend.

### RFSIM

The virtual implementation composes the established SLICES path:

```text
provider experiment
-> reservation/allocation
-> live preflight
-> Open5GS + srsRAN/RFSIM deployment
-> srsUE/PDU path proof
-> deterministic IoT workload
-> experiment evidence
```

The existing deployment and experiment functions remain reusable internal services. Their historical CLI wrappers were removed when `synthran run` became the sole lifecycle entry.

### R2Lab

The physical implementation composes:

```text
provider experiment
-> active R2Lab lease
-> exact N3xx + UE claim
-> selected compute-node authority
-> Kubernetes/Open5GS foundation
-> stopped pinned N3xx gNB render
-> singleton gNB + stable N2
-> selected UE activation
-> registration + PDU
-> route-bound user-plane proof
-> deterministic IoT workload
-> exact physical cleanup
```

Every physical mutation refreshes current authority. Earlier accepted evidence never substitutes for a current lease, allocation, resource state, gNB pod, registration state, route, or PDU observation.

## Provider context

A SLICES project must already exist and the operator must already be authenticated. A run may select the configured project, create or reuse the provider experiment associated with its run ID, acquire the Post5G prefix, and verify the resulting network context.

Provider experiment creation is therefore part of the unified run, while project creation and authentication remain outside SynthRAN.

## Ansible execution

SynthRAN relies on pinned upstream Ansible content rather than duplicating provider mechanics.

All long Ansible execution uses:

```text
synthran.ansible_streaming.run_streaming_ansible_command
```

The shared streamer:

- captures stdout/stderr without a shell;
- emits only reviewed, operator-useful task names;
- keeps failures visible even when routine task chatter is suppressed;
- emits heartbeats for long tasks;
- returns complete command output for sanitized run logs and evidence.

It is used by virtual deployment, physical Open5GS reconciliation, and physical UE role execution. New Ansible-driven paths must use the same implementation rather than adding another subprocess wrapper.

## Unified progress and logs

`synthran run` writes a single sanitized event stream:

```text
.synthran/events/<run-id>.jsonl
```

The same messages are mirrored to the terminal unless `--quiet` is used. `synthran logs` reads this event stream, so live output and later diagnostics share one contract.

The event stream contains high-level run transitions and sanitized child-operation output. Raw provider output is not a second public log API.

## Evidence model

Runtime evidence has two jobs:

1. prove a boundary was satisfied at a particular time;
2. provide enough provenance to reproduce or audit the run.

Evidence does not authorize later mutation. Current provider and runtime observation remain authoritative for live control.

Typical persisted records include:

```text
provider/resource manifests
network evidence
physical-run.json
experiment-evidence.json
research-summary.json
telemetry.jsonl / telemetry.parquet
probe.jsonl / probe.parquet
network-samples.jsonl / network-samples.parquet
load.jsonl / load.parquet
.synthran/events/<run-id>.jsonl
```

## Deterministic IoT path

The scientific workload is backend-independent at the experiment level:

```text
10 deterministic Contiki-NG/Cooja sensors
-> RPL/6LoWPAN border router
-> counted ingress
-> UE-side MQTT handoff
-> 5G user plane
-> Open5GS UPF
-> run-owned central MQTT collector
-> canonical JSONL
-> deterministic Parquet
```

RFSIM uses srsUE and a virtual radio. R2Lab uses the selected physical Quectel UE and N3xx radio. Interface names, modem commands, radio addresses, and provider identifiers are implementation details and must not change scientific telemetry semantics.

## Research boundary

Controlled measurement tools operate on an accepted base path. The current controlled-load campaign implementation is validated on the virtual network-evidence representation. Physical runs already execute the deterministic workload through the physical user plane, but controlled-load campaign parity is not claimed until physical measurement and load control have accepted evidence.

This distinction is intentional: a common public lifecycle does not justify claiming common scientific capability before it is measured.

## Source layout

The principal runtime boundaries are:

```text
synthran/cli.py                 public parser entry
synthran/operator.py            public command definitions and dispatch
synthran/provider.py            shared SLICES provider context
synthran/backends/run.py        backend-selecting run orchestration
synthran/ansible_streaming.py   shared Ansible progress
synthran/network/               virtual compute/network implementation
synthran/r2lab/                 physical authority/radio/UE implementation
synthran/experiment/            deterministic workload runtime
synthran/research/              controlled measurements and analysis
synthran/privacy.py             repository/privacy controls
```

`command_runtime.py` is internal support for existing virtual-path and research functions. It does not define a public parser or command dispatch tree.

## Design rules

- one installed executable;
- one public lifecycle command;
- backend selection through `--radio`;
- shared progress/log semantics;
- exact ownership-bound mutation and cleanup;
- provider/direct observation outranks historical evidence for live authority;
- backend-specific mechanics do not leak into the scientific data contract;
- unsupported capability is reported as unsupported rather than inferred from a neighboring successful boundary.
