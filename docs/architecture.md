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
 srsUE PDU endpoint                     Open5GS + N3xx gNB
          |                              physical UE/PDU
          +-----------------+-----------------+
                            |
                 selected IoT workload
                            |
                    accepted evidence
                            |
                   JSONL + Parquet
```

There is one installed executable and one lifecycle command: `synthran run`. RFSIM and R2Lab are implementations selected by `--radio`; they are not independent applications.

## Public command boundary

The supported top-level commands are:

```text
run
doctor
calibrate
inspect
analyze
release
deps
dev
```

Only `run` performs an experiment lifecycle. `doctor` is read-only, `inspect` reads capabilities/evidence, `calibrate` measures accepted RAN/UE-path capacity, `analyze` consumes persisted research evidence, and `release` performs exact persistent-resource cleanup where supported.

There is no second public live-log command. Runtime progress belongs to `run`.

Backend-specific resource preparation, Open5GS reconciliation, gNB staging, UE activation, PDU verification, and cleanup functions are Python implementation boundaries. They are deliberately not separate public command groups.

## Public run lifecycle

A run is rendered through the same logical lifecycle on both backends:

```text
provider
-> infrastructure
-> network
-> workload
-> acceptance
-> cleanup (when applicable)
```

Backend-specific sub-boundaries remain visible only when they provide useful operator state.

### RFSIM

The virtual implementation composes:

```text
provider experiment
-> reservation/allocation
-> node/Kubernetes/tool preparation
-> Open5GS + srsRAN/RFSIM deployment
-> live 5G session readiness
   (gNB + srsUE + PDU + UPF route)
-> selected IoT workload
-> workload transport proof through the live PDU endpoint
-> experiment evidence
```

Network/session readiness is intentionally distinct from end-to-end workload transport. The readiness gate proves the live components and routing state. The workload transport gate uses a connection or traffic source explicitly bound to the live UE PDU address.

### R2Lab

The physical implementation composes:

```text
provider experiment
-> active R2Lab lease
-> exact N3xx + UE claim
-> selected compute-node/Open5GS foundation
-> stopped pinned N3xx gNB render
-> singleton gNB + stable N2
-> selected UE activation
-> registration + PDU state
-> route-bound user-plane verification
-> selected IoT workload
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

The shared adapter:

- captures complete subprocess output for forensic evidence;
- treats PLAY/TASK headers as implementation metadata, not execution proof;
- suppresses a task if Ansible subsequently reports it skipped;
- promotes only reviewed operator-useful work;
- emits heartbeats for long meaningful tasks;
- keeps failures visible with bounded sanitized task/host/state/reason context.

It is used by virtual deployment, physical Open5GS work, and physical UE role execution. New Ansible-driven paths must use the same implementation rather than adding another progress parser.

## Unified runtime events

Lifecycle state and child-runtime progress converge on one canonical event pipeline:

```text
                    RunEventStream
                          ^
          +---------------+---------------+
          |               |               |
      lifecycle        Ansible          AMBER /
       stages           adapter          research
```

The terminal renderer emits:

```text
[synthran] → network
[synthran]   … Open5GS locked images · 2m
[synthran]   ✓ PDU session · 12.1.0.x
[synthran] ✓ network: READY
```

The same semantic events are persisted as structured JSONL:

```text
.synthran/events/<run-id>.jsonl
```

The event file is evidence, not another public operator workflow. Detailed preparation/deployment/component logs remain forensic artifacts.

Internal static/live readiness validators may still guard execution boundaries. During `run`, their full PASS tables are collapsed into lifecycle prerequisite state. The standalone `doctor` command exists for direct readiness diagnosis.

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
research-summary-v2.json
telemetry.jsonl / telemetry.parquet
probe.jsonl / probe.parquet
network-samples.jsonl / network-samples.parquet
load.jsonl / load.parquet
.synthran/events/<run-id>.jsonl
```

## Ambient-IoT data path

For the current `ambient-v1` scientific profile:

```text
AMBER Ambient-IoT source model
-> energy/controller/access/backscatter simulation
-> decoded Ambient-IoT events
-> counted MQTT ingress
-> UE-side edge transport
-> live 5G user plane
-> Open5GS UPF
-> central collector
-> canonical JSONL / deterministic Parquet
```

AMBER models the Ambient-IoT source/backscatter side. Decoded events are transported through the 5G test path. AMBER tags are not represented as 5G NR UEs, and SynthRAN does not claim that AMBER injects an Ambient-IoT RF waveform into srsRAN.

RFSIM uses srsUE and a virtual radio. R2Lab substitutes the selected physical Quectel UE and N3xx radio where the selected profile is supported. Interface names, modem commands, radio addresses, and provider identifiers are implementation details and must not change scientific telemetry semantics.

## Research boundary

Controlled measurement tools operate on an accepted base network. The current controlled-load campaign implementation is validated on the virtual network-evidence representation. Physical runs already execute deterministic workloads through the physical user plane, but controlled-load campaign parity is not claimed until physical measurement and load control have accepted evidence.

A common public lifecycle does not justify claiming common scientific capability before it is measured.

## Source layout

The principal runtime boundaries are:

```text
synthran/cli.py                     public parser entry
synthran/operator.py                public command definitions and dispatch
synthran/provider.py                shared SLICES provider context
synthran/backends/run.py            backend execution implementation
synthran/backends/unified_run.py    canonical run adapter
synthran/run_events.py              lifecycle + child event renderer/evidence
synthran/ansible_streaming.py       Ansible event adapter
synthran/network/                   virtual compute/network implementation
synthran/r2lab/                     physical authority/radio/UE implementation
synthran/experiment/                workload runtime
synthran/research/                  controlled measurements and analysis
synthran/privacy.py                 repository/privacy controls
```

`command_runtime.py` is internal support for established virtual-path and research functions. It does not define a public parser or command dispatch tree.

## Design rules

- one installed executable;
- one public lifecycle command;
- backend selection through `--radio`;
- one canonical live runtime event stream;
- Ansible is an event producer, not a second logger;
- exact ownership-bound mutation and cleanup;
- provider/direct observation outranks historical evidence for live authority;
- network readiness is not overstated as end-to-end transport proof;
- backend-specific mechanics do not leak into the scientific data contract;
- unsupported capability is reported as unsupported rather than inferred from a neighboring successful boundary.
