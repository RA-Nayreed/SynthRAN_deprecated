# Experiment protocol

SynthRAN’s scientific unit is an immutable run with deterministic workload inputs, a proven 5G data path, fixed measurement boundaries, persisted validity evidence, and exact cleanup semantics.

## Deterministic workload

The canonical workload contains ten Contiki-NG/Cooja sensors. Each sensor has a stable identity and deterministic timing derived from the selected seed.

At the experiment level the path is:

```text
10 deterministic sensors
-> RPL/6LoWPAN border router
-> counted ingress
-> UE-side MQTT handoff
-> 5G user plane
-> Open5GS UPF
-> run-owned central MQTT collector
-> canonical JSONL
-> deterministic Parquet
```

RFSIM and R2Lab differ in how the UE and radio are implemented, not in the meaning of the emitted sensor records.

## Run identity

A run ID is immutable and unique. It binds:

- provider context;
- selected compute/radio resources;
- workload identity;
- evidence directories;
- sanitized event stream;
- acceptance outcome.

A failed run ID is never reused for a retry with different state or intent.

## Acceptance

An accepted run must prove the required path rather than infer success from process exit codes. The common logical order is:

```text
provider
-> resources
-> 5G path
-> UE/PDU/user plane
-> workload
-> acceptance
-> cleanup
```

R2Lab records additional hardware-specific boundaries such as N2, UE management, cell acquisition, registration, and PDU state. Those checks strengthen physical safety but do not change the scientific workload definition.

## Telemetry record semantics

Canonical telemetry records preserve at least:

- run identity;
- sensor identity;
- sequence identity;
- source timestamp/order information required by the collector;
- payload fields defined by the deterministic sensor program;
- collection metadata required for provenance.

JSONL is the append-only audit representation. Deterministic Parquet files are analysis derivatives and must be reproducible from the accepted JSONL source.

## Sequence integrity

Observation-window occupancy and packet loss are different concepts.

A periodic source may contribute one fewer record when the fixed window starts or ends between transmissions. Therefore:

- nominal expected count is a window-occupancy reference;
- a missing sequence inside an observed contiguous range is an observed sequence gap;
- a repeated sequence identity is a duplicate;
- fixed-window count alone is not an end-to-end loss estimator.

Scientific reporting should state sequence gaps and duplicates explicitly.

## Measurement window

Research runs separate startup/warmup from the fixed measurement interval.

A measurement specification records:

- warmup duration;
- measurement duration;
- requested telemetry/sample cadence;
- RTT probe cadence;
- load configuration;
- external measurement peer;
- deterministic seed and condition.

Requested cadence is not evidence of achieved cadence. Instrumentation validity is determined from persisted timestamps, durations, and scheduling lag.

## RTT probes

RTT probing is collected independently from slower network-counter sampling. Each probe attempt belongs to the fixed measurement interval and records success/failure plus timing information required for analysis.

A probe timeout is a measured outcome when the measurement path and instrumentation remain valid; it is not automatically an infrastructure failure.

## Network counters

Network counters provide path-health and transport evidence such as ingress, UE-side transfer, UPF transfer, and drop counters. They are sampled read-only.

The sampler must record enough timing information to determine whether the requested cadence was actually achieved. A run that materially misses its instrumentation contract may remain useful diagnostic evidence but must fail the corresponding research-validity gate.

## Controlled load

Controlled research load uses an external peer outside the 5G core host. The supported virtual research path is:

```text
UE PDU
-> 5G user plane
-> Open5GS UPF
-> core egress
-> prepared external measurement peer
```

Load conditions may use an absolute target bitrate or a fraction of a freshly calibrated reference capacity. The run records configured target, achieved transfer evidence, and cleanup.

The core node must not be used as the measurement server when that choice can collapse into a same-host or Kubernetes hairpin path.

## Calibration

Capacity calibration belongs to the current accepted network epoch. It is not a universal property of the testbed.

Calibration evidence records the target peer, duration, transport configuration, resulting reference bitrate, and enough path identity to prevent accidental reuse on a different network state.

## Campaigns

A campaign is a deterministic schedule of conditions and seeds. The schedule is persisted before execution. Typical blocked conditions are:

```text
baseline
load50=0.5
load80=0.8
load95=0.95
```

Each condition is paired with the baseline from the same seed block during analysis. Failed or invalid runs remain in the evidence set but are excluded from scientific summaries according to explicit validity gates.

## Validity layers

Keep these questions separate:

1. Was the base 5G path accepted?
2. Did the deterministic IoT path remain valid?
3. Was the external measurement peer valid?
4. Was the requested load actually sustained?
5. Did RTT/network instrumentation meet its contract?
6. Did cleanup and post-run path verification succeed?
7. What scientific outcome was observed?

The expected scientific result must never be encoded into infrastructure validity. For example, higher latency, lower latency, packet loss, or zero packet loss can all be legitimate outcomes if the path and instrumentation are independently valid.

## Physical backend boundary

The deterministic workload is implemented through the R2Lab physical user plane. Physical controlled-load research campaigns are not yet claimed as accepted scientific capability because the physical measurement-peer/load-control contract has not been validated to the same standard as the current virtual campaign.

Do not reinterpret architectural run parity as measured research parity.

## Preservation

A complete research bundle should preserve:

```text
run/campaign specification
measurement window
measurement path
telemetry.jsonl + telemetry.parquet
probe.jsonl + probe.parquet
network-samples.jsonl + network-samples.parquet
load.jsonl + load.parquet
research summary
artifact digests
provider/dependency provenance
unified event log
```

Raw immutable campaign bundles belong in durable research/object storage rather than ordinary Git history.

## Current accepted results

The canonical current measurements, checksums, limitations, and interpretation boundaries are maintained in [`results.md`](results.md).
