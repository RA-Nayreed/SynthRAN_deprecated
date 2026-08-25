# Current accepted results

This document is the canonical public summary of accepted SynthRAN research evidence. Engineering failures and intermediate testbed observations remain useful in raw run evidence, but capability and scientific claims should point here rather than to dated implementation notes.

## Platform status

The accepted controlled-load research campaign below was executed on the RFSIM reference backend. The R2Lab backend now implements the corresponding physical lifecycle through exact hardware authority, N3xx gNB/N2, selected Quectel UE activation, PDU/user-plane proof, deterministic workload execution, and exact cleanup. Physical controlled-load campaign results are **not** claimed here until a physical campaign satisfies the same measurement-validity standard.

## Accepted campaign

```text
campaign_id:            campaign-20260819-06
network_run_id:         network-acceptance-20260818-09
campaign_seed:          2026081910
Cooja seeds:            424242, 424243, 424244
sensors:                10
sensor period:          5 s
warmup:                 30 s
measurement window:     180 s
RTT probe target:       1 s
UDP flows when loaded:  2
measurement peer:       172.28.2.95
reference capacity:     66,366,402 bps
```

Conditions were randomized within each seed block:

- `baseline`: no background load;
- `load50`: 50% of reference capacity;
- `load80`: 80%;
- `load95`: 95%.

All **12/12 experimental runs** completed and their persisted validity gates reported `ready_for_campaign_analysis=true`.

## Campaign integrity

Across the accepted campaign:

- 2,160 RTT attempts completed with **0 timeouts**;
- all 9 loaded runs sustained their configured aggregate UDP target;
- external iperf3 receiver summaries reported **0 lost UDP packets** in every loaded run;
- all run summaries reported zero UE TX drops and zero UPF RX drops;
- the same run-time UE and PDU epoch was used throughout the campaign;
- every run completed post-window path verification and run-scoped cleanup/base-network reproof;
- all artifact SHA-256 values referenced by the 12 run summaries were independently rechecked against the preserved files.

### Telemetry sequence integrity

The fixed-window summaries contain 4,317 records against a nominal 4,320-event expectation. That difference must **not** be interpreted as three observed packet losses.

The three shorter per-sensor streams are contiguous sequence ranges:

```text
block 2 / load95 / sensor-03: sequence 10..44 (35 records)
block 2 / load95 / sensor-05: sequence 10..44 (35 records)
block 3 / baseline / sensor-09: sequence 8..42  (35 records)
```

Across all 12 runs:

```text
sequence gaps:        0
duplicate sequences:  0
```

A 180-second observation window and a 5-second periodic source have boundary-timing ambiguity: depending on where the sensor cycle falls relative to the exact window edges, 35 or 36 records may lie inside the window even when the observed sequence is continuous.

Therefore the current `delivery_ratio` field in the v1alpha1 summary should be read as **nominal window occupancy**, not as an end-to-end packet-loss estimator. Scientific reporting for this campaign uses sequence gaps/duplicates for observed telemetry integrity.

## Controlled load

The calibrated external UE-path reference was:

```text
66,366,402 bps
```

Mean measured background goodput was:

| Condition | Mean goodput | Target fraction |
|---|---:|---:|
| load50 | 33.183 Mbps | 0.50 |
| load80 | 53.093 Mbps | 0.80 |
| load95 | 63.048 Mbps | 0.95 |

The load generator therefore held the intended treatment throughout every accepted loaded measurement.

## RTT result

Condition-level RTT summaries from the persisted campaign analysis are:

| Condition | Median RTT | Mean run-median RTT | Mean run-p95 RTT |
|---|---:|---:|---:|
| baseline | 27.65 ms | 27.93 ms | 37.77 ms |
| load50 | 17.15 ms | 16.93 ms | 22.67 ms |
| load80 | 16.20 ms | 16.23 ms | 19.47 ms |
| load95 | 16.40 ms | 16.33 ms | 18.30 ms |

Paired median differences versus the blocked baseline were:

| Treatment | Median difference | Bootstrap interval from 3 blocks |
|---|---:|---:|
| load50 | -10.50 ms | [-12.95, -9.55] ms |
| load80 | -11.15 ms | [-13.15, -10.80] ms |
| load95 | -11.55 ms | [-12.95, -10.30] ms |

The raw probe traces show that this separation persists through the 180-second measurement windows rather than appearing only during startup.

### Interpretation boundary

This is an **exploratory observation**, not yet a causal conclusion that network load improves latency.

The three loaded conditions all sit near 16–17 ms despite very different offered rates. That shape is more consistent with an **idle-versus-continuously-active path effect** than with a monotonic congestion effect. Possible mechanisms include scheduling, power/idle state, queue activity, or another implementation-specific active-path behavior.

There are only three independent seed blocks. The bootstrap intervals describe the observed three-block experiment; they should not be presented as publication-grade population inference without replication.

The targeted follow-up should therefore separate *path activity* from *load magnitude*, for example with:

```text
baseline / idle
very-low continuous traffic
load10
load50
load95
```

with more independent repetitions.

## Network-counter sampling limitation

Campaign-06 requested a 1-second network-counter interval, but the raw `network-samples.jsonl` files contain 61 samples per 180-second run. Effective spacing is approximately 3 seconds because one sample previously performed the ingress, UE, and UPF remote queries sequentially and required roughly 2.6 seconds.

This does **not** invalidate the run-level counter deltas, throughput totals, path completeness, or drop-counter observations. It does mean campaign-06 must not be described as having 1 Hz network-counter resolution.

The sampler now collects independent read-only counter queries concurrently and future runs fail closed when achieved cadence falls materially below the requested cadence.

RTT probing is separate from this limitation: every campaign-06 run contains 180 RTT attempts over its 180-second window.

## Preserved evidence

The raw preservation object is stored in the SLICES project object store:

```text
s3://ilabt.imec.be-project-post5g-beta/
  synthran/campaigns/2026-08-19/campaign-20260819-06/
  campaign-20260819-06.tar.gz
```

Archive SHA-256:

```text
bf23f8c5623ecb3566fa0686faa3b86611266a0d819692eb5995919a5a893bba
```

The upload was verified byte-for-byte, versioned, and reported replication complete by the object store.

Derived campaign analysis:

```text
s3://ilabt.imec.be-project-post5g-beta/
  synthran/analysis/2026-08-19/campaign-20260819-06/
  campaign-20260819-06-analysis.json
```

Analysis SHA-256:

```text
c96a3c402088420400c8727606376c86b08b0f6658a32220466be866d89aafa3
```

The unrounded campaign analysis JSON is intentionally tracked under [`../results/`](../results/) for direct inspection. The complete immutable raw run bundle remains in SLICES object storage, where its archive checksum was verified byte-for-byte.

### Preservation-manifest note

The frozen raw archive is valid at the archive level, but its internal `SHA256SUMS` file accidentally includes a checksum entry for itself. The self-entry therefore cannot verify after the manifest was written. All other manifest entries checked successfully, and the S3 archive-level SHA-256 matched the local archive byte-for-byte.

The frozen object is intentionally left unchanged. Future preservation manifests must exclude their own checksum file.

## What campaign-06 establishes

The evidence supports the following bounded statement:

> In the accepted SynthRAN virtual configuration, deterministic ten-sensor IoT telemetry remained sequence-continuous through the srsUE/srsRAN/Open5GS user path while controlled external UDP background traffic was sustained up to 95% of the calibrated reference capacity. The same campaign exposed a reproducible reduction in measured RTT whenever sustained background traffic was active; the mechanism remains an open research question requiring targeted replication.

It does **not** establish physical-RF behavior, multi-UE scaling, a general causal latency effect, or performance under arbitrary RAN/core configurations.

## Next scientific work

1. Verify the hardened network-counter sampler with a fresh short run before another long campaign.
2. Treat fixed-count telemetry coverage as window occupancy and use sequence continuity for observed loss/integrity claims.
3. Produce publication figures directly from the raw campaign-06 traces: RTT distributions/time series, paired block effects, telemetry inter-arrival behavior, and UE/UPF transport agreement.
4. Run a targeted active-versus-idle replication with more independent blocks.
5. Add physical controlled-load research only after the R2Lab measurement peer, load generation, timing validity, and cleanup contract are accepted end-to-end.
