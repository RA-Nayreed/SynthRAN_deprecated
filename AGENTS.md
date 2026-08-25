# SynthRAN Repository Instructions

## Purpose

SynthRAN is a reproducible experiment-control platform joining deterministic IoT emulation, open 5G networking, physical testbed integration, and research-grade evidence.

The accepted virtual golden path is:

```text
10 deterministic Contiki-NG/Cooja sensors
-> RPL/6LoWPAN border router
-> Cooja Serial Socket
-> loopback-only reverse SSH tunnel
-> remote tunslip6/tun0
-> counted TCP ingress
-> Mosquitto bridge inside srsUE network namespace
-> tun_srsue1
-> srsRAN gNB
-> Open5GS UPF
-> run-owned central Mosquitto
-> canonical JSONL
-> deterministic Parquet
```

SynthRAN owns orchestration, contracts, integration adapters, validation, evidence, cleanup, and reproducibility reporting. It does not reimplement Open5GS, srsRAN, Contiki-NG, Cooja, Mosquitto, iperf3, SLICES, or R2Lab provider services.

For current live evidence, use [`docs/results.md`](docs/results.md). Do not duplicate a competing list of latest run IDs across documentation.

## Integration truth

Before substantial work, inspect the current target integration branch, code, tests, and documentation rather than relying on an older PR, chat transcript, or historical result note.

Development history is not product architecture. Public commands, schemas, filenames, and statuses must describe durable concepts rather than temporary implementation milestones.

There is one supported product executable:

```text
synthran
```

There is no interactive frontend or external workbench protocol. Explicit CLI arguments are the public operator surface. Internal modules may expose Python APIs for composition and testing, but they are not separate products.

## Backend invariant

RFSIM remains the accepted virtual reference backend. R2Lab implements the corresponding physical path. Physical integration must satisfy the same experiment-level semantics rather than becoming a separate product workflow.

Backend-specific mechanics may differ below the network and user-plane boundary, including radio selection, UE implementation, registration observation, PDU interface, and physical authority. Above that boundary, experiment identity, deterministic workload inputs, telemetry meaning, research validity, evidence, provenance, and cleanup semantics remain common.

Never weaken an accepted RFSIM invariant merely to make the physical path fit. Never describe an R2Lab stage as accepted without current physical evidence for that stage.

## State and reconciliation invariants

Requested intent and discovered facts remain separate:

- `ExperimentDesiredState`: declared intent and stable constraints;
- observed state: discovered provider or runtime facts.

PDU addresses, pod names, reservation/allocation identifiers, current lease state, registration state, routes, and similar dynamic values are observations, never desired state.

Truth ranking is:

```text
provider
> direct observation
> persisted evidence
> manifest
> cache
```

Only fresh provider or direct observations may authorize current provider mutation. Historical evidence proves what happened; it does not become current authority.

Unknown, stale, foreign, expired, failed, or ambiguous ownership fails closed.

## Operation control

Controlled operations bind current desired state, observed state, exact targets, ownership, and relevant input digests.

Risk classes are:

```text
R0  local/read-only
R1  live/read-only verification or evidence access
R2  controlled mutation requiring approval or explicit invocation
R3  destructive mutation requiring explicit destructive authority
```

Only one mutation may hold a workspace mutation claim. If a mutation fails or is interrupted and clean rollback cannot be proven, preserve recovery state rather than guessing success.

Structured evidence and events are trusted progress surfaces. Never infer accepted operation state by parsing arbitrary provider prose alone.

## Resource and provider safety

Resource selection must be deterministic and based on reviewed descriptors plus fresh complete provider inventory.

Generic rollback authority comes only from exact resources proven to have been created or owned by the current operation. Roll back in reverse acquisition order when required. Never guess provider ownership from naming conventions.

Never use broad cleanup such as `pkill`, `killall`, wildcard resource deletion, global radio power-off, or guessed reservation/allocation IDs when an exact run-owned target is required.

Provider experiment creation remains an explicit operator action. SynthRAN may bind to an existing SLICES experiment but does not silently log in, switch projects, create projects, create provider experiments, or allocate a Post5G prefix on the operator's behalf.

## Live-accepted research boundary

The accepted virtual configuration is deliberately narrow:

- core: Open5GS;
- RAN: srsRAN;
- radio: RFSIM;
- UE: one srsUE acting as the IoT edge gateway;
- one SST-1 slice with DNN `internet`;
- exactly ten deterministic Cooja sensors;
- UDP for controlled research load;
- JSONL as append-only audit data and deterministic Parquet as its derivative.

Current accepted research evidence is summarized in [`docs/results.md`](docs/results.md). Physical RF, multiple UEs or slices, formal A1/E2/RIC integration, generative models, synthetic telemetry, and automated RAN-policy synthesis remain unproven unless accepted evidence explicitly changes that status.

## Research measurement peer invariant

Capacity calibration and controlled background load must terminate outside the 5G core host.

For the supported virtual inventory:

```text
UE PDU
-> tun_srsue1
-> 5G user plane
-> Open5GS UPF
-> core egress / NAT
-> prepared external measurement peer
```

The core node is rejected as the iperf3 measurement server when a same-host path can collapse into a Kubernetes or hairpin route.

Run-owned iperf lifecycle must preserve server ownership, exact target assignment, UE PDU binding, exact routing, live control-connection readiness, load evidence, and cleanup.

See [`docs/research-measurement-peer.md`](docs/research-measurement-peer.md).

## Research data semantics

Do not confuse observation-window occupancy with packet loss.

A periodic sensor can legitimately contribute one fewer record when exact measurement-window boundaries fall between transmissions. Existing `delivery_ratio` values are nominal window-coverage metrics; observed sequence gaps and duplicates are the primary continuity evidence.

Network sampling has a separate timing contract. A requested interval is not proof that the sampler achieved that cadence. Persisted sample duration and schedule lag are measurement evidence.

## Experiment validity

Keep these concepts distinct:

- base-network path acceptance;
- integrated IoT-path acceptance;
- external-peer calibration validity;
- current measurement-path validity;
- load-target validity;
- instrumentation validity;
- cleanup and base-network reproof;
- scientific interpretation.

Zero telemetry is not automatically a network result. A telemetry sequence loss may be a legitimate scientific outcome when independent path, load, and instrumentation validity remains healthy. Do not encode the desired scientific result into infrastructure validity gates.

Failed and invalid runs remain immutable diagnostic evidence and must never be silently reclassified or reused under the same run ID.

## Reproducibility and preservation

Pinned upstream checkouts live below ignored `.deps/` storage. Do not vendor or partially copy upstream projects for convenience. Keep selected runtime images digest-pinned and preserve third-party license and provenance records.

Research artifacts should preserve immutable run specification, measurement window, telemetry, RTT probes, network counters, load records, validity summary, and artifact digests. Complete raw campaign bundles belong in durable research or object storage.

Checksum manifests must never include an entry for the manifest file itself.

## Credentials and privacy

Never commit subscriber credentials, SLICES tokens, S3 secrets, private SSH keys, kubeconfigs, private authority or environment files, unsanitized secret-bearing captures or logs, generated live run directories, or dependency worktrees.

Privacy protections are layered through ignore rules, repository scanning, pre-push checks, CI, and GitHub controls. Do not weaken a privacy rule merely to make a check pass; correct false positives narrowly while preserving detection of the sensitive type.

Prefer route proof, counters, broker receipt, and message-integrity evidence over packet capture when they prove the required boundary with lower privacy risk.

## Documentation discipline

Public documentation has distinct jobs:

- `README.md`: new-reader overview and compact runnable path;
- `docs/results.md`: canonical current live evidence and scientific interpretation boundary;
- `docs/experiment.md`: experiment and research protocol;
- `docs/architecture.md`: durable system boundaries;
- `docs/operator-guide.md`: full live procedure, provider prerequisites, recovery, and preservation;
- historical result files: immutable engineering history, not current capability truth.

Keep public commands consistent with the installed `synthran` executable. Current and historical evidence must not be mixed.

## Validation before completion

From the repository root in the `synthran` environment, run applicable checks:

```bash
python -m unittest discover -s tests -v
synthran privacy scan --worktree
git diff --check
git status --short
```

When history secret scanning is available, run it as well. Inspect the complete intended diff before merging and confirm that documentation matches current code, no private evidence was added, planning is not presented as execution, accepted and unproven capabilities remain distinct, and new mutation or cleanup behavior is exact, ownership-bound, and fail-closed.
