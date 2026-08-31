# Backend contract

SynthRAN exposes RFSIM and R2Lab through the same operator lifecycle. This document defines what must remain common and what may legitimately differ.

## Public contract

A backend is selected only through:

```text
synthran run --radio rfsim ...
synthran run --radio r2lab ...
```

Backend-specific lifecycle command groups are not part of the product interface. Readiness, inspection, analysis, and cleanup remain backend-neutral top-level commands. Live progress belongs to `synthran run`; there is no second live-log command.

## Common run semantics

Every accepted run must have:

- one immutable run ID;
- verified provider context;
- exact resource authority;
- verified live 5G session state;
- workload-specific transport evidence when transport is claimed;
- the selected deterministic IoT workload;
- persisted acceptance evidence;
- a sanitized structured run event stream;
- bounded, exact cleanup semantics.

A backend may not declare acceptance merely because deployment returned zero. Acceptance is evidence-based.

Network/session readiness and end-to-end transport proof are distinct claims. A healthy gNB, a live UE PDU session, and a valid UPF route establish network readiness. A transport claim requires traffic or a connection explicitly sourced through the live UE PDU path.

## Allowed backend differences

The following are implementation details and may differ:

| Concern | RFSIM | R2Lab |
| --- | --- | --- |
| Radio | virtual RFSIM | N300/N320 |
| UE | srsUE | selected FR1 Quectel UE |
| Hardware authority | SLICES compute resources | SLICES compute + active R2Lab lease + exact radio/UE claim |
| Registration observation | srsUE/Kubernetes state | modem/runtime state |
| PDU interface | `tun_srsue1` | selected physical data interface, normally `wwan0` |
| gNB deployment | virtual srsRAN path | pinned N3xx Helm values and singleton hardware radio |
| Cleanup | transient workload cleanup; accepted network epoch may be reused | run-owned gNB + exact radio/UE resources |

These differences must stay below the experiment data contract.

## Experiment semantics that must not differ

The following meanings are backend-independent:

- run identity;
- selected IoT source/profile/seed and source parameters;
- telemetry schema and sequence semantics;
- collection-window definition;
- minimum evidence gates;
- artifact hashing and provenance;
- accepted/failed status meaning;
- immutable failure evidence;
- cleanup evidence.

A physical interface name or radio identifier must not appear as a new scientific telemetry field unless the field is genuinely part of the experimental variable being studied.

## Authority rules

Current live control uses fresh provider/runtime observation. The ordering is:

```text
current provider state
> current direct runtime observation
> persisted acceptance evidence
> manifests
> cached information
```

Persisted evidence can justify resuming a run only when the current authority and current target state are reverified.

Unknown, stale, foreign, expired, malformed, or ambiguous ownership fails closed.

## Provider rules

A SLICES project must already exist. The operator must already be authenticated. A run may:

- select the configured project;
- create or reuse the provider experiment associated with the run;
- acquire the Post5G prefix;
- verify the active provider network.

A run must not create projects, bypass authentication, or manufacture authority identifiers.

## Physical resource rules

R2Lab adds these requirements:

- an active lease must be proven before hardware mutation;
- the exact selected radio and UE are bound to the run;
- selected compute-node allocation authority is verified;
- a physical gNB is staged at zero replicas before singleton start;
- only one run-owned physical gNB may be active for the selected radio;
- UE setup/connect/stop mechanics come from the pinned `fiveg_ansible` roles;
- user-plane proof must bind traffic to the selected physical data interface;
- cleanup must prove the selected resources are off/clean before the claim is released.

Global radio/UE cleanup and guessed ownership are prohibited.

## Ansible rules

All long Ansible work must use the shared sanitized streaming implementation. This applies equally to:

- RFSIM resource/network Ansible;
- R2Lab Open5GS Ansible;
- R2Lab UE setup/connect/stop roles.

A PLAY/TASK header is not evidence that work executed. The adapter must suppress tasks that are subsequently skipped. Routine implementation chatter remains in forensic logs; long meaningful operations may produce heartbeats; failures preserve bounded sanitized context. Adding a backend-specific Ansible output parser would violate the contract.

## Run event contract

Every run writes:

```text
.synthran/events/<run-id>.jsonl
```

The event stream is structured evidence produced by the same renderer used for live `[synthran]` progress. It is not a separate operator logging workflow. `--quiet` affects terminal rendering, not persistence.

Backend-specific raw logs may be retained internally when required for diagnosis, but they do not replace or redefine the common event stream.

## Acceptance boundaries

The public lifecycle is:

```text
provider
-> infrastructure
-> network
-> workload
-> acceptance
-> cleanup (when applicable)
```

`network` remains open until the backend-specific gNB/UE/PDU/routing readiness gates pass. Workload setup then proves any stronger transport property required by the experiment.

For R2Lab the evidence record is more granular because hardware safety requires explicit N2, management, acquisition, registration, PDU, and user-plane boundaries. That extra granularity is an implementation safety requirement; it does not create a different public lifecycle.

## Resume behavior

A run may resume when persisted evidence exists, but it must:

1. validate the requested topology against persisted topology;
2. refresh current authority;
3. re-observe any live state needed to authorize the next mutation;
4. continue only from a boundary consistent with current evidence;
5. preserve previous failure evidence instead of silently rewriting history.

Run IDs are never recycled for a different topology or experimental intent.

## Research parity

The deterministic workload is implemented on both backends. Controlled-load research campaigns are a separate scientific capability. The current campaign runtime is accepted on RFSIM; R2Lab campaign parity is not claimed until physical load generation, measurement peer selection, timing validity, and cleanup have current accepted evidence.

This is the required distinction between architectural parity and scientifically demonstrated parity.

## Adding a backend or hardware profile

A new backend or physical profile should not add a new command family. It must instead:

- extend the backend selection/capability model;
- implement the required run boundaries;
- use the common run-event stream;
- use shared Ansible streaming where Ansible is involved;
- produce experiment evidence compatible with the common semantics;
- document unsupported scientific capability explicitly;
- add tests that prove the public command surface did not expand unnecessarily.
