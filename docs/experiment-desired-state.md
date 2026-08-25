# Experiment desired-state contract

SynthRAN keeps requested experiment configuration separate from provider-assigned runtime observations.

## Persistence

A persistent experiment can contain:

```text
.synthran/experiments/sran-YYYYMMDD-NNN/
├── experiment.toml
├── desired.json
├── observed.json
├── status.json
└── runs/
```

`experiment.toml` is durable issued identity and binding metadata. `desired.json` is the validated detailed request. `observed.json` and `status.json`, when present, are observation or compatibility caches and never become desired state or permanent mutation authority.

Runtime-assigned addresses, pods, allocations, reservations, leases, resource IDs, registration state, and interfaces remain observations.

## Implementation choices

The desired-state model can express implementation choices beyond the currently accepted paths. Schema validity is not live acceptance.

The accepted virtual reference path is Open5GS + srsRAN + one srsUE + RFSIM. Physical R2Lab support is accepted only through the stages established by current physical evidence.

## Addressing and topology

Static requested values belong in desired state only when the schema explicitly models them as operator intent. Provider- or runtime-assigned values remain observed state.

RAN topology, UE configuration, PLMN, DNN, slice, QoS, placement, optional Multus, and optional RIC-related fields are validated as requested constraints. Model support for an option does not imply that its complete executor is live accepted.

## Radio capability

Radio intent separates mode, backend, and optional hardware selection. Contradictory combinations are rejected. A valid `physical`/`r2lab` desired state is not evidence that the hardware path has completed acceptance.

## Placement

Automatic placement is resolved from reviewed resource requirements and fresh provider inventory. Manual pins are hard constraints, not permission: they still require capability, current availability, ownership, provider authority, and operation policy.

Allocation results never rewrite desired placement silently.

## Issuance

Experiment IDs use `sran-YYYYMMDD-NNN` and are non-reusable. Identity is issued before detailed desired-state persistence so an interrupted or failed issuance cannot cause ID reuse.

Replacing existing desired state requires an explicit replacement path. Silent overwrite is not allowed.

## Reconciliation boundary

Desired state answers what the researcher requested. Observed state answers what is currently known to exist. Reconciliation and workflow policy compare those models and may produce immutable operation plans. Live mutation still requires current authority, ownership, freshness, policy, and executor-specific checks.

## Product boundary

Desired-state helpers are internal domain APIs. The supported user interface remains the installed `synthran` CLI; no separate interactive experiment-setup interface is part of the product.
