# Observed state and reconciliation contract

SynthRAN separates requested state from current provider and runtime facts. `desired.json` records requested configuration; observations are collected independently and reduced into `ObservedState`.

A persisted `observed.json` is a cache and evidence surface. It never grants mutation authority by itself.

## Truth ranking

```text
provider
> direct observation
> persisted evidence
> manifest
> cache
```

Current observations require an explicit freshness boundary. Among current observations, source authority outranks timestamp. If no current observation exists, historical evidence can still be shown or inspected but cannot become current mutation authority.

## Dimensions and state

Observed dimensions cover provider context, reservation, allocation, preparation, Kubernetes, core, RAN, UE, PDU, UPF, radio, R2Lab lease, IoT, path, experiment, and dataset state.

Each observation carries state, source, observation time, freshness when applicable, ownership, optional resource identity, and bounded detail or scalar facts.

Common states are:

```text
unknown
absent
pending
ready
degraded
failed
blocked
```

## Ownership

Ownership is explicit and contextual. Fresh SynthRAN ownership may permit reviewed automatic mutation paths; destructive operations require their own policy and exact target binding. `other` or `unknown` ownership fails closed for mutation.

An absent resource can be unowned without granting permission to create it. Creation still requires provider authority, policy, and reviewed resource selection.

## Lifecycle

Lifecycle is derived from desired state and current observations. Representative states include configured, reserved, allocated, prepared, network ready, path proven, experiment running, recovery required, and blocked.

`PATH_PROVEN` requires a current path observation. Historical success remains provenance, not a claim that the current path is usable.

## Reconciliation

`plan_reconciliation()` is pure and emits only the next safe unresolved boundary. Unknown or blocked earlier state prevents later mutation.

Representative progression is:

```text
provider context
-> reservation
-> allocation
-> physical lease when required
-> preparation
-> network runtime
-> path verification
```

Application workflow policy handles experiment, evidence, logs, and teardown separately from network reconciliation.

## Adapter boundary

Provider and domain adapters produce validated observations without redefining the truth hierarchy. They may parse provider-specific state, but raw provider text is never promoted directly into trusted common state.

The installed `synthran` CLI and internal orchestration consume these domain contracts; there is no separate frontend-specific observed-state model.
