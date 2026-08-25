# Persistent workspace and identity model

SynthRAN separates durable controller identity, workspace configuration, experiment identity, requested state, observed provider/runtime facts, operation records, and measurement evidence. These records have different lifetimes and authority.

## On-disk model

Representative persistent state is:

```text
~/.config/synthran/profiles/<profile>.toml

<repository>/.synthran/
├── workspace.toml
├── registry.sqlite3
├── active.json
├── access/
├── experiments/
│   └── sran-YYYYMMDD-NNN/
│       ├── experiment.toml
│       ├── desired.json
│       ├── observed.json
│       └── runs/
├── operations/
│   ├── active-mutation.json
│   └── op-NNNNNN/
└── sessions/events.jsonl
```

Historical accepted preparation, run, and experiment directories may coexist with this model and are not silently migrated or renamed.

## Authority by record

Profiles contain stable controller identity references. `workspace.toml` binds profile, project, and stable workspace policy. Access records are caches. `active.json` selects a local experiment. `experiment.toml` records durable experiment identity and provider binding. `desired.json` is requested-state authority. `observed.json` is an observation cache whose original source and freshness remain authoritative.

Current reservation, allocation, lease, registration, route, and runtime facts come from provider or direct observation when fresh. An on-disk cache never authorizes mutation by itself.

## Credentials

Profiles store identity references and public-key fingerprints, never private-key bytes. Private keys, provider tokens, subscriber credentials, kubeconfigs, and raw secret-bearing authority payloads do not belong in persistent tracked state.

## Experiment and run identity

Experiment IDs use `sran-YYYYMMDD-NNN`. Runs use experiment-local `run-NNN[-label]`. Operations use workspace-wide `op-NNNNNN` IDs. Issued IDs are non-reusable even when an interrupted creation leaves only partial durable state.

The registry provides atomic allocation and lookup, but durable filesystem records remain part of recovery so counters cannot move backward after registry loss.

## Desired and observed state

Runtime-assigned addresses, pod names, reservation/allocation IDs, leases, registration state, and interfaces never become desired state merely because they were observed.

The truth hierarchy is:

```text
provider
> direct observation
> persisted evidence
> manifest
> cache
```

Persisting an observation does not promote it above its original authority or extend its freshness.

## Mutation claims

Mutating operations can hold one exclusive workspace claim. The claim remains when a mutation fails or is interrupted unless exact cleanup is proven. Recovery must reconcile current provider state before releasing it.

## Existing evidence

Initialization and workspace helpers preserve pre-existing accepted evidence. Ambiguous partial persistent-workspace state fails closed rather than being overwritten or recursively removed.

## Product boundary

Workspace modules are internal state-management primitives used by the installed `synthran` command and shared orchestration. There is no interactive first-launch product path or alternate workspace control protocol.
