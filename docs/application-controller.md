# Application controller

`ApplicationController` is an internal domain boundary for persistent workspace authority, desired and observed state, reconciliation, workflow policy, resource decisions, and operation control. It is not a product interface.

## Responsibilities

The controller can:

- resolve the active workspace, profile, project, and experiment;
- create validated local SynthRAN experiment records;
- ingest source-specific observations and persist the reconciled snapshot;
- derive lifecycle and reconciliation state;
- evaluate application workflow policy;
- create, approve, authorize, finish, and interrupt immutable operations;
- bind resource decisions and exact targets to operations;
- coordinate the generic resource-transaction engine when concrete adapters are supplied.

Provider-specific parsing and mutation remain outside this class. The controller consumes validated domain objects rather than treating raw provider text as state.

## State separation

Requested configuration lives in desired state. Provider-assigned or runtime-discovered values live in observed state. Stale observations remain useful history but cannot authorize current mutation.

The truth order remains:

```text
provider
> direct observation
> persisted evidence
> manifest
> cache
```

## Operation boundary

An operation plan binds the current desired state, reconciled observations, policy result, exact targets, and any required resource decision. Authorization recomputes the relevant policy and rejects drift before issuing an ephemeral `ExecutionPermit`.

The permit is a local control-plane handoff, not provider authority. Concrete executors must still perform final live provider, ownership, freshness, and safety checks at their mutation boundary.

Failed or interrupted mutation retains the exclusive mutation claim unless exact clean rollback is proven.

## Resource transactions

`execute_resource_operation()` is the generic multi-provider path when a plan, current inventory, matching `ResourceDecision`, and concrete provider adapters are all available. Generic rollback is limited to exact resource IDs that an adapter proves were created by the current operation.

Provider-specific scripted executors remain responsible for their own live safety contracts until deliberately migrated behind a shared backend boundary. They must not be wrapped in hidden compatibility calls that bypass their established checks.

## Product boundary

The supported operator interface is the installed `synthran` command. `ApplicationController` and its Python methods are internal implementation APIs used by policy, orchestration, and tests.
