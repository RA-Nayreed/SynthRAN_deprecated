# Operation control boundary

SynthRAN separates policy, authorization, and execution. Reconciliation or workflow policy identifies a permitted action; the operation layer binds that action to current state and can then issue an ephemeral execution permit. Provider-specific executors remain responsible for final live safety checks.

## Operation lifecycle

Operations use non-reusable workspace IDs such as `op-000041`. Durable records may include:

```text
.synthran/operations/op-000041/
├── operation.json
├── plan.json
├── state.json
├── approval.json
└── events.jsonl
```

`plan.json` is immutable. `state.json` records current local operation state. Approval evidence, when required, is bound to the exact plan. Event logs contain sanitized structured control events rather than raw provider output.

## Immutable plan binding

A plan binds the selected action to the relevant desired state, reconciled observations, policy result, exact targets, and explicitly bound inputs such as a `ResourceDecision`. Its digest is recomputed on load so local modification is rejected.

This prevents approval for one state or target set from being reused after drift.

## Risk and approval

| Risk | Meaning | Approval |
| --- | --- | --- |
| R0 | local/read-only | none |
| R1 | live/read-only | none |
| R2 | controlled mutation | explicit standard approval where policy requires it |
| R3 | destructive mutation | explicit destructive approval |

Approval is local operator consent. It does not replace provider authentication, current ownership, lease authority, or executor-specific safety checks.

## Authorization and drift

Immediately before issuing a permit, SynthRAN recomputes the policy that produced the plan. Authorization fails when desired state, observations, policy, target scope, required approval, or bound inputs no longer match.

A stale approved action must be replanned from current state.

## Exclusive mutation claim

Only one mutating operation may hold workspace mutation authority at a time. A successful clean mutation releases its exact claim. A failed or interrupted mutation retains the claim and enters recovery-required state unless clean rollback is proven.

Recovery inspects current state and releases only the exact claim after safety is established.

## Executor contract

An `ExecutionPermit` proves that local state, policy, approval, targets, and concurrency checks agreed at authorization time. It does not prove that provider state remains unchanged afterward.

Concrete executors must still verify their live boundary. Examples include current SLICES ownership, active R2Lab lease, singleton hardware ownership, exact teardown targets, and current experiment prerequisites.

Unknown provider outcome fails closed and must not be interpreted as no mutation.

## Product boundary

The installed `synthran` CLI is the operator interface. Internal operation APIs may be used by shared orchestration and tests, but they are not an alternate control protocol or user-facing application.
