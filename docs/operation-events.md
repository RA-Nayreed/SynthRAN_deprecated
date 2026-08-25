# Structured operation event stream

SynthRAN records operation progress as validated structured events rather than treating raw provider command output as trusted state.

Each operation can have an append-only `.synthran/operations/<operation-id>/events.jsonl` stream. Sanitized events may also be appended to the workspace session event stream for audit and aggregation.

## Event types

The control vocabulary includes:

```text
operation.started
plan.created
approval.requested
approval.granted
operation.authorized
stage.started
stage.progress
stage.completed
stage.failed
state.changed
operation.completed
operation.failed
operation.interrupted
recovery.required
```

Stage progress contains bounded stage and counter metadata. Failure events contain safe failure classifications rather than arbitrary stderr, command lines, addresses, tokens, or provider payloads.

`state.changed` is restricted to reviewed SynthRAN dimensions and state values.

## Integrity

Operation events bind operation ID, contiguous local sequence, derived event ID, timestamp, risk, mutation flag, plan digest, and bounded validated attributes.

Loading an event stream validates sequence and immutable plan bindings. A malformed or modified stream is rejected rather than partially accepted as trusted history.

## Executor boundary

Concrete executors translate provider activity into controlled stage names, progress counters, state transitions, and safe failure codes. Detailed private logs can remain in their run-specific evidence locations; they do not become trusted operation state merely by being emitted to stdout or stderr.

An operation plan alone is not evidence that live execution began. Provider progress exists only when a concrete executor has been authorized and emits corresponding events.

## Interruption

Interruption uses `operation.interrupted`. If a mutating operation already holds the exclusive claim, interruption retains that claim and enters recovery-required state until current infrastructure state and exact cleanup are proven.
