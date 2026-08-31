# Contributing to SynthRAN

SynthRAN is a reproducible experiment platform joining deterministic IoT traffic, open 5G networking, physical testbed integration, and research evidence. Contributions should preserve one product interface, one experiment contract, and strict evidence-based safety.

## Product boundary

There is one installed executable:

```text
synthran
```

The supported top-level commands are intentionally small:

```text
run
doctor
calibrate
inspect
analyze
release
deps
dev
```

Do not add a second executable, a backend-specific command family, or a second live-log command. Backend choice belongs behind `--radio rfsim|r2lab`. Internal Python functions may remain narrow and backend-specific when the hardware mechanics genuinely differ.

## Backend contract

RFSIM is the virtual reference implementation. R2Lab provides the physical-radio implementation of the same experiment semantics.

Backend-specific mechanics may differ below the user-plane boundary: radio hardware, UE control, registration observation, PDU interfaces, lease authority, and cleanup. Above that boundary, run identity, deterministic workload inputs, telemetry meaning, measurement validity, evidence, provenance, and cleanup semantics must remain consistent.

Never weaken an accepted virtual-path invariant merely to make a physical path fit. Never claim a physical capability beyond current accepted evidence.

## Authority and safety

Current provider observation is the source of truth for live mutation. Persisted evidence proves what happened previously; it does not become current authority.

Unknown, stale, foreign, expired, failed, or ambiguous ownership must fail closed.

Never use broad cleanup such as wildcard deletion, global radio power-off, `pkill`, or `killall` when an exact run-owned target exists. Rollback and cleanup must operate on the exact resources proven to belong to the current run.

Run IDs are immutable. Failed or invalid runs remain diagnostic evidence and must not be reused or silently reclassified.

## Provider context

A SLICES project must already exist and be accessible to the operator. A unified `synthran run` may select the configured project, create or reuse the provider experiment named by the run, and acquire the Post5G prefix required for that run. It must not create projects, bypass authentication, or invent provider authority.

## Ansible boundary

SynthRAN wraps pinned upstream Ansible content rather than reimplementing its mechanics. All long Ansible operations must use `synthran.ansible_streaming.run_streaming_ansible_command` so RFSIM and R2Lab produce the same sanitized execution events and heartbeat behavior.

An Ansible TASK header is not execution evidence. A task that is subsequently skipped must not appear in the normal operator stream as executed work. Routine package/configuration chatter remains forensic-log material. Long meaningful tasks may emit heartbeats. Failures must retain a bounded sanitized task, host, state, and reason.

Do not introduce a second Ansible subprocess/progress contract for a new live path.

## Run event contract

`synthran run` is the only live operator progress surface. Lifecycle events, Ansible-derived progress, AMBER/research events, and acceptance use the same `[synthran]` renderer.

Every run also persists the canonical structured event evidence under:

```text
.synthran/events/<run-id>.jsonl
```

The JSONL event record is evidence, not a second public logging workflow. Detailed preparation/deployment/component logs are forensic artifacts and must not become another operator progress API.

Network readiness and workload transport proof are distinct claims. A healthy gNB, live UE PDU session, and UPF route establish network/session readiness. End-to-end transport claims require a traffic or connection proof through the live UE PDU path.

## Research data

JSONL is the append-only audit source and deterministic Parquet is the analysis derivative. Preserve run specifications, measurement windows, telemetry, RTT probes, network counters, load evidence, validity summaries, dependency provenance, and artifact digests.

Do not confuse nominal observation-window occupancy with packet loss. Sequence gaps and duplicate sequence identifiers are the primary observed telemetry-continuity evidence. Requested network-counter cadence is not proof of achieved cadence; persisted timing evidence determines instrumentation validity.

Capacity calibration and controlled load must terminate outside the 5G core host. See `docs/research-measurement-peer.md`.

## Dependencies

Pinned upstream repositories belong under ignored `.deps/` storage. Do not vendor partial copies for convenience. Runtime images must remain locked to reviewed identities and third-party provenance must remain documented.

## Credentials and privacy

Never commit provider tokens, subscriber credentials, private SSH keys, kubeconfigs, private authority files, unsanitized secret-bearing captures or logs, generated live run directories, or dependency worktrees.

Repository checks include privacy scanning and pre-push protection. Correct false positives narrowly; do not weaken a rule simply to make a check pass.

## Documentation

Public documentation has clear roles:

- `README.md` — overview and compact runnable examples;
- `docs/architecture.md` — durable system boundaries;
- `docs/backend-contract.md` — backend parity and safety contract;
- `docs/operator-guide.md` — complete operating procedure;
- `docs/experiment.md` — deterministic workload and research protocol;
- `docs/r2lab-integration.md` — physical backend details;
- `docs/results.md` — canonical accepted evidence and interpretation limits;
- `docs/dependencies.md` — pinned external dependencies;
- `docs/security.md` — credentials, privacy, and mutation safety;
- `docs/development.md` — local validation and contribution workflow.

Do not create historical command guides or duplicate current-status documents. Put accepted measurements in `docs/results.md` and preserve raw evidence outside ordinary Git history.

## Validation

Before merging substantial changes, run from the repository root in the `synthran` environment:

```zsh
python -m unittest discover -s tests -v
synthran dev privacy scan --worktree
git diff --check
git status --short
```

Inspect the complete diff and confirm that public commands match the installed parser, documentation matches current behavior, generated private evidence is absent, cleanup is exact, and capability claims remain bounded by accepted evidence.
