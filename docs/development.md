# Development Guide

## Environment

SynthRAN supports Linux for the reviewed development, CI, and live-control path. Repository hooks, CI, and live operation use the named Conda environment `synthran`. `environment.yml` is the supported Linux environment definition and `pyproject.toml` defines the installed `synthran` command.

Create or reconcile the environment, then install the repository command without resolving a second dependency graph:

```sh
conda env create --file environment.yml
conda activate synthran
python -m pip install --no-deps -e .
```

After a direct dependency update:

```sh
conda env update --file environment.yml --prune
python -m pip install --no-deps -e .
```

Verify the environment and run tests:

```sh
python -c "import os; assert os.environ.get('CONDA_DEFAULT_ENV') == 'synthran'"
python -m unittest discover -s tests -v
```

Direct package versions are exact. Conda still selects platform-specific transitive builds during solving, so the current environment is not an artifact-level lock. A reviewed platform artifact lock is required before making that stronger claim.

When adding or changing a direct dependency, keep `environment.yml` and the authoritative direct dependency metadata in `dependencies.lock.yml` synchronized. Do not weaken dependency-consistency tests to accommodate drift.

## Product command

The supported operator executable is:

```text
synthran
```

There is no interactive frontend or external workbench protocol. Product behavior is reached through explicit CLI arguments. Internal Python modules remain implementation details and are tested directly where appropriate.

## Git hooks

Activate the tracked hook once per clone:

```sh
synthran hooks install --dry-run
synthran hooks install
```

The pre-push hook runs the outgoing-commit privacy scan inside the configured `synthran` Conda environment. Do not bypass a true privacy or secret finding. Remove sensitive content from every affected outgoing commit and rotate an exposed credential when applicable.

## Architecture-sensitive test expectations

Offline tests protect the accepted experiment path and reusable control primitives.

Important areas include:

- **Workspace identity and reconstruction:** initialization, legacy `.synthran` adoption, non-reusable IDs, registry reconstruction, authority conflicts, and safe rollback.
- **Desired/observed separation:** desired-state validation, source truth ordering, freshness, ownership, lifecycle derivation, and fail-closed reconciliation.
- **Operation control:** immutable plan hashes, approval binding, drift rejection, mutation claims, interruption/recovery semantics, and structured operation events.
- **Resource selection/transactions:** deterministic capability placement, fresh and complete inventory requirements, exact resource binding, provider ordering, exact rollback scope, and recovery-required behavior on unknown partial failure.
- **CLI:** parser coverage, stable command routing, campaign runtime cleanup, and a single installed product command.
- **Research schemas and validity:** experiment specifications, campaigns, summaries, measurement windows, probes, network samples, load results, artifact digests, and invalid-run classification.
- **RFSIM resilience:** reconciled UE/PDU handoff, delayed tunnel readiness, dead-process distinction, repeated zero-sample stall detection, complete retry attempts, and route/ownership restoration.
- **R2Lab safety:** lease and allocation authority, exact radio/UE ownership, gNB/N2 evidence, modem readiness, user-plane proof, and bounded cleanup.
- **Research load safety:** temporary target-route ownership, owned iperf3 lifecycle, control-connection readiness, load-target achievement, synchronized sampling, path reproof, and cleanup.
- **Campaign analysis:** deterministic blocked randomization, run immutability, paired differences, and bootstrap confidence intervals.

An offline unit test is not live SLICES or R2Lab acceptance. Live-accepted claims require evidence from the real environment.

## Documentation rule

Documentation is part of the correctness surface. Before completion, compare docs against current code rather than PR intent.

- product commands must exist in the installed `synthran` parser;
- planning must not be described as provider execution;
- source-truth order must match implementation;
- operation risks and gates must match policy;
- live-accepted, offline-tested, and unproven capabilities must remain distinct;
- current evidence belongs in `docs/results.md`, not duplicated across architecture documents.

## Validation

Before considering a change complete:

```sh
python -m unittest discover -s tests -v
synthran privacy scan --worktree
git diff --check
git status --short
```

When available, also run the repository and Git-history secret scan used by CI. Inspect the complete intended diff manually. Offline tests must not require live SLICES credentials.
