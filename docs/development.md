# Development

SynthRAN development should preserve a small public interface and push backend-specific complexity into tested internal modules.

## Environment

```zsh
cd ~/SynthRAN
conda activate synthran
python -m pip install --no-deps -e .
synthran deps sync
```

Run the complete unit suite before submitting changes:

```zsh
python -m unittest discover -s tests -v
```

Run repository privacy checks through the maintenance namespace:

```zsh
synthran dev privacy scan --worktree
```

## Public interface invariant

The allowed top-level commands are:

```text
run
 doctor
inspect
logs
stop
research
deps
dev
```

Tests intentionally assert this exact set. Do not add a backend-specific command group to expose an internal function. If new functionality belongs to the lifecycle, compose it inside `synthran run`; if it is read-only/run-oriented, consider `doctor`, `inspect`, or `logs`; if it is cleanup, use `stop`.

## Main code boundaries

```text
synthran/cli.py                 parser entry
synthran/operator.py            public commands and dispatch
synthran/provider.py            shared provider context
synthran/backends/run.py        complete run orchestration
synthran/ansible_streaming.py   shared Ansible streaming
synthran/network/               virtual implementation
synthran/r2lab/                 physical implementation
synthran/experiment/            deterministic workload
synthran/research/              controlled measurements/analysis
synthran/command_runtime.py     internal virtual/research support
```

`command_runtime.py` deliberately has no public parser or top-level dispatch tree.

## Ansible

Use the existing sanitized streamer for every long Ansible operation:

```python
run_streaming_ansible_command(...)
```

It already handles output capture, useful-task filtering, failure rendering, and heartbeats. New raw `ansible-playbook` subprocess wrappers should be treated as duplication unless there is a demonstrated requirement the shared wrapper cannot satisfy.

## Run progress

A run writes terminal progress and `.synthran/events/<run-id>.jsonl` through the same stream. New long-running work should report through the run-provided `TextIO` progress handle rather than inventing a new logger format.

Sanitized child output may be written to the handle directly. Do not emit subscriber credentials, private keys, kubeconfig material, or raw secret-bearing provider output.

## Backend changes

Backend mechanics may differ below the 5G user-plane boundary, but changes must preserve common run semantics and evidence meaning. Check `backend-contract.md` before changing physical or virtual acceptance behavior.

A physical change requires particular care around:

- current lease/allocation verification;
- exact run ownership;
- singleton radio/gNB behavior;
- UE role scoping;
- independent postcondition proof;
- exact cleanup.

## Tests

Prefer tests around contracts and boundaries instead of implementation call counts when possible.

Required coverage for public-interface changes includes:

- exact top-level parser choices;
- rejection of removed legacy command groups;
- backend selection through `run --radio`;
- unified progress persistence;
- `--quiet` retaining persisted events;
- provider context create/reuse behavior;
- physical safety checks relevant to the change.

Live testbed acceptance is additional evidence; it does not replace unit tests.

## Documentation

Keep documentation synchronized in the same change as CLI/runtime behavior. Do not add dated engineering diaries to `docs/`. Current accepted live evidence belongs in `results.md`; detailed raw evidence belongs in ignored/preserved run storage.

Avoid introducing temporary implementation terminology into public docs. Describe durable concepts: run, backend, provider, resource authority, path proof, workload, measurement, evidence, cleanup.

## Privacy

Before pushing:

```zsh
synthran dev privacy scan --worktree
git diff --check
git status --short
```

The pre-push hook can be activated with:

```zsh
synthran dev hooks install
```

Do not weaken privacy checks to accommodate generated private evidence. Keep generated authority, dependency worktrees, run directories, private captures, and credentials out of Git.

## Pull requests

A substantial pull request should state:

- what durable behavior changed;
- which public commands changed, if any;
- which evidence/contracts are affected;
- test results;
- whether live acceptance was performed;
- any capability that remains intentionally unproven.

Do not describe planning or unit-test success as live experiment acceptance.
