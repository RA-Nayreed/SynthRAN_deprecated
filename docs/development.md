# Development

SynthRAN is an experiment orchestrator. 5g-Ansible owns provider context, reservation, POS, Kubernetes, core, RAN, RU/UE deployment state, and teardown. Development must preserve that boundary rather than reintroducing a second infrastructure controller.

## Environment

```zsh
cd ~/SynthRAN
conda activate synthran
python -m pip install --no-deps -e .
synthran deps sync
```

Run the complete offline suite before submitting changes:

```zsh
python -m unittest discover -s tests -v
synthran dev privacy scan --worktree
git diff --check
git status --short
```

The normal GitHub workflow runs the same unit/privacy boundary and also scans Git history for secrets.

## Public interface invariant

The allowed top-level commands are:

```text
run
doctor
inspect
deps
dev
calibrate
analyze
release
```

Tests intentionally protect this compact surface. Do not add backend-specific command groups such as `r2lab`, `network`, or `slices` to expose internal behavior.

- `run` requests an upstream deployment or executes a controlled experiment.
- `doctor` performs a non-mutating upstream `capabilities` + `plan` validation.
- `inspect` reads upstream capabilities or persisted run state.
- `release` delegates exact teardown to 5g-Ansible `down`.
- `calibrate` and controlled/campaign `run` modes operate only on an accepted experiment path.
- `deps` and `dev` are repository-maintenance surfaces.

## Main code boundaries

```text
synthran/cli.py                 public parser and dispatch
synthran/lifecycle.py           full-run experiment orchestration
synthran/adapters/fiveg.py      thin 5g-Ansible machine-API adapter
synthran/network/               read-only network observation/evidence
synthran/experiment/            Amber workload and experiment transport
synthran/research/              controlled measurements and analysis
synthran/utils/                 generic local helpers
```

The following retired architecture must not return:

```text
synthran/r2lab/
synthran/provider.py
synthran/slices_controller.py
synthran/network/resources.py
synthran/upstream_overlay.py
deploy/ansible/
```

Normal unit tests assert these absences and reject direct infrastructure commands in SynthRAN lifecycle/observation code.

## 5g-Ansible boundary

All infrastructure mutation goes through the pinned machine interface:

```text
fiveg capabilities
fiveg plan
fiveg up
fiveg status
fiveg down
fiveg scenario
```

SynthRAN sends a native `fiveg/deployment/v1` specification and consumes the upstream manifest and generated inventory. Do not add local topology support tables, local provider/POS state, worktree overlays, Ansible wrappers, Kubernetes repair paths, gNB restart logic, or UE activation logic.

Experiment code may create bounded experiment-owned resources such as MQTT objects, probes, exact routes, transient relays, and measurement processes. It must clean up exactly what it created and must never repair the 5G deployment.

## Run progress

A run writes terminal progress and `.synthran/events/<run-id>.jsonl` through the same canonical event stream. New long-running experiment work should report through the run-provided progress handle rather than inventing another logger format.

Do not emit subscriber credentials, private keys, kubeconfig material, provider tokens, or raw secret-bearing upstream output.

## Tests

Prefer contract and ownership tests over implementation call counts.

Required coverage for public/runtime changes includes, where relevant:

- exact top-level parser choices and rejection of retired command groups;
- native `fiveg/deployment/v1` construction;
- upstream manifest/inventory provenance;
- absence of retired infrastructure modules;
- read-only network observation;
- RFSIM and physical experiment transport safety;
- exact experiment-owned cleanup;
- deterministic Amber/source identity and evidence;
- privacy and product-language invariants.

Live testbed acceptance is additional evidence; it does not replace the offline suite. Conversely, offline success must never be described as live RFSIM or physical R2Lab acceptance.

## Documentation

Keep documentation synchronized in the same change as CLI/runtime behavior. Do not add dated engineering diaries to `docs/`. Current accepted live evidence belongs in `results.md`; detailed raw evidence belongs in ignored/preserved run storage.

Describe durable concepts: upstream deployment, path observation, workload, measurement, evidence, acceptance, and exact cleanup.

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

Keep generated authority, dependency checkouts, run directories, captures, credentials, and provider evidence out of Git.

## Pull requests

A substantial pull request should state:

- what durable behavior changed;
- which public commands changed, if any;
- which ownership/evidence contracts changed;
- offline test and privacy results;
- whether live RFSIM/physical acceptance was actually performed;
- any capability that remains intentionally unproven.

Do not describe planning, mocks, or unit-test success as live experiment acceptance.
