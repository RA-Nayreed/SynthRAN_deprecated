# Dependencies

SynthRAN integrates reviewed upstream projects by immutable identity. External repositories remain external dependencies; SynthRAN wraps them with validation, narrow overlays, evidence, and cleanup rather than vendoring or rewriting them.

## Lock file

`dependencies.lock.yml` is the reproducibility authority for reviewed upstream Git commits, tool versions, and container identities.

Synchronize the managed checkouts with:

```zsh
synthran deps sync
```

or select exact dependencies:

```zsh
synthran deps sync --name fiveg_ansible --name srsran_helm
```

Managed checkouts live below ignored `.deps/` storage and are not committed.

## Core upstream projects

The run path depends on reviewed versions of components including:

- `fiveg_ansible` for SLICES/R2Lab infrastructure and UE roles;
- `srsran_helm` for reviewed srsRAN deployment/radio profiles;
- Open5GS Kubernetes material used by the virtual and physical foundation;
- Contiki-NG/Cooja for deterministic IoT traffic;
- runtime tools such as Helm, Ansible, Mosquitto, iperf3, and provider CLIs.

Exact commits and image identities belong in the lock file, not duplicated as mutable documentation claims.

## Shared Ansible execution

Ansible is a common actuation mechanism across both radio backends. Long Ansible work must use:

```text
synthran.ansible_streaming.run_streaming_ansible_command
```

This applies to virtual deployment, physical Open5GS reconciliation, and R2Lab UE setup/connect/stop. The shared wrapper provides one sanitized progress/failure/heartbeat contract.

Do not add a second Ansible subprocess implementation for a new backend path.

## Overlays

SynthRAN may create an isolated worktree or temporary copy of a locked upstream dependency and apply reviewed, deterministic transformations required by the experiment contract. Such transformations must:

- start from the exact locked commit;
- fail if expected upstream source no longer matches;
- remain run-local;
- record the resulting provenance/hash where relevant;
- never mutate the managed locked checkout in place.

## Containers

Runtime container identities should be digest-addressed where supported. A mutable tag alone is not sufficient reproducibility evidence for a claimed accepted path.

Physical radio images that are tied to a reviewed upstream profile must match the profile and lock contract before cluster mutation.

## Tool discovery

A missing executable is an environment error, not permission to fetch an arbitrary replacement during a live run. Installation belongs to the reviewed environment/setup path. Live execution should use the dependency identities already selected by the repository environment and lock file.

## Updating a dependency

A dependency update should include:

1. the new immutable identity in `dependencies.lock.yml`;
2. adapter/overlay updates required by upstream changes;
3. tests for the affected boundary;
4. a clean privacy scan;
5. fresh live evidence before capability claims are updated;
6. third-party attribution updates if licensing/provenance changed.

A unit-test pass proves adapter consistency, not live acceptance. `docs/results.md` changes only when new accepted evidence justifies them.

## Third-party attribution

Licenses and provenance are summarized in [`../THIRD_PARTY.md`](../THIRD_PARTY.md). Do not remove attribution merely because a dependency is fetched dynamically rather than vendored.
