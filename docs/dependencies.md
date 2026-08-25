# Dependency Reuse and Updates

## Reuse model

SynthRAN composes upstream systems; it does not absorb them.

- Git dependencies are complete detached checkouts at immutable commits.
- Container images use immutable digests.
- Direct Conda dependencies use exact versions under `conda.packages`, and the lock declares `linux-64` as the supported platform.
- Mutable upstream branch names are provenance notes only and are never runtime selectors.
- No Git submodules are used.
- Dependency source belongs under ignored `.deps/` storage.

Complete checkouts matter because `5g_ansible` and Contiki-NG behavior depends on repository-relative roles, examples, templates, build files, and scripts. Copying only visible entry points would make SynthRAN responsible for reconstructing undocumented upstream coupling.

## Synchronization

Activate `synthran` and install the repository command before running these operations.

Preview direct checkouts:

```sh
synthran deps sync --dry-run
```

Synchronize them:

```sh
synthran deps sync
```

Inspect locked transitive Git repositories as well:

```sh
synthran deps sync --all
```

Sync named dependencies without inspecting unrelated checkouts:

```sh
synthran deps sync \
  --name fiveg_ansible \
  --name srsran_helm
```

Synchronization refuses an origin mismatch or dirty managed checkout. It never merges an upstream branch and never discards local work.

## Golden-path variable mapping

The pinned `5g_ansible` tree accepts its transitive repositories through Ansible variables:

| Locked dependency | Ansible variable |
|---|---|
| `sopnode/open5gs-k8s` | `repo_branch` |
| `turletti/srsran-helm` | `version` |

The golden-path planner and executor pass these exact commits. The SynthRAN-owned preparation overlay pins required Ansible collections and host tooling. Deployment is separately evidence-gated, verifies locked inputs, replaces selected mutable image tags with Linux AMD64 digests before Kubernetes sees a manifest, and uses an isolated detached `5g_ansible` worktree rather than invoking upstream `deploy.sh`.

The tracked preparation and deployment patches apply only to their locked upstream commits. A patch-context mismatch is terminal.

## Research measurement dependencies

Controlled research experiments and capacity calibration rely on tooling across controller, host, and container environments:

- `iperf3` provides reference capacity and controlled UDP background load.
- `ping` and `ip` provide continuous path probing and route ownership checks.
- `pyarrow` derives deterministic compressed Parquet tables from accepted JSONL audit records.
- OpenJDK provides the Java runtime required by Cooja.

The current operator interface does not require Node.js or an interactive-terminal library.

## Update procedure

Update one dependency at a time:

1. Resolve the intended source reference to an immutable commit or digest.
2. Inspect its license and redistribution implications.
3. Update `dependencies.lock.yml` and `THIRD_PARTY.md` together when provenance changes.
4. Synchronize and verify a clean detached checkout.
5. Run all offline tests and privacy checks.
6. Complete the golden-path compatibility test appropriate to that dependency.
7. Preserve accepted evidence outside generated or private repository paths.

Do not copy `5g_ansible` source into SynthRAN. Its pinned tree has no asserted top-level license, so derivative publication requires clarification.
