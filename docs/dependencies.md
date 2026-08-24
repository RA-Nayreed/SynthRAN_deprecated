# Dependency Reuse and Updates

## Reuse model

SynthRAN composes upstream systems; it does not absorb them.

- Git dependencies are complete detached checkouts at immutable commits.
- Container images use immutable digests.
- Direct Conda dependencies use exact versions under `conda.packages` (including Python 3.12.13 and OpenJDK 21.0.9 required by Cooja), and the lock declares `linux-64` as the only supported platform.
- Mutable upstream branch names are provenance notes only and are never runtime selectors.
- No Git submodules are used.
- Dependency source belongs under ignored `.deps/` storage.

Complete checkouts matter because `5g_ansible` and Contiki-NG behavior depends on repository-relative roles, examples, templates, build files, and scripts. Copying only visible entry points would make SynthRAN responsible for reconstructing undocumented upstream coupling.

## Synchronization

Activate `synthran` before running these commands.

Preview the two direct checkouts:

```sh
python -m synthran deps sync --dry-run
```

Synchronize them:

```sh
python -m synthran deps sync
```

Inspect locked transitive Git repositories as well:

```sh
python -m synthran deps sync --all
```

Sync named dependencies without inspecting unrelated checkouts:

```text
python -m synthran deps sync \
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

The golden-path planner and executor pass these exact commits. The SynthRAN-owned preparation overlay pins `kubernetes.core==6.5.0`, `community.general==13.0.1`, and `ansible.posix==2.2.2`; the latter two provide the locked upstream roles' required `community.general.modprobe` and `ansible.posix.mount` actions. The locked graph contains no `community.kubernetes` call, so that legacy collection is not installed. The overlay also ensures runtime host packages (`net-tools` for `ifconfig`) are present, pins Helm `3.18.4` from its locked Linux AMD64 archive digest, yq `4.45.1` from its locked binary digest, and exact direct remote Python package versions including `kubernetes==32.0.1`. Those direct Python versions do not freeze the complete transitive installation graph.

The tracked `resource-preparation-boundary.patch` applies only to the locked upstream commit. It removes per-node free/allocation tasks, skips the mutable `k9s` helper, and prevents entry into Open5GS or srsRAN roles. The first native preparation intentionally accepts the remaining upstream apt, chart, manifest, and installer transitives; it is version-pinned, not artifact-reproducible. After explicit operator acceptance, `dependencies.lock.yml` records `resource_bootstrap.status` as `ready`. Later locking should target only dependencies shown by a native run to be unstable or scientifically material.

Deployment is separately evidence-gated, verifies its locked inputs, and replaces every selected mutable image tag with a Linux AMD64 digest before Kubernetes sees a manifest. It uses an isolated detached `5g_ansible` worktree and never invokes upstream `deploy.sh`.

The tracked `deploy/ansible/patches/golden-path-boundary.patch` applies only to the locked `5g_ansible` commit and is checked before application. It prevents the selected roles from restarting the cluster, installing or upgrading host packages, downloading mutable tools, deploying the optional WebUI, overriding remote task interpreters with the controller's local Python path, or expanding the runtime beyond slice one and one srsUE. A patch-context mismatch is terminal.

## Research measurement dependencies

Controlled research experiments and capacity calibration rely on tooling across controller, host, and container environments:

- `iperf3`: Installed in the srsUE container image (`-c ue`) and on the root core node (`inventory.core_node`), executed as a run-owned server on the core node and as a client inside srsUE for saturating capacity calibration and controlled UDP background load generation.
- `ping` and `ip`: Installed inside the srsUE container environment and preflighted at runtime for continuous RTT probing (`-I tun_srsue1`) and temporary target route management (`ip route add`).
- `pyarrow` (`19.0.1` in Conda lock): Used by the research collector on the controller to derive deterministic, compressed Parquet tables (`probe.parquet`, `network-samples.parquet`, `load.parquet`, `telemetry.parquet`) directly from accepted JSONL audit records.

## Update procedure

Update one dependency at a time:

1. Resolve the intended source reference to an immutable commit or digest.
2. Inspect its license and redistribution implications.
3. Update `dependencies.lock.yml` and `THIRD_PARTY.md` together.
4. Synchronize and verify a clean detached checkout.
5. Run all offline tests and privacy checks.
6. Complete the golden-path compatibility test appropriate to that dependency.
7. Record the rationale and evidence in the local decision journal.

Do not copy `5g_ansible` source into SynthRAN. Its pinned tree has no asserted top-level license, so derivative publication requires clarification.
