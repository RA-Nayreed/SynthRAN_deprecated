# Dependencies

SynthRAN locks only dependencies it consumes directly. Infrastructure internals owned by 5g-Ansible are deliberately absent from the SynthRAN lock.

## Lock file

`dependencies.lock.yml` is the reproducibility authority for SynthRAN's direct Git dependencies, experiment container/tool identities, Conda runtime, and CI actions.

Synchronize the managed Git checkouts with:

```zsh
synthran deps sync
```

or select one direct dependency:

```zsh
synthran deps sync --name fiveg_ansible
synthran deps sync --name amber
```

Managed checkouts live below ignored `.deps/` storage and are not committed.

## Direct upstream projects

SynthRAN directly pins exactly two Git repositories:

- `fiveg_ansible`: provider and 5G infrastructure authority exposed through `bin/fiveg`;
- `amber`: deterministic Ambient-IoT source model.

The lock does **not** pin 5g-Ansible's internal Open5GS, srsRAN, Helm, yq, Ansible collection, remote-bootstrap, radio-image, or Kubernetes implementation choices. Those identities belong to the upstream deployment repository and must be reviewed there.

SynthRAN also directly locks the Mosquitto container used by its experiment collector/transport, the iperf3 source used by controlled research measurements, and the Conda packages required to run SynthRAN, Amber, and the pinned machine interface.

## 5g-Ansible boundary

SynthRAN invokes only the pinned machine API:

```text
bin/fiveg capabilities
bin/fiveg plan
bin/fiveg up
bin/fiveg status
bin/fiveg down
bin/fiveg scenario
```

There is no SynthRAN-owned Ansible wrapper tree, no upstream source overlay, and no direct deployment executor. Provider selection, SLICES experiment creation/reuse, reservation/POS work, Kubernetes, core/RAN/RU/UE deployment, and teardown are upstream responsibilities.

`ansible-core` remains in the controller Conda environment because the pinned 5g-Ansible checkout executes its own Ansible implementation in that environment. This does not make SynthRAN the owner of upstream Ansible collections or playbooks.

## Containers and tools

A dependency belongs in this repository only when SynthRAN itself consumes it. Current direct runtime identities are:

- digest-locked Mosquitto for experiment-owned MQTT resources;
- source-locked iperf3 for reproducible research load generation.

A missing upstream deployment tool or image is a 5g-Ansible environment/deployment error. SynthRAN must not compensate by downloading, pinning, or installing an upstream-internal replacement.

## Updating a dependency

A direct dependency update should include:

1. the new immutable identity in `dependencies.lock.yml`;
2. adapter or experiment-boundary updates required by the new interface;
3. tests for the affected boundary;
4. a clean privacy scan;
5. fresh live evidence before capability or result claims are updated;
6. attribution updates when licensing/provenance changes.

For a 5g-Ansible change, first prove its machine-interface tests, then pin the exact reviewed commit in SynthRAN and prove SynthRAN's normal suite against that commit. Unit CI proves interface consistency; live RFSIM/R2Lab acceptance proves the integrated deployment/experiment path.

## Third-party attribution

Licenses and provenance are summarized in [`../THIRD_PARTY.md`](../THIRD_PARTY.md). Do not remove attribution merely because a dependency is fetched dynamically rather than vendored.
