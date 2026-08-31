# 5g-Ansible ownership boundary

SynthRAN pins `RA-Nayreed/5g-Ansible` as the authoritative implementation of 5G deployment and infrastructure mechanics. SynthRAN does not rewrite the pinned checkout and does not maintain a second deployment feature matrix.

## Rule

5g-Ansible owns the mechanics required to build and tear down the requested 5G environment: core/RAN selection, POS and Kubernetes preparation, Open5GS/OAI/Free5GC, srsRAN/OAI/UERANSIM, RFSIM, R2Lab RU/UE operations, monitoring, and the generic scenarios already implemented upstream.

SynthRAN owns experiment semantics: experiment identity, deterministic workload generation, measurement/evidence, research acceptance, result collection, and experiment-level resumability. During the transition, legacy SynthRAN lifecycle code may still invoke existing upstream playbooks directly, but it must treat the dependency tree as immutable.

## Pinned machine interface

The dependency must expose the machine-facing 5g-Ansible interface introduced by the pinned commit:

```text
bin/fiveg capabilities
bin/fiveg plan
bin/fiveg up
bin/fiveg status
bin/fiveg down
bin/fiveg scenario
```

Deployment policy is expressed through upstream variables rather than source transformations. The required policy surface includes preparation-only mode, live-install policy, OS/Python dependency ownership, disruptive cluster-operation policy, a prepared Python interpreter, selected slices/UEs, optional Open5GS UI/admin setup, POS allocation ownership, explicit cleanup namespaces, and R2Lab host-key policy.

An empty upstream slice/UE selection means the full selected 5G profile. SynthRAN must not hard-code a second list of 5G combinations merely to decide whether 5g-Ansible may deploy them.

## R2Lab cleanup

The pinned 5g-Ansible cleanup path is selected-resource only:

- stop only UEs present in the deployment inventory;
- power off only the selected RU;
- never run `all-off`;
- delete no Kubernetes namespace unless it is explicitly authorized for cleanup.

SynthRAN's compatibility check fails closed if the pinned dependency regresses to global R2Lab cleanup.

## SSH contract

Machine-mode R2Lab deployment defaults to strict host-key checking. SynthRAN-owned SSH that remains during the migration must also preserve its existing strict transport policy. No caller is allowed to patch the 5g-Ansible checkout to change SSH behavior; required behavior belongs upstream as an explicit option.

## Source-tree rules

- `.deps/5g_ansible*` is immutable after dependency synchronization.
- Runtime search/replace, monkey-patching, copied role edits, and temporary source transformations are forbidden.
- `synthran.upstream_overlay` is now a temporary **read-only compatibility validator**; despite its historical name it performs no overlay.
- If deployment behavior must change, change `RA-Nayreed/5g-Ansible`, validate it there, pin the new commit, then consume it from SynthRAN.
- Existing SynthRAN deployment wrappers are migration debt and should be removed in later Great Purge batches in favor of the machine interface; they must not grow new deployment mechanics.
- Experiment workload, measurement and evidence code remains a SynthRAN responsibility.
