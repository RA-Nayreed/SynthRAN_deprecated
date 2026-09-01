# 5g-Ansible ownership boundary

SynthRAN pins `RA-Nayreed/5g-Ansible` as the authoritative implementation of provider context and 5G infrastructure. SynthRAN does not rewrite the checkout and does not maintain a second provider/deployment feature matrix.

## Rule

5g-Ansible owns:

- SLICES project/experiment selection and Post5G network identity;
- provider reservation and POS allocation/preparation;
- Kubernetes and core/RAN deployment;
- RFSIM and physical R2Lab platform mechanics;
- RU and physical UE operations;
- deployment state and teardown;
- the generic scenarios implemented upstream.

SynthRAN owns experiment identity, deterministic Amber workload generation, live path observation, experiment transport, measurements, scientific evidence, acceptance, and experiment-local cleanup.

## Pinned machine interface

The only infrastructure interface consumed by SynthRAN is:

```text
bin/fiveg capabilities
bin/fiveg plan
bin/fiveg up
bin/fiveg status
bin/fiveg down
bin/fiveg scenario
```

The native request is `fiveg/deployment/v1`; the accepted upstream deployment record is `fiveg/deployment-manifest/v1`.

Provider intent is passed in the native request. When provider management is enabled, `fiveg up` selects the requested SLICES project, reuses or creates the named experiment, acquires the Post5G network, persists that evidence, and revalidates it on resume. SynthRAN never implements that sequence itself.

## Capability ownership

Core, RAN, platform, RU, UE, monitoring, reservation, cleanup, and scenario choices are upstream capabilities. SynthRAN does not hard-code a second list merely to decide whether 5g-Ansible may deploy a request.

Experiment requirements are different. A current workload may require a specific observed PDU interface or physical user-plane route after deployment. Such a requirement can reject an experiment without redefining upstream deployment support.

## R2Lab cleanup

The pinned 5g-Ansible cleanup path is selected-resource only:

- stop only UEs present in the deployment inventory;
- power off only the selected RU;
- never broaden cleanup to all hardware;
- delete no Kubernetes namespace unless explicitly authorized by upstream deployment policy.

SynthRAN does not duplicate this cleanup logic.

## SSH contract

Machine-mode R2Lab deployment defaults to strict host-key checking. Experiment-side SSH must preserve strict verification as well. Required infrastructure behavior belongs upstream as an explicit machine-interface option, not as a source patch in SynthRAN.

## Source-tree rules

- `.deps/5g_ansible*` is immutable after dependency synchronization.
- Runtime source rewriting, copied upstream roles, and deployment overlays are forbidden.
- There is no `synthran/provider.py`, `synthran/slices_controller.py`, `synthran/r2lab/`, `synthran/network/resources.py`, `synthran/upstream_overlay.py`, or `deploy/ansible/` infrastructure layer.
- If infrastructure behavior must change, change `RA-Nayreed/5g-Ansible`, validate it there, pin the exact reviewed commit, and consume it through `FiveGAdapter`.
- Experiment workload, observation, measurement, evidence, and acceptance remain SynthRAN responsibilities.
