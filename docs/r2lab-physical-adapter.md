# R2Lab physical adapter

The physical gNB path uses the exact configuration selected by the pinned
R2Lab `5g-Ansible` adapter. SynthRAN does not maintain a second radio model.

## Pinned source contract

The dependency lock identifies these immutable revisions:

```text
fiveg_ansible  a0149fc0dde39e2872945a0f3c91e804ece52d4f
srsran_helm    8dfb9890d127734cdcd6eee9df8c5d09b1a8076a
```

At the pinned adapter revision,
`roles/5g/srsRAN/config/tasks/main.yml` selects this chart file for N300:

```text
charts/srsran-gnb/values-n300-n78-20MHz.yaml
```

The user fork `RA-Nayreed/5g-Ansible` and the locked upstream repository resolve
to the same adapter commit. Keeping the existing locked origin therefore uses
the same content while preserving reproducible dependency checkout behavior.

The selected values define the R2Lab radio contract, including:

| Setting | Pinned value |
|---|---:|
| Band | 78 |
| DL ARFCN | 640000 |
| Channel bandwidth | 20 MHz |
| Common SCS | 30 kHz |
| Sample rate | 61.44 MHz |
| TX gain | 35 dB |
| RX gain | 60 dB |
| SS0 index | 0 |
| CORESET0 index | 12 |
| PRACH index | 1 |

The same source supplies the complete UHD N300 device arguments, RU subnet,
and RU pod address.

## SynthRAN overlay boundary

SynthRAN applies only data that is specific to the current authorized run or
required by its safety contract:

- current AMF N2 and gNB bind addresses;
- namespace and selected RAN node;
- digest-locked physical gNB image;
- exact CPU and memory resources;
- zero replicas and `Recreate` replacement;
- disabled optional mutable log sidecar;
- reviewed RU network metadata used for binding discovery.

No carrier, bandwidth, sampling, gain, antenna, PDCCH, PRACH, or UHD frame
setting is overridden.

## Offline validation

The isolated workspace contains both the untouched pinned source values and the
small SynthRAN overlay. Helm receives them in that order. Validation requires:

- exact dependency commits and locked Helm version;
- a SHA-256 proof of the pinned source values;
- a digest-locked gNB image;
- zero replicas and `Recreate`;
- exact equality between every reviewed radio value in the source and render;
- an N300 address matching the authorized binding;
- the reviewed resource request and limit contract;
- no RFSIM, broad cleanup, or optional mutable log sidecar behavior.

The package includes the chart, source values, and overlay. Staging transfers
all three and verifies every digest remotely before Helm can write to the
cluster.

## Singleton lifecycle

One N300 can have only one gNB owner. The lifecycle is:

```text
scale the exact gNB Deployment to zero
  -> prove all matching pods are gone
  -> stage the exact digest-bound package at zero replicas
  -> bind package, values, render, run, and authority evidence
  -> refresh authority
  -> scale the bound Deployment to one
  -> prove exactly one matching pod is Running and ready
```

An overlap or failed N2 proof triggers an exact scale-to-zero recovery. Release
also stops and proves the bound gNB clean before powering off the selected qfit
and N300.

## Dependency sync

Only the physical dependencies are required for this boundary:

```text
python -m synthran deps sync \
  --name fiveg_ansible \
  --name srsran_helm
```

Selective sync never inspects or changes an unrelated local dependency checkout.

## Acceptance boundary

An offline render proves configuration identity and safety, not live success.
Physical acceptance still requires the same authorized run to prove gNB/N2,
qfit management, cell acquisition, registration, PDU session, user plane,
workload, and exact cleanup.
