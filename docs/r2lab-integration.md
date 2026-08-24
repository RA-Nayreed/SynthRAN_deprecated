# R2Lab integration

`r2lab-integration` is the temporary integration branch for physical R2Lab work.
`main` remains the accepted RFSIM truth until an immutable physical run completes
the acceptance sequence and cleanup review.

## Current truth

The virtual Open5GS, srsRAN, srsUE, and RFSIM path remains unchanged.

Historical R2Lab runs established exact-resource authority, the SLICES/POS and
Kubernetes foundation, Open5GS, an N300-backed gNB, N2/SCTP, qfit provisioning,
UHD/IQ transport, and exact cleanup. The physical gNB now consumes the exact
N300 values selected by the pinned R2Lab adapter rather than reconstructing a
second radio profile inside SynthRAN:

| Setting | Value |
|---|---:|
| Band | 78 |
| Adapter commit | `a0149fc0dde39e2872945a0f3c91e804ece52d4f` |
| Chart commit | `8dfb9890d127734cdcd6eee9df8c5d09b1a8076a` |
| Values source | `charts/srsran-gnb/values-n300-n78-20MHz.yaml` |
| DL ARFCN | 640000 |
| SCS | 30 kHz |
| Channel bandwidth | 20 MHz |
| Sample rate | 61.44 MHz |
| TX/RX gain | 35/60 dB |
| PDCCH | SS0 0, CORESET0 12 |
| PRACH index | 1 |

The source file is copied, hashed, rendered, transferred, and verified without
radio-field overlays. A rendered value that differs from the pinned source is
rejected before any cluster write.
Current live work has observed NR5G-SA registration and an IPv4 PDU address on
qfit, but these observations do not replace an immutable end-to-end acceptance
record. User plane, the physical workload, and exact cleanup must still be proven
in the same authorized run.

## Package

The physical implementation is contained in `synthran/r2lab/`:

```text
controller.py   resource authority, provider commands, prepare and release
provider.py     provider-state parsing
radio.py        reviewed RF profile and sanitized UE state
deployment.py   physical chart, staging, start, and render validation
acceptance.py   ordered immutable evidence
readiness.py    FIT/qfit readiness
n2.py           N2 evidence parsing
runtime.py      read-only gNB, qfit, and user-plane observation
ue.py           MBIM activation, rollback, and workload handoff
handoff.py      external authority handoff
foundation.py   SLICES, Kubernetes, and pinned Open5GS reconciliation/proof
```

The CLI and tests import this package directly. There is no duplicate R2Lab
surface under `synthran.network`.

## Safety rules

- Exact provider observation is state truth; return codes are diagnostic.
- Every mutation is bound to the active run, lease, and selected resources.
- Broad cleanup commands and automatic reservation are prohibited.
- A claim is removed only after every selected resource is proven clean.
- The N300 is a singleton owner: zero matching gNB pods must be proven before
  release or reconfiguration, and exactly one ready pod must be proven at start.
- Physical staging is immutable, digest-bound, and stopped at zero replicas.
- FIT image loading is scoped to the selected qfit and a freshly verified lease.
- Raw modem output and subscriber identifiers are not persisted.

## Physical runtime

The deployment boundary renders the reviewed Open5GS/f2 and srsRAN/f3 topology,
pins the UHD image by digest, enforces `Recreate`, validates the pinned source,
and uses exact CPU and memory requests and limits for predictable gNB placement.
SynthRAN overlays only current N2 addresses, namespace and node placement,
immutable image identity, resources, and stopped singleton safety.

The qfit path maps logical resources such as `qfit07` to physical FIT hosts such
as `fit07` in one controller function. All nested SSH commands use strict host
verification. When provider state proves a qfit off, preparation loads the
reviewed `mbim-quectel-any-dnn` image on that exact FIT node and requires the
provider to prove the host on before the R2Lab SSH wait. An already-on
provisioned node is preserved and must still pass the SSH wait before modem
preparation. The FIT host and its external modem USB rail are separate power
boundaries. Preparation observes the exact USB state, powers on only an
observed-off selected rail, and waits until `/dev/ttyUSB2`,
`/dev/cdc-wdm0`, and `wwan0` are all present before running the image initializer.
It reproves that management surface after initialization. Unknown state fails
closed and retains the claim. Release verifies both the selected USB rail and
FIT host off before removing the claim. Software-radio and packet-attachment
mutations remain isolated in the later UE activation boundary.

The provider image can reveal registration after packet attachment is requested.
The mutation order is therefore:

```text
current authority and gNB/N2 proof
  -> qfit management proof
  -> current sanitized modem observation
  -> software radio on
  -> packet attach request
  -> MBIM connection and IPv4 configuration
  -> final sanitized modem observation
```

Acceptance remains ordered independently of mutation order:

```text
resource authority
  -> SLICES foundation
  -> Kubernetes
  -> Open5GS
  -> gNB/N2
  -> UE management
  -> cell acquisition
  -> registration
  -> PDU session
  -> user plane
  -> workload
```

A failed stage blocks every later stage. User-plane and workload acceptance each
refresh current authority and reprove the physical path.

## Foundation acceptance

After `r2lab prepare` has made the selected N300 and qfit ready, the physical
foundation command verifies both authority domains, both selected Kubernetes
nodes, a stopped physical gNB, and the Open5GS namespace owner. If the
allocation identifier is omitted and another
operator still holds either selected node, one active owner reservation covering
both nodes authorizes exact forced releases and one shared replacement allocation.
The command binds every allocation mutation to the same live reservation and
unchanged R2Lab claim, then proves the new allocation before continuing. An
explicit allocation identifier never permits reclamation.

The namespace handoff also recovers the exact legacy state where the `open5gs`
namespace and `srsran-gnb` Deployment have no run owner. Recovery is allowed
only when at most one matching unowned gNB pod exists. The command refreshes
both authority domains, scales that exact Deployment to zero, proves that its
pod is gone, and then assigns the current run owner. A foreign owner or multiple
matching pods still fails before mutation.

After the exact, retry-safe namespace handoff, the command requires exactly one
ready AMF, SMF, and UPF pod. A missing or unready network function triggers one
guarded reconciliation of the pinned `fiveg_ansible` Open5GS roles. That wrapper
first reconciles the dedicated Python runtime on the core node from the exact
`remote_python` package versions in `dependencies.lock.yml`. The runtime action
is limited to `core_node`; it does not prepare the RAN node or rebuild the
cluster. The wrapper then uses the default R2Lab profile for `qfit07`, the locked
Open5GS source commit, digest-addressed images, the existing f2/f3 Kubernetes
cluster, and the current run-owned namespace. It does not execute OAI, srsRAN,
N300, qfit power, POS, or reservation roles. It removes only the pinned deferred
`smf2` and `upf2` objects before reconciling the selected `smf1` and `upf1`.
Current R2Lab and SLICES authority is refreshed around both software mutations,
and all three selected core functions are observed again before acceptance
evidence is written.

```text
python -m synthran r2lab foundation \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --run-id "$R2LAB_RUN" \
  --previous-run-id "$PREVIOUS_R2LAB_RUN" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

Success writes `physical-run.json` beside the R2Lab run manifest and advances
the immutable acceptance record through Open5GS. When reconciliation was needed,
its sanitized manifest and log are written below `open5gs-foundation/`. The next
stage is then gNB/N2. Unknown, malformed, foreign-owned, or still-unhealthy state
fails without creating acceptance evidence.

## Stopped gNB staging and N2 proof

Sync only the two physical configuration dependencies. This leaves unrelated
checkouts, including a locally modified Contiki-NG tree, untouched:

```text
python -m synthran deps sync \
  --name fiveg_ansible \
  --name srsran_helm
```

The gNB command boundary reuses the network bindings from the currently stopped
physical Helm release, renders the pinned chart in an isolated workspace, checks
the locked Helm version, packages deterministic artifacts, and stages the exact
Deployment at zero replicas. Explicit network bindings remain available as an
all-or-none override when no stopped release exists.

```text
python -m synthran r2lab gnb-stage \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --run-id "$R2LAB_RUN" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS" \
  --json
```

Success binds the package, values, and render digests into `physical-run.json`.
The start command then refreshes both authority domains, proves zero existing
gNB pods, starts exactly one ready pod, and polls for a current N2 association.

```text
python -m synthran r2lab gnb-start \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --run-id "$R2LAB_RUN" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS" \
  --json
```

The gNB commands discover the one active owner reservation and the one common
allocation for `sopnode-f2` and `sopnode-f3`. The identifier options can still
pin exact records when desired; ambiguous or split ownership fails closed.

An unsuccessful N2 proof requests an exact scale-to-zero recovery. `r2lab
release` also detects a bound gNB start and proves that exact Deployment is at
zero replicas and zero pods before it powers off the qfit and N300 or releases
the local resource claim.

## Completion criteria

The physical path is complete only when one immutable authorized run proves:

1. the pinned R2Lab RF source and singleton gNB/N2 state;
2. qfit readiness, cell acquisition, registration, and PDU state;
3. bounded `wwan0` user-plane traffic;
4. the physical workload through the R2Lab handoff;
5. exact reverse-order cleanup for every selected resource.

The detailed historical run records remain in `docs/r2lab-smoke-002.md` and
`docs/r2lab-smoke-002-development-log.md`. Current subsystem contracts are in
`docs/r2lab-code-architecture.md`, `docs/r2lab-physical-adapter.md`,
`docs/r2lab-runtime-verification.md`, and `docs/r2lab-ue-activation.md`.
