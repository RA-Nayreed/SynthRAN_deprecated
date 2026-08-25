# R2Lab integration

R2Lab is SynthRAN's physical-radio backend. It is not a separate product interface: physical operations use the same installed `synthran` command as the accepted RFSIM path, with additional lease, radio, modem, and hardware-safety boundaries.

The virtual Open5GS, srsRAN, srsUE, and RFSIM path remains the accepted reference implementation. Physical acceptance is progressive and evidence-gated; a proven resource, gNB, or N2 stage does not imply that UE registration, PDU, user plane, workload, or cleanup is accepted.

## Current physical boundary

Historical R2Lab runs established exact-resource authority, the SLICES/POS and Kubernetes foundation, Open5GS, N300-backed gNB operation, N2/SCTP, qfit provisioning, UHD/IQ transport, and exact cleanup. Current live work has also observed NR5G-SA registration and an IPv4 PDU address on qfit. Those observations do not replace one immutable end-to-end acceptance record.

The physical gNB consumes the exact N300 values selected by the pinned R2Lab adapter:

| Setting | Value |
| --- | ---: |
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

The source file is copied, hashed, rendered, transferred, and verified without radio-field overlays. A rendered value that differs from the pinned source is rejected before cluster mutation.

## Physical package

The physical implementation is contained in `synthran/r2lab/`:

```text
controller.py   resource authority, provider commands, prepare and release
provider.py     provider-state parsing and verified transitions
radio.py        sanitized modem and user-plane state
runtime.py      read-only physical observation
deployment.py   physical chart, staging, start, and render validation
gnb.py          stopped staging and gNB/N2 acceptance
n2.py           N2 evidence parsing
readiness.py    FIT/qfit readiness
ue.py           modem activation, rollback, user-plane proof, workload handoff
acceptance.py   ordered immutable physical evidence
guards.py       physical mutation preconditions
handoff.py      exact namespace ownership handoff
foundation.py   SLICES, Kubernetes, and Open5GS reconciliation/proof
```

There is no second user-facing R2Lab executable and no duplicate R2Lab surface under `synthran.network`.

## Safety rules

- Exact provider observation is state truth; return codes are diagnostic.
- Every mutation is bound to the active run, current authority, and selected resources.
- Broad cleanup, guessed identifiers, and global radio or UE cleanup are prohibited.
- A claim is removed only after every selected resource is proven clean.
- The N300 is a singleton owner: zero matching gNB pods must be proven before release or reconfiguration, and exactly one ready pod must be proven at start.
- Physical staging is immutable, digest-bound, and stopped at zero replicas.
- FIT image loading is scoped to the selected physical UE and a freshly verified lease.
- Raw modem output and subscriber identifiers are not persisted.

## Acceptance order

Mutation order may differ from evidence order, but accepted physical evidence advances only through the following sequence:

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

A failed stage blocks every later stage. User-plane and workload acceptance refresh current authority and reprove the physical path rather than trusting stale evidence.

## Resource preparation

The R2Lab resource controller binds one selected radio and one selected UE to a run. Preparation reuses the active lease, verifies current provider state, mutates only exact selected resources, and records sanitized evidence.

Preview the resource plan:

```bash
synthran r2lab plan \
  --radio n300 \
  --ue qfit07 \
  --run-id "$R2LAB_RUN"
```

Run the read-only environment doctor before physical mutation:

```bash
synthran r2lab doctor \
  --radio n300 \
  --ue qfit07 \
  --run-id "$R2LAB_RUN"
```

Preparation and release remain exact-resource operations. Use the selected slice, identity, and known-hosts inputs required by the active environment; never substitute a broad power or cleanup command.

## Foundation acceptance

After the selected radio and UE are prepared, foundation acceptance verifies both authority domains, both selected Kubernetes nodes, a stopped physical gNB, and Open5GS namespace ownership. Unknown, malformed, split, expired, or foreign ownership fails before mutation.

```bash
synthran r2lab foundation \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --run-id "$R2LAB_RUN" \
  --previous-run-id "$PREVIOUS_R2LAB_RUN" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

The namespace handoff can recover only exact unowned legacy state that satisfies its bounded safety checks. Open5GS reconciliation uses pinned source and image identities and is limited to the selected core functions. It does not execute radio, UE, POS, or reservation roles.

Success writes `physical-run.json` beside the R2Lab run manifest and advances the immutable acceptance record through Open5GS.

## Stopped gNB staging

Synchronize only the physical configuration dependencies when unrelated managed checkouts must remain untouched:

```bash
synthran deps sync \
  --name fiveg_ansible \
  --name srsran_helm
```

The staging boundary reuses current physical network bindings, renders the pinned chart in an isolated workspace, validates the locked Helm artifact and source values, and stages the exact Deployment at zero replicas.

```bash
synthran r2lab gnb-stage \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --run-id "$R2LAB_RUN" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS" \
  --json
```

Success binds package, values, and render digests into the physical evidence before any singleton start.

## gNB and N2 acceptance

The start boundary refreshes current authority, proves zero existing gNB pods, starts exactly one ready pod, and requires a current N2 association. Initial convergence and consecutive stability use separate bounded poll budgets.

```bash
synthran r2lab gnb-start \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --run-id "$R2LAB_RUN" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS" \
  --json
```

AMF fallback evidence is accepted only while the current run-owned gNB pod is ready and within the current pod lifetime. Failure diagnostics are sanitized before persistence, and unsuccessful acceptance requests exact scale-to-zero recovery.

## UE and user plane

The physical UE path separates management readiness from software-radio state, packet attachment, PDU configuration, and user-plane proof. For qfit, the expected progression is:

```text
current authority and gNB/N2 proof
-> physical UE management proof
-> sanitized current modem observation
-> software radio on
-> packet attach request
-> MBIM connection and IPv4 configuration
-> current registration and PDU observation
-> route-bound user-plane proof
```

The accepted physical data path must prove traffic through the selected data interface, normally `wwan0` for qfit, rather than accepting generic host reachability.

## Workload parity

The physical backend is complete only when the canonical deterministic IoT workload crosses the accepted physical user plane and produces the same experiment-level evidence semantics as RFSIM. Backend-specific interface names and hardware identifiers must not leak into the scientific data contract.

## Completion criteria

One immutable authorized physical run must prove:

1. exact current resource authority and selected N300/qfit ownership;
2. pinned foundation and Open5GS state;
3. singleton gNB and stable current N2;
4. UE readiness, cell acquisition, registration, and PDU state;
5. bounded route-specific physical user-plane traffic;
6. the deterministic IoT workload through the physical handoff;
7. canonical experiment data and provenance;
8. exact reverse-order cleanup for every selected resource.

Current accepted evidence and measured limitations belong in `docs/results.md`. Focused implementation contracts are documented in `docs/r2lab-code-architecture.md`, `docs/r2lab-physical-adapter.md`, `docs/r2lab-runtime-verification.md`, and `docs/r2lab-ue-activation.md`.
