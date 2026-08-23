# R2Lab integration

`r2lab-integration` is the temporary integration branch for physical R2Lab work.
`main` remains the accepted RFSIM truth until an immutable physical run completes
the acceptance sequence and cleanup review.

## Current truth

The virtual Open5GS, srsRAN, srsUE, and RFSIM path remains unchanged.

Historical R2Lab runs established exact-resource authority, the SLICES/POS and
Kubernetes foundation, Open5GS, an N300-backed gNB, N2/SCTP, qfit provisioning,
UHD/IQ transport, and exact cleanup. They also exposed an incorrect transcription
of the reviewed OAI radio profile. The corrected R2Lab profile is:

| Setting | Value |
|---|---:|
| Band | 78 |
| Carrier-center ARFCN | 621312 |
| SSB ARFCN | 621312 |
| Point-A ARFCN | 620040 |
| Resource blocks | 106 |
| SCS | 30 kHz |
| Nominal bandwidth | 40 MHz |
| TX/RX paths | 2/2 |

The earlier `162 PRB / 60 MHz / 621984` candidate is rejected by validation.
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
- FIT image loading remains an explicit authorized operation.
- Raw modem output and subscriber identifiers are not persisted.

## Physical runtime

The deployment boundary renders the reviewed Open5GS/f2 and srsRAN/f3 topology,
pins the UHD image by digest, enforces `Recreate`, validates the radio profile,
and uses exact CPU and memory requests and limits for predictable gNB placement.

The qfit path maps logical resources such as `qfit07` to physical FIT hosts such
as `fit07` in one controller function. All nested SSH commands use strict host
verification. Runtime observation is read-only; modem mutation is isolated in
the UE activation boundary.

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

## Completion criteria

The physical path is complete only when one immutable authorized run proves:

1. the corrected RF profile and singleton gNB/N2 state;
2. qfit readiness, cell acquisition, registration, and PDU state;
3. bounded `wwan0` user-plane traffic;
4. the physical workload through the R2Lab handoff;
5. exact reverse-order cleanup for every selected resource.

The detailed historical run records remain in `docs/r2lab-smoke-002.md` and
`docs/r2lab-smoke-002-development-log.md`. Current subsystem contracts are in
`docs/r2lab-code-architecture.md`, `docs/r2lab-physical-adapter.md`,
`docs/r2lab-runtime-verification.md`, and `docs/r2lab-ue-activation.md`.
