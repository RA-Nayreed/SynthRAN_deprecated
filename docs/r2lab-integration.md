# R2Lab integration branch

## Branch model

`r2lab-integration` is the temporary integration main for physical R2Lab work while `main` remains the accepted RFSIM truth. Physical checkpoints merge into `r2lab-integration` first; `main` is not advanced until physical acceptance is complete.

The current checkpoint is `r2lab-smoke-gate`. The already-open pull request retains its historical remote head name until that PR is complete, but new branch names, documentation, and plans use the `r2lab-*` convention without the old prefix.

```text
main                         accepted RFSIM truth
  |
  +-- r2lab-integration      temporary physical-integration main
        |
        +-- r2lab-smoke-gate current PR/checkpoint
        +-- r2lab-*          future physical checkpoints
```

## Current physical truth

The accepted virtual Open5GS + srsRAN + srsUE + RFSIM path remains unchanged.

`r2lab-smoke-002` established live evidence for:

- active R2Lab authority and exact-resource control;
- SLICES/POS preparation and a two-node Kubernetes foundation;
- Open5GS core deployment;
- an N300-backed srsRAN gNB;
- gNB-to-AMF N2/SCTP establishment;
- managed qfit07 reachability and modem preparation;
- exact qfit, gNB, N300, core, and namespace cleanup.

It did **not** establish UE cell acquisition, registration, PDU session, user plane, or the physical research workload. Post-run inspection also showed that the final smoke-002 frequency test had reused an OAI SSB ARFCN as an srsRAN carrier-center ARFCN, so smoke 002 was not evidence that the R2Lab RF path itself was defective.

`r2lab-smoke-003` progressed further and established:

- active R2Lab lease and exact resource authority;
- SLICES/POS foundation and Kubernetes;
- Open5GS;
- immutable stopped physical staging;
- one N300-backed srsRAN gNB with no overlapping owner;
- gNB-to-AMF N2/SCTP, proven from AMF-side evidence;
- fit07 provisioning with the reviewed `mbim-quectel` image;
- strict fit07 SSH;
- external USB power and RM500Q-GL enumeration;
- SIM readiness and AT transport;
- Quectel MBIM initialization and software-radio enable;
- active UHD/IQ transport between the gNB pod and the N300.

The `ru1` counters increased by about 1.51 GB TX and 1.52 GB RX over three seconds, so the N300 transport was not stalled.

With the modem software radio proven ON, qfit07 reported:

```text
+QNWINFO: No Service
+C5GREG: 0,0
```

Cell acquisition therefore failed and the acceptance ladder correctly blocked registration, PDU session, user plane, and workload.

Smoke 003 then exposed an offline source-of-truth error in the branch: the OAI reference had been transcribed as 162 PRBs even though the actual reviewed R2Lab N310 source uses 106 PRBs.

## Package architecture

The implementation lives in one cohesive package:

```text
synthran/r2lab/
  __init__.py
  controller.py
  provider.py
  radio.py
  deployment.py
  acceptance.py
  runtime.py
  ue.py
```

`synthran/network/r2lab.py` remains only as the stable compatibility import used by existing CLI/callers.

The architecture and the reason for the consolidation are recorded in `docs/r2lab-code-architecture.md`.

## Implemented safety semantics

### Provider state

Exact provider observation is the state truth. Mutation and status return codes are diagnostic evidence; the exact selected-resource state decides whether a transition is accepted.

A mutation timeout does not imply no mutation. The controller still issues the exact provider-state query after a mutation transport failure. Missing or contradictory evidence remains unknown.

### Claims and cleanup

A workspace claim is removed only when every selected physical resource is proven clean. An unresolved UE cleanup does not trigger global cleanup and does not prevent an independently authorized exact N300 cleanup. `all-off` and broad `rhubarbe bye` remain forbidden.

### qfit provider power

qfit resources use their own provider path. `qfit on|off qfitNN` is followed by independent `rhubarbe status N` verification. qfit provider state is not inferred from the helper return code.

### Physical gNB ownership

An N300 is a singleton hardware owner. The physical lifecycle therefore performs:

```text
scale exact gNB deployment to zero
  -> prove zero matching pods, including terminating pods
  -> allow UHD release
  -> apply reviewed configuration
  -> scale to one
  -> prove exactly one Running/ready gNB pod
```

Ambiguous startup or overlapping pods requests exact scale-to-zero recovery and fails closed.

### Radio semantics and corrected R2Lab source

The reviewed R2Lab OAI N310 source `gnb.band78.sa.fr1.106PRB.2x2.usrpn310.conf` records:

- SSB ARFCN `621312`;
- Point-A ARFCN `620040`;
- `106` PRBs at `30 kHz` SCS;
- 2 TX and 2 RX paths.

The occupied resource grid is:

```text
106 PRB x 12 subcarriers x 30 kHz = 38.16 MHz
half grid = 19.08 MHz
19.08 MHz / 15 kHz FR1 raster = 1272 steps
620040 + 1272 = carrier-center ARFCN 621312
```

The corrected offline candidate is therefore:

```text
band 78
carrier-center ARFCN 621312 (~3319.68 MHz)
expected SSB ARFCN 621312
nominal bandwidth 40 MHz
common SCS 30 kHz
2x2 antennas
```

Carrier-center, SSB, and Point-A remain distinct typed semantics even though the corrected carrier-center and SSB happen to have the same numeric ARFCN.

The previous `162 PRB / 60 MHz / carrier 621984` candidate was a transcription error discovered by smoke 003 and is now explicitly rejected by regression/staging gates.

This corrected candidate is still not live accepted until a new immutable physical run proves cell acquisition.

## Physical deployment boundary

The physical backend is separate from the accepted RFSIM `fiveg_ansible` adapter.

The R2Lab deployment subsystem provides:

- a narrow Open5GS/f2 + srsRAN/f3 + N300 deployment plan;
- canonical physical srsRAN values derived from the reviewed reference;
- a dedicated digest lock for the UHD gNB image;
- a guarded overlay for the exact pinned srsRAN Helm chart;
- values-driven zero replicas and `Recreate` strategy;
- digest-addressed physical gNB image rendering;
- isolated chart workspace hashing;
- offline `helm template` validation;
- deterministic chart/value packaging;
- strict SLICES authority checks and a stopped-only cluster staging boundary;
- a non-overlapping singleton start lifecycle;
- fresh R2Lab claim/lease/N300 binding at start;
- immutable staged/start artifact hashes.

The render validator now derives its expected carrier/bandwidth/antenna values from `r2lab_oai_aligned_candidate()` rather than hard-coding a second set of radio numbers. Helm validation compares rendered values with the reviewed chart intent. Staging independently rejects render evidence that does not match the reviewed R2Lab profile before any cluster write.

The stopped staging operation is intentionally not a radio start. It requires fresh reservation/allocation authority, strict known-host SSH, a run-owned namespace, matching artifact hashes, the locked Helm version, zero desired replicas, and zero gNB pods. It stages only the reviewed artifact at `replicas=0`.

## Read-only physical runtime boundary

`synthran/r2lab/runtime.py` provides the live observation path after the gNB starts:

- current run/artifact-bound gNB/N2 proof;
- current qfit management proof;
- allow-listed cell/registration/packet/IP observation;
- strict nested qfit SSH;
- no `AT+CIMI`, `check-ue`, attach, connect, or `start.sh` in the read-only path;
- immediate reduction of raw output into sanitized categorical evidence;
- optional bounded `wwan0` user-plane proof for an already-established PDU session.

The runtime verifier does not mutate modem, radio power, Helm, or Kubernetes state.

One smoke-003 gap remains: gNB-side log parsing did not prove N2 even though the AMF showed the exact gNB N2 peer accepted. The composed smoke automation should accept sanitized AMF-side evidence bound to the expected gNB N2 address instead of producing that false negative.

## Controlled qfit activation boundary

`synthran/r2lab/ue.py` owns the mutating COTS-UE/session path.

The current activation contract is intentionally narrow:

```text
DNN       internet
MBIM      /dev/cdc-wdm0
interface wwan0
session   0
IP type   ipv4
```

The activation sequence is gated by current authority and observations. Packet attach/PDU mutation must not occur until cell acquisition and registration are proven.

A non-zero mutation return code is diagnostic. The independently observed state decides whether a transition succeeded.

On unresolved activation failure the exact rollback is software-radio off plus `wwan0` down. Cleanup is accepted only when radio-off, packet-detached, and no-IPv4 observations are all proven. Otherwise activation evidence remains unresolved.

Smoke 003 also showed that basic qfit power/reachability is not sufficient preparation evidence for a COTS modem. A complete physical smoke workflow must separately prove the FIT host is booted, strict SSH is established, external USB power is on, and the RM500Q devices are enumerated. Destructive FIT image loading must remain an explicit operator-approved stage rather than being hidden inside ordinary `prepare`.

## Physical user-plane and workload handoff

After PDU acceptance, the user-plane entry point refreshes R2Lab authority, singleton gNB/N2, qfit management, and current PDU state before executing the bounded `wwan0` traffic proof.

After user-plane acceptance, `execute_physical_workload_handoff()` provides an explicit physical-only handoff. A virtual/RFSIM result cannot satisfy the physical workload stage.

## Physical acceptance model

Acceptance remains ordered:

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

Stages cannot be skipped. A failed stage blocks later acceptance and later stages remain explicitly `not-reached`.

## Automation status

The PR already has deterministic smoke coverage for exact resource control, stopped staging, and authorized singleton gNB start. The acceptance model already blocks advancement after failure.

The remaining product automation is to compose the existing physical stages into one fail-closed smoke workflow/CLI boundary:

```text
active external R2Lab lease
  -> exact resource authority
  -> FIT/qfit readiness
  -> explicitly approved image provisioning when required
  -> strict qfit host trust
  -> external USB + RM500Q enumeration
  -> corrected offline RF-reference validation
  -> immutable stopped staging
  -> exact singleton gNB start
  -> Open5GS + N2 proof
  -> Quectel initialization
  -> software radio ON
  -> cell acquisition
       FAIL => persist failure; no attach/PDU
  -> registration
  -> packet attach + PDU
  -> user-plane proof
  -> physical workload
  -> exact reverse-order cleanup
```

Automatic R2Lab reservation/booking remains prohibited.

## Evidence and development history

Detailed records are maintained in:

- `docs/r2lab-smoke-002.md` — live smoke-002 chronology and acceptance result;
- `docs/r2lab-smoke-002-development-log.md` — how smoke-002 observations became code, including an explicit smoke-003 correction to the earlier RF interpretation;
- `docs/r2lab-physical-adapter.md` — physical chart/adapter investigation and corrected RF reference;
- `docs/r2lab-runtime-verification.md` — read-only qfit/N2 observation design;
- `docs/r2lab-ue-activation.md` — mutating qfit activation, rollback, and workload-handoff design;
- `docs/r2lab-code-architecture.md` — package-consolidation rationale and current subsystem boundaries.

## Remaining work before this checkpoint can merge

The PR remains draft. Remaining work is:

- keep the complete repository unit/privacy workflow green on the current head;
- review PRACH/TDD parity separately from the corrected carrier/bandwidth source;
- compose the live stages into the fail-closed smoke workflow/CLI boundary;
- perform a fresh immutable run with the corrected `106 PRB / 40 MHz / 621312` candidate;
- verify cell acquisition and registration before invoking qfit packet activation;
- verify PDU session and `wwan0` user plane;
- run the physical workload through the explicit handoff;
- perform and review exact cleanup evidence.

Do not merge this checkpoint into `r2lab-integration` until those boundaries are green and the follow-up physical run has been reviewed.
