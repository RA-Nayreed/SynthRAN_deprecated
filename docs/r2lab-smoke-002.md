# R2Lab physical smoke run 002

This document records the second live physical R2Lab smoke run and the engineering conclusions that came from it. It is intentionally evidence-oriented: accepted facts are separated from hypotheses so later code does not silently turn one experimental interpretation into product behavior.

## Purpose

The run extended the initial resource-control smoke gate into the first end-to-end physical 5G bring-up attempt using the reviewed SLICES/R2Lab path:

```text
SLICES/POS nodes
  -> Kubernetes foundation
  -> Open5GS
  -> physical srsRAN gNB
  -> N300
  -> qfit UE
```

The goal was not yet to run the full SynthRAN research workload. The immediate acceptance ladder was:

```text
resource authority
  -> exact hardware preparation
  -> Kubernetes foundation
  -> Open5GS
  -> gNB / N2
  -> UE management
  -> cell acquisition
  -> registration
  -> PDU session
  -> user plane
```

The run reached gNB/N2 acceptance and UE management, but did not reach UE cell acquisition.

## Safety boundary used during the run

The live work followed these rules throughout:

- no automatic R2Lab booking;
- no R2Lab password storage;
- strict public-key SSH to Faraday;
- exact selected-resource mutations only;
- no `rhubarbe all-off`;
- no broad `rhubarbe bye` cleanup;
- no upstream top-level deployment helper that performs global cleanup;
- no broad SLICES allocation release while the run was still being diagnosed;
- physical mutations were preceded by an active-lease authority check;
- ambiguous hardware state was treated as unknown rather than inferred from a failed command.

These rules remain product requirements, not one-off operator conventions.

## Foundation and core acceptance

The SLICES/POS foundation was prepared on two run-owned nodes without reallocating them during the live debugging cycle. Kubernetes, Flannel, OpenEBS, the required bridges, Multus networking, and the inter-node GRE path were accepted before the core was deployed.

Open5GS was deployed as run-owned Kubernetes resources. The core reached a stable state with MongoDB and the required 5G core network functions running without restarts. The AMF N3 attachment, run-owned namespace labeling, subscriber preparation, and image locks were checked before moving to radio work.

The core was intentionally left running while radio diagnosis continued so an RF failure would not be confused with repeated core reconstruction.

## Physical gNB acceptance

The physical srsRAN gNB was deployed against the N300 using the UHD-backed image rather than the accepted virtual RFSIM image.

Accepted observations:

- the N300 was reachable from the RAN node;
- the gNB acquired the UHD device;
- the gNB pod remained stable after controlled startup;
- SCTP/N2 association with Open5GS AMF was established;
- the gNB was able to remain running at the reviewed TX/RX gain range;
- there was no evidence of a gNB process crash as the reason for UE failure.

This means the run passed the `N300 -> gNB -> AMF/N2` portion of the acceptance ladder.

## Discovery: rolling restart is unsafe for a single N300

During one Helm configuration change, Kubernetes used its normal rolling deployment behavior. The replacement pod started before the previous gNB pod had fully released the UHD device, so two pods briefly competed for the single physical N300.

The run was recovered by changing the operational sequence to:

```text
scale gNB to zero
  -> wait until no gNB pod remains
  -> allow UHD claim release
  -> apply the new configuration
  -> start one gNB pod
```

### Product consequence

SynthRAN must not rely on an ordinary overlapping rolling deployment for a single physical SDR. Physical gNB updates need an explicit non-overlapping lifecycle, for example a `Recreate` strategy or a controller-enforced stop/wait/start sequence with evidence that the previous UHD owner is gone.

## qfit07 management and modem preparation

The qfit UE used for the successful management path was `qfit07` with a Quectel RM500Q-GL modem in MBIM mode.

The modem was prepared for NR5G standalone operation and the `internet` DNN. Management reachability and the MBIM control device were accepted before attach was attempted.

The first packet-service attach did not complete. The helper enabled the software radio and then timed out while requesting packet-service attachment. No PDU session or IPv4 user-plane address was established.

Rather than repeatedly resetting the modem, the run switched to read-only RF visibility diagnostics.

## RF visibility observations

While the gNB was stable, the UE repeatedly reported:

- `No Service` / searching state;
- 5G registration state equivalent to not registered and not camping;
- packet service detached;
- unavailable/sentinel signal values;
- no IPv4 address on the modem interface;
- zero results from active nearby-cell scans.

Three srsRAN carrier settings were tried during the run:

1. the initial approximately 3.6 GHz configuration;
2. an approximately 3.405 GHz configuration;
3. an approximately 3.31968 GHz configuration derived from a known R2Lab OAI reference value.

At each setting the qfit modem remained unable to report a visible NR cell, and the gNB did not show a real UE random-access attempt.

### What this proves

It proves that **these three tested srsRAN configurations did not produce a cell that qfit07 could acquire**.

It does **not** yet prove that the physical N300-to-qfit RF path is defective.

## Discovery: the final frequency experiment was not a faithful copy of the OAI reference

After the RF experiments, the known-good R2Lab OAI configuration was inspected more carefully. That reference distinguishes at least:

- `absoluteFrequencySSB`;
- `dl_absoluteFrequencyPointA`;
- carrier bandwidth;
- two transmit and two receive paths.

The live srsRAN experiment had treated one OAI SSB-related value as a candidate srsRAN carrier `dl_arfcn` and used a narrower/SISO-oriented profile. Those are not equivalent configurations.

### Product consequence

The three failed scans must remain evidence of the configurations that were actually tested, not evidence that R2Lab RF connectivity itself is broken.

Before another physical transmit attempt, SynthRAN needs an offline physical-radio validation step that distinguishes carrier frequency, SSB placement, bandwidth, antenna count, and COTS-UE-specific settings. A candidate configuration should be rejected before hardware mutation if those values are internally inconsistent or are merely copied between OAI and srsRAN fields with different semantics.

The exact srsRAN profile to use next remains **unaccepted** until rendered output confirms the intended carrier and SSB placement.

## Discovery: Rhubarbe PDU mutation exit codes are not state truth

The N300 cleanup exposed a provider semantic that the original smoke-gate code did not model correctly.

The exact power-off command reported the N300 as `OFF`, but the mutation command returned status `1`. An immediate exact-resource status query again reported the N300 as `OFF`.

Therefore this rule is now required:

```text
mutation return code != resulting hardware state
```

For PDU-backed resources, SynthRAN must perform an exact-resource state query after a mutation and parse the provider's textual `ON`/`OFF` observation. The mutation return code is retained as diagnostic evidence but is not sufficient to declare the transition failed.

This discovery is now codified in `synthran/network/r2lab_power.py` with regression coverage.

## Discovery: timeout means unknown, not failed-and-clean

A physical command can time out after the provider has already acted. Consequently, a transport timeout cannot safely be translated into a known power state.

The controller must:

- preserve the run claim on mutation timeout or ambiguous state;
- record the stage as unresolved;
- query exact current state when it is safe to do so;
- avoid widening cleanup scope;
- remove the claim only after every run-owned physical resource is proven clean.

This is especially important for release: failure of the first cleanup action must not tempt the controller to call a global cleanup helper.

## Exact cleanup performed

The run was shut down in dependency order.

### UE

The modem helper was stopped first. Post-stop evidence showed:

- software radio off;
- packet service detached;
- modem interface down;
- no IPv4 address.

The exact qfit resource was then powered off. The corresponding R2Lab node reported off state afterward.

### gNB

The srsRAN deployment was scaled to zero and the controller waited until the gNB pod count was exactly zero. The Open5GS core was deliberately left untouched at that point.

The srsRAN Helm release was then removed only after proving that its desired replica count and live gNB pod count were both zero.

### N300

With no gNB pod remaining, the N300 was powered off. The provider mutation returned a non-zero status, but the immediate exact PDU status query reported:

```text
N300: OFF
```

That textual observation was treated as the accepted hardware state.

### Core

After physical hardware cleanup was proven, ownership of the Open5GS namespace and its substantive resources was inspected. The namespace, workloads, services, configuration, network attachments, and MongoDB PVC were all labeled as belonging to this run. No Open5GS cluster role or cluster-role binding was found.

The run-owned Open5GS namespace was therefore removed as one exact ownership boundary. The MongoDB PV used reclaim policy `Delete` and was subsequently absent.

### Foundation

The Kubernetes foundation was intentionally preserved. After core deletion:

- both SLICES nodes remained `Ready`;
- Flannel remained deployed;
- OpenEBS remained deployed;
- the SLICES allocation was not broadly freed during the diagnostic session.

## Evidence hashes

Two local evidence artifacts were hashed during cleanup:

```text
final-cleanup-state.txt
sha256: 8008568859c16edb6ef1b6870d09299f8a1cb76251685d1f9f73e0c2c422637e

rf-visibility-blocker.txt
sha256: bf83d7f326a56846eba2cfc1404aecb3ec44c043091f6bf1641220d1dace9eb0
```

The generated live evidence stays outside tracked source. This document records only the non-secret conclusions required to reproduce and review the engineering decisions.

## Acceptance result

The run result is:

```text
resource authority          PASS
SLICES/POS foundation       PASS
Kubernetes                  PASS
Open5GS                     PASS
gNB <-> AMF N2              PASS
N300/gNB runtime            PASS
qfit management             PASS
UE cell acquisition         FAIL / NOT REACHED
5G registration             NOT REACHED
PDU session                 NOT REACHED
user plane                  NOT REACHED
full SynthRAN workload      NOT REACHED
exact physical cleanup      PASS
```

The run must therefore be retained as a **failed physical acceptance run with successful lower-layer bring-up and cleanup**, not as a successful R2Lab backend acceptance.

## Code work derived from the run

The live evidence creates the following implementation requirements for the current R2Lab smoke-gate work:

1. parse exact PDU status text and do not equate mutation exit status with hardware state;
2. retain claims on timeout, conflicting state, or missing state evidence;
3. make release recovery stage-aware and exact-resource only;
4. support qfit resources as first-class reviewed UE selections;
5. encode non-overlapping physical gNB restart semantics;
6. separate physical radio profiles from RFSIM/srsUE assumptions;
7. add offline carrier/SSB/bandwidth/antenna validation before transmit;
8. classify UE acquisition, registration, PDU session, and user plane as separate acceptance stages;
9. preserve a sanitized evidence trail for every live stage;
10. keep the accepted RFSIM path green while R2Lab work accumulates on `r2lab-integration`.

## Branch model

For the remainder of this integration effort, the intended naming model is:

```text
main
  |
  +-- r2lab-integration          temporary R2Lab integration main
        |
        +-- r2lab-smoke-gate     current checkpoint / PR
        +-- r2lab-*              future checkpoints
```

The existing GitHub pull request was created before the prefix convention changed, so its remote head may retain the older name until that PR is completed. New R2Lab branch names and documentation should not introduce the old prefix again.
