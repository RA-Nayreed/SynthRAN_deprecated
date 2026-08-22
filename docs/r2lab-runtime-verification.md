# R2Lab physical runtime verification

This document records the read-only runtime verification layer added after the `r2lab-smoke-002` investigation. It records both the implementation and the evidence that led to it. It does not change the historical result of smoke 002: that run still failed at UE cell acquisition and never reached registration, PDU session, user plane, or the SynthRAN workload.

## Why this is one runtime subsystem

The R2Lab implementation was previously over-decomposed into many one-purpose modules. That structure was consolidated into cohesive subsystems. Runtime verification is intentionally one additional cohesive subsystem rather than a new file for every probe: it owns the read-only observations that occur after an artifact-bound singleton gNB has started and before a physical path may be accepted.

The boundary is:

```text
reviewed/staged physical artifact
  -> exactly one started gNB
  -> current gNB/N2 proof
  -> qfit management proof
  -> qfit cell/registration/PDU observation
  -> optional wwan0-bound user-plane proof
  -> workload eligibility
```

The module performs no R2Lab power mutation, UE attach, Helm operation, Kubernetes scale operation, or automatic reservation action.

## Discovery: the stock Quectel inspection helper is not safe evidence

While implementing the qfit executor, the public R2Lab/OAI Quectel utilities were inspected instead of assuming that their diagnostic helpers were safe to call from SynthRAN.

The reviewed `quectel-utils/check-ue` script opens `/dev/ttyUSB2` and its first diagnostic is `AT+CIMI`, labelled `Get IMSI`. It then prints the returned modem text. That makes the helper unsuitable for a SynthRAN observation path whose output can become evidence.

### Consequence

SynthRAN does not call `check-ue` for runtime acceptance. The runtime verifier has an explicit AT-command allow-list containing only:

```text
AT+QNWINFO
AT+C5GREG?
```

The raw replies exist only long enough to be classified. Persistent evidence receives only categorical state such as `acquired-nr-sa`, `registered`, `attached`, or `unknown`.

There is deliberately no generic `run arbitrary AT command` API in the acceptance path.

## Discovery: the qfit MBIM/interface contract is already visible in the upstream utilities

The public `quectel-utils/start.sh` implementation was inspected to verify the actual device/interface contract used by the RM500Q-GL workflow. It uses:

```text
/dev/cdc-wdm0
wwan0
mbimcli --set-radio-state=on
mbimcli --attach-packet-service
mbimcli --connect ...
mbim-set-ip.sh
```

This also confirms why smoke 002's `start.sh -F internet -q` failure happened at a meaningful boundary: the script had already turned software radio on and then timed out while attaching packet service.

### Consequence

The new observation executor uses only the read-only portions needed to classify existing state:

```text
mbimcli -p -d /dev/cdc-wdm0 --query-packet-service-state
ip -o link show dev wwan0
ip -o -4 addr show dev wwan0
```

It does **not** call `start.sh`, `--attach-packet-service`, `--connect`, `mbim-set-ip.sh`, or any other operation that would create a PDU session. Observation and attach remain separate responsibilities.

## qfit transport boundary

The qfit hosts live behind the R2Lab/Faraday control boundary. The older upstream automation demonstrates the topology by SSHing from Faraday to `root@<UE>`, but it disables host-key checking. SynthRAN does not copy that behavior.

The runtime verifier uses:

1. the existing strict SynthRAN SSH boundary to Faraday;
2. a second SSH command from Faraday to exactly the selected reviewed qfit;
3. `BatchMode=yes`;
4. `StrictHostKeyChecking=yes`;
5. no `accept-new`, no `/dev/null` known-host bypass, and no password path.

If Faraday cannot verify the qfit host key, the observation fails closed. No acceptance stage is advanced merely because management transport was attempted.

## qfit runtime evidence

The executor independently observes four dimensions:

- NR cell visibility from `AT+QNWINFO`;
- 5G registration state from `AT+C5GREG?`;
- MBIM packet-service state;
- IPv4 presence on `wwan0`.

These are reduced through the existing `QfitRuntimeEvidence` classifier. The acceptance implications remain ordered:

```text
cell acquired
  -> registered
  -> packet service attached + wwan0 IPv4
  -> PDU session accepted
```

A PDU session still does not prove user-plane traffic.

Any probe transport failure or malformed/contradictory response becomes `unknown`; it is never interpreted as a clean or passing state.

## Discovery: N2 proof must belong to the current singleton pod

Smoke 002 proved that broad Kubernetes rollout behavior is unsafe for a single physical N300 because a replacement pod can overlap a terminating pod. The deployment layer already prevents that by enforcing zero pods before configuration and exactly one ready pod after start.

The runtime verifier extends that ownership rule to N2 evidence. It does not accept an arbitrary historical log line. Before reading gNB logs it rechecks:

- the run-owned `open5gs` namespace;
- the run label on `deployment/srsran-gnb`;
- the staged package/render bindings on that Deployment;
- desired replicas equal to one;
- exactly one matching gNB pod;
- that pod is Running/Ready and not terminating.

Only then are the current pod's bounded logs inspected for affirmative N2/NGAP/AMF connection evidence. The raw log and pod name are not persisted.

The log classifier is intentionally conservative. Failure/error/timeout/disconnect lines cannot satisfy the proof. Unknown wording remains `not-observed` and may be expanded only after another controlled run provides concrete evidence for the exact srsRAN build.

## Authority is refreshed during observation

A successful gNB start does not create permanent authority. Before runtime verification, SynthRAN reuses `authorize_physical_start()` to re-prove:

- the active local run claim;
- the active R2Lab lease;
- the exact selected N300 state as ON;
- the claim digest used by the started gNB evidence.

That authority is refreshed again before qfit observation and before optional user-plane proof. If the claim changes, the run cannot continue through acceptance.

## User-plane proof

When a literal IPv4 peer is explicitly supplied and the PDU-session stage has passed, the existing physical user-plane probe is executed through the selected qfit host. It is bound to `wwan0` and has bounded packet count/timeouts.

Persistent evidence stores only:

- interface name;
- packet counts;
- a SHA-256 fingerprint of the peer;
- success/failure state.

The peer address and raw ping output are not persisted.

## Persistent acceptance behavior

`execute_physical_runtime_verification()` advances the existing ordered `PhysicalRunEvidence` state machine and can atomically write `physical-run.json` after each stage.

A failed gNB/N2 proof stops before UE management. A failed UE-management proof stops before modem inspection. A failed cell-acquisition proof leaves registration/PDU/user-plane/workload as `not-reached`. Successful cell, registration and PDU observations still leave user plane unaccepted until the separate traffic probe succeeds.

This preserves the same truthful semantics used to record smoke 002.

## Tests added before another RF run

The runtime regression tests use injected runners only; they do not contact R2Lab or SLICES. They verify that:

- the qfit command set contains no IMSI query, `check-ue`, attach, connect, or `start.sh` path;
- strict host-key checking is retained for nested qfit SSH;
- raw PLMN/address data disappears at the classifier boundary;
- an unreviewed qfit identifier is rejected before transport;
- gNB/N2 proof requires current run/artifact binding and one ready pod;
- a changed render binding fails closed;
- failure wording cannot accidentally satisfy N2 acceptance;
- the complete observational sequence can advance through user plane while the persisted evidence contains none of the raw modem address, peer address, or current pod name.

No live radio or UE mutation was required to implement or test this layer.

## Remaining live boundary

This code makes the next physical run safer, but it does not make smoke 002 successful retroactively. The next controlled run still has to establish whether the reviewed 60 MHz / 2x2 physical srsRAN candidate is actually acquired by the selected qfit and then prove registration, PDU session, user plane, and finally the SynthRAN workload.
