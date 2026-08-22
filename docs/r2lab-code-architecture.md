# R2Lab code architecture

This note records the structural correction made after the first physical R2Lab implementation pass. It is deliberately separate from the live-run chronology: `docs/r2lab-smoke-002.md` records what happened on the testbed, while this file records how those discoveries are organized in product code.

## Why the structure changed

The first smoke-gate implementation translated each live discovery into a narrowly scoped module. That made individual behaviors easy to isolate initially, but the approach was applied too mechanically. The result was 18 flat `r2lab_*` production modules inside `synthran/network/`.

That layout had two problems at the same time:

- related behavior was fragmented across many tiny modules; and
- the high-level controller was still large, so the fragmentation did not actually make the subsystem easier to understand.

For example, the physical deployment path had been split across separate plan, render, chart, workspace, Helm, artifact, staging, and gNB-lifecycle modules. Understanding one operation required following a chain of imports through most of those files.

The safety decisions were retained. The file decomposition was not.

## How the problem was identified

The issue was caught during an explicit architecture review of the smoke-gate branch after the live work. The branch directory was inspected rather than judging individual functions in isolation. The review showed that most of the R2Lab implementation names shared one prefix but did not form an actual package boundary.

The correction therefore treats R2Lab as a subsystem with coherent internal responsibilities instead of treating every observed behavior as a new top-level network module.

## Current package

R2Lab implementation now lives under:

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

The responsibilities are:

### `controller.py`

Owns public resource/start orchestration and workspace authority:

- resource selection and plan construction;
- strict Faraday SSH boundary;
- read-only doctor checks;
- exact prepare flow;
- exact release flow;
- active run manifests and resource claims;
- fresh R2Lab claim/lease/N300 authorization for physical gNB start.

It coordinates lower-level behavior but does not redefine provider state, radio semantics, or qfit session mechanics.

### `provider.py`

Owns R2Lab provider-facing resource semantics:

- exact PDU state parsing;
- verified PDU transitions;
- qfit resource/node mapping;
- exact qfit provider-state parsing;
- verified qfit power transitions;
- cleanup evidence and claim-release assessment.

This grouping follows the smoke-002 discovery that provider state, not process return code, is the hardware truth.

### `radio.py`

Owns radio and sanitized UE state semantics:

- carrier-center, SSB, and Point-A ARFCN meaning;
- OAI-reference-derived physical candidate validation;
- nominal bandwidth and 2x2 reference checks;
- qfit cell-acquisition state;
- registration state;
- packet-service state;
- IPv4/PDU evidence classification;
- bounded `wwan0` user-plane probe evidence.

These belong together because they describe what the radio/UE state means, not how Kubernetes, R2Lab power control, or MBIM mutation is executed.

### `deployment.py`

Owns the physical gNB deployment pipeline as one subsystem:

- reviewed physical deployment intent;
- canonical srsRAN configuration render;
- pinned Helm-chart binding;
- guarded chart overlay;
- isolated chart workspace;
- offline Helm rendering and validation;
- deterministic chart packaging;
- stopped cluster staging;
- non-overlapping singleton gNB lifecycle;
- exact staged-artifact binding at start.

This is intentionally one coherent deployment boundary. The sequence is easier to inspect from plan through staged artifact without jumping between many sibling `r2lab_physical_*` files.

### `acceptance.py`

Owns the monotonic physical acceptance state machine:

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

It also persists immutable staging/start bindings and ordered acceptance evidence. This remains separate because acceptance is a product-level truth model rather than deployment or modem mechanics.

### `runtime.py`

Owns the read-only live observation boundary after a singleton gNB has started:

- current run/artifact-bound gNB/N2 proof;
- current qfit management reachability;
- allow-listed cell/registration/packet/IP observation;
- strict nested qfit SSH;
- immediate reduction of raw modem/log output into sanitized evidence;
- optional `wwan0` user-plane observation for already-established sessions.

It deliberately does not power hardware, attach packet service, create an MBIM session, scale Kubernetes, or redeploy anything.

### `ue.py`

Owns the complementary mutating COTS-UE/session lifecycle and the physical-only workload handoff:

- fixed reviewed qfit activation contract (`internet`, `/dev/cdc-wdm0`, `wwan0`, MBIM session 0, IPv4);
- explicit radio-on, packet-attach, connect, and interface-configuration primitives;
- postcondition-based state truth rather than mutation return-code truth;
- exact rollback using software-radio off plus `wwan0` down;
- rollback proof from radio-off + detached + no-IPv4 observations;
- fresh authority/gNB/N2/qfit rechecks immediately before mutation;
- resumed user-plane proof after PDU activation;
- explicit physical workload-executor handoff that refuses virtual/RFSIM results.

`ue.py` is a justified additional module because mutating modem/session lifecycle has a different safety contract from `runtime.py` observation. It is not one file per command or per probe.

## Compatibility surface

`synthran/network/r2lab.py` remains as a small compatibility import for existing CLI and callers. It contains no R2Lab implementation logic. New implementation imports should use `synthran.r2lab` or its internal package modules.

Keeping this shim avoids coupling an architecture cleanup to a public-interface migration.

## What was removed

The following flat implementation modules were removed after their behavior was consolidated:

```text
r2lab_acceptance.py
r2lab_controller.py
r2lab_gnb_lifecycle.py
r2lab_lifecycle.py
r2lab_operations.py
r2lab_physical_artifact.py
r2lab_physical_chart.py
r2lab_physical_chart_workspace.py
r2lab_physical_deployment.py
r2lab_physical_helm.py
r2lab_physical_render.py
r2lab_physical_staging.py
r2lab_power.py
r2lab_qfit.py
r2lab_qfit_operations.py
r2lab_qfit_runtime.py
r2lab_radio_profile.py
```

Only the compatibility `synthran/network/r2lab.py` remains under the old network path.

## Safety behavior preserved and extended

The package keeps the live-derived safety contract:

- no automatic R2Lab booking;
- no broad `all-off` or broad `rhubarbe bye`;
- exact selected-resource mutation only;
- strict SSH host verification;
- mutation return code is diagnostic, not hardware-state truth;
- timeout remains unknown until an independent exact observation resolves it;
- unresolved physical state retains the local resource claim;
- release continues only through independently authorized exact cleanup stages;
- claim removal requires all selected physical resources to be proven clean;
- qfit power is handled through its qfit-specific provider path;
- physical gNB ownership is singleton and non-overlapping;
- rendered physical deployment must be stopped before start authorization;
- subscriber-identity diagnostic helpers are excluded from acceptance observation;
- UE attach/connect is not hidden inside the read-only runtime verifier;
- failed attach requests exact modem/interface rollback and records unresolved cleanup explicitly;
- physical workload acceptance cannot be satisfied by the virtual/RFSIM experiment backend;
- RFSIM remains a separate accepted backend;
- physical acceptance remains ordered and evidence-backed.

## Architectural rule going forward

A new live discovery does **not** automatically get a new module.

The default remains:

1. identify which R2Lab subsystem owns the behavior;
2. add the behavior and regression test there;
3. create a new module only when a genuinely independent abstraction has enough public surface and internal cohesion to justify it.

The addition of `runtime.py` and `ue.py` follows that rule: one is the read-only observation subsystem and one is the mutating COTS-UE/session subsystem. Neither is decomposed further into one-purpose files.

The goal is not the smallest possible files. The goal is a subsystem that can be understood by following a small number of meaningful boundaries while retaining the fail-closed behavior required for physical testbed control.
