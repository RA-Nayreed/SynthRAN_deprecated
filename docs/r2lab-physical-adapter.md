# R2Lab physical adapter implementation record

This document records the physical network/chart work that followed `r2lab-smoke-002`: what was inspected, how each issue was discovered, how smoke 003 corrected one earlier RF interpretation, and how the result is encoded in the consolidated R2Lab package.

The live chronology is in `docs/r2lab-smoke-002.md`. The broader implementation chronology is in `docs/r2lab-smoke-002-development-log.md`. Package structure is documented in `docs/r2lab-code-architecture.md`.

## Starting rule

The accepted `synthran.fiveg_ansible` adapter remains RFSIM-only. Physical support is not added by widening its radio whitelist or adding N300 branches to the virtual path.

The physical backend has different invariants: UHD/N300 instead of ZMQ/RFSIM, one physical SDR owner at a time, a COTS qfit modem instead of srsUE, explicit carrier/SSB/Point-A semantics, exact provider-state evidence, a dedicated physical image digest, and a chart Deployment that must be staged stopped and started without overlapping owners.

Those behaviors live together in `synthran/r2lab/deployment.py`, with provider semantics in `provider.py` and radio/UE semantics in `radio.py`.

## Exact pinned sources reviewed

The checkpoint was reviewed against exact dependency-lock revisions rather than repository default branches:

```text
fiveg_ansible
a0149fc0dde39e2872945a0f3c91e804ece52d4f

srsran_helm
8dfb9890d127734cdcd6eee9df8c5d09b1a8076a
```

The R2Lab OAI radio reference was also checked against the repository source `sopnode/oai5g-rru`, specifically `ran-config/conf/gnb.band78.sa.fr1.106PRB.2x2.usrpn310.conf`.

## Discovery: the pinned chart matches useful N300 topology, but is not safe enough by itself

The pinned srsRAN chart established useful structure:

- `.Values.gnbConfig` supplies the gNB configuration;
- AMF configuration lives under `cu_cp.amf`;
- the N300 path uses UHD;
- the RU network is macvlan-based;
- the RAN node is selected explicitly;
- the chart exposes the remote-control port through `gnbConfig.remote_control`.

The canonical render in `synthran/r2lab/deployment.py` follows the chart's real `cu_cp.amf` structure. SynthRAN review metadata is kept outside the final `gnbConfig` so it cannot become an unknown srsRAN key.

The same upstream values also contain CORESET/PRACH settings explicitly described as matching srsUE capabilities. They are deliberately absent from the qfit/COTS candidate until independently reviewed for the COTS UE path.

## Discovery: normal Deployment replacement can create two physical gNB owners

During the live run, a normal Kubernetes replacement briefly left a terminating gNB while a replacement pod attempted to start. Both competed for one N300 UHD device.

The pinned Deployment template was then inspected and found to contain a hard-coded `replicas: 1` with no explicit non-overlapping replacement strategy.

### Implementation consequence

`deployment.py` owns a singleton lifecycle:

```text
scale exact gNB Deployment to zero
  -> prove all matching pods are gone, including terminating pods
  -> allow UHD release
  -> apply reviewed configuration
  -> scale exact Deployment to one
  -> prove exactly one matching pod is Running and ready
```

More than one matching pod causes fail-closed scale-to-zero recovery.

The guarded chart overlay also makes replica count values-driven and installs `Recreate`, allowing the chart to be staged safely at zero replicas.

## Discovery: the pinned chart renders the physical image by mutable tag

The exact Deployment template renders `repository:tag`. Smoke-002, however, had exercised a specific UHD image digest.

### Implementation consequence

The lock contains separate virtual and physical srsRAN gNB entries. The virtual RFSIM lock is unchanged. `srsran_gnb_physical` records the reviewed UHD/N300 image and digest.

The guarded chart overlay changes the reviewed image expression to:

```text
repository:tag@sha256:digest
```

If any exact upstream anchor changes, the overlay refuses to apply rather than silently patching a different chart.

## Discovery: the optional log sidecar is not digest-pinned

The chart can add a `busybox` log sidecar using an unpinned image reference. It is not required for physical acceptance because SynthRAN owns its evidence path.

### Implementation consequence

The physical values disable that sidecar. Offline render validation rejects a rendered unpinned log sidecar.

## Discovery: the upstream N300 retry path has incompatible ownership semantics

The exact pinned `fiveg_ansible` physical tasks were inspected. For N300/N320 the upstream path can uninstall/retry the release, inspect the first returned pod, and swap paired radio IP addresses after failure.

Those behaviors are reasonable for a human recovery playbook, but not for SynthRAN's research-evidence contract. Automatic IP swapping changes the tested hardware binding, and selecting the first pod is unsafe during replacement.

SynthRAN therefore consumes the pinned chart contract through its reviewed overlay and singleton lifecycle rather than using that retry behavior as the production ownership model.

## RF reference correction: smoke 003 supersedes the initial smoke-002 interpretation

After smoke-002, the OAI SSB and Point-A fields were correctly recognized as distinct semantics, but the PR initially transcribed the carrier width incorrectly as 162 PRBs. That produced a derived 60 MHz candidate with carrier-center ARFCN 621984.

Smoke 003 showed that this candidate was internally streamable to the N300 but did not produce a cell visible to qfit07. A direct re-check of the actual R2Lab OAI N310 source then found the transcription error.

The reviewed source records:

```text
absoluteFrequencySSB       621312
dl_absoluteFrequencyPointA 620040
dl_carrierBandwidth        106 PRBs
subcarrier spacing         30 kHz
TX/RX paths                2x2
```

### Correct derivation

```text
106 PRB x 12 subcarriers x 30 kHz = 38.16 MHz occupied grid
half grid = 19.08 MHz
19.08 MHz / 15 kHz FR1 ARFCN raster = 1272 steps
620040 + 1272 = carrier-center ARFCN 621312
```

The corrected reviewed offline candidate is therefore:

```text
band 78
carrier-center ARFCN 621312 (~3319.68 MHz)
expected SSB ARFCN 621312
nominal bandwidth 40 MHz
common SCS 30 kHz
2x2 antennas
```

Carrier-center and SSB remain different semantic types even though this source happens to place both at the same numeric ARFCN.

### Implementation consequence

`synthran/r2lab/radio.py` now stores `106` PRBs for the reviewed R2Lab OAI reference. The old `162/60/621984` candidate is retained only in a negative regression demonstrating that the stale smoke-003 profile is rejected.

`PhysicalSrsranRender.validate()` derives its expected radio values from the reviewed candidate. `validate_physical_helm_render()` compares the rendered text against the validated chart intent rather than a second hard-coded radio tuple. `execute_stopped_physical_staging()` independently rejects render evidence whose carrier, bandwidth, or antenna counts do not match the reviewed R2Lab candidate before any cluster write.

This corrected candidate remains offline-only until a follow-up physical run proves COTS UE cell acquisition.

## One coherent deployment subsystem

The first implementation pass split the physical pipeline across plan/render/chart/workspace/Helm/artifact/staging/lifecycle modules. After architecture review, those responsibilities were consolidated into `synthran/r2lab/deployment.py` as one subsystem.

```text
reviewed radio intent
  -> physical deployment plan
  -> canonical srsRAN render
  -> pinned chart bundle
  -> guarded isolated chart overlay
  -> locked Helm template render
  -> rendered-text validation
  -> deterministic package + hashes
  -> stopped-only cluster staging
  -> singleton start lifecycle
```

## Offline Helm render gate

Before Kubernetes is contacted, the deployment subsystem verifies the locally locked Helm version, runs only `helm template`, and rejects the output unless it proves:

- exactly the digest-locked physical gNB image;
- zero replicas;
- `Recreate`;
- the carrier ARFCN and nominal bandwidth derived from the reviewed R2Lab candidate (`621312`, 40 MHz at the current checkpoint);
- 2 downlink and 2 uplink antenna paths;
- no inherited srsUE-specific CORESET/PRACH override;
- no optional mutable log sidecar;
- no RFSIM or broad cleanup behavior.

Successful validation returns a SHA-256 render hash. It is still offline evidence, not live acceptance.

A dedicated regression also feeds the stale smoke-003 `621984 / 60 MHz` values into Helm validation and requires rejection.

## Stopped-only cluster staging

The staging boundary can transfer and install only the already-reviewed artifact while keeping the gNB stopped. Before any remote mutation it now also checks that the render evidence matches the current reviewed radio candidate.

Before Helm staging it requires:

- reviewed carrier/bandwidth/antenna evidence;
- fresh SLICES reservation authority;
- matching f2/f3 allocation ownership;
- strict known-host SSH;
- local and remote artifact hash equality;
- the locked Helm version on the controller;
- an Open5GS namespace owned by the same run;
- an absent or zero-replica existing gNB Deployment;
- zero matching gNB pods.

After staging it again requires desired replicas `0` and pod count `0`.

This operation does not power the N300, does not touch qfit, and does not claim physical acceptance.

A smoke-gate regression verifies that stale smoke-003 render evidence is rejected with zero provider/cluster commands issued.

## Smoke-003 transport evidence

Smoke 003 proved that the stale RF candidate was nevertheless being delivered to the N300 rather than getting stuck in the UHD transport path. Over a three-second observation window, the gNB pod's `ru1` interface increased by roughly 1.51 GB TX and 1.52 GB RX.

That result narrowed the failure from generic radio transport to the transmitted cell/profile itself. It is the reason the source reference was re-audited instead of treating the N300 link as failed.

## qfit runtime evidence

Cell visibility, registration, packet-service attachment, address assignment, and user-plane traffic are different acceptance stages.

`synthran/r2lab/radio.py` classifies already-collected qfit evidence into conservative states for:

- NR-SA cell acquisition / no service / other service / unknown;
- registration / searching / not registered / unknown;
- packet attached / detached / unknown;
- IPv4 present / absent / unknown.

Packet attachment plus IPv4 can become PDU-session evidence only after cell acquisition and registration. User-plane acceptance still requires a separate traffic probe.

Smoke 003 also showed that provider power plus ping reachability is not enough to declare a qfit/FIT UE management-ready. The live path required FIT OS provisioning, strict SSH trust, external USB power, and RM500Q enumeration before modem probes were meaningful. Current preparation loads the reviewed FIT image only when exact provider state first proves the selected qfit host off; an already-on host is preserved and must pass the same strict readiness checks.

## Ordered physical acceptance

`synthran/r2lab/acceptance.py` records:

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

A stage cannot be skipped. A failed stage blocks later acceptance. In smoke 003 the modem reported `No Service` with software radio ON, so cell acquisition failed and packet attach/PDU mutation was not attempted.

## CI and automation implications

The PR already contains deterministic coverage for resource preparation, stopped staging, and authorized singleton gNB start. The new RF regressions make the source-of-truth part of that automation boundary: a stale 60 MHz render cannot be staged simply because it is syntactically valid Helm output.

The remaining orchestration work is to compose the live stages into a single fail-closed smoke workflow while retaining the existing safety rules:

```text
external lease
  -> exact resource authority
  -> FIT/qfit readiness and explicit provisioning approval
  -> strict host trust + modem enumeration
  -> reviewed RF render/staging
  -> singleton gNB start
  -> Open5GS/N2
  -> modem initialization + software radio on
  -> cell acquisition
       fail => persist and stop
  -> registration
  -> PDU activation
  -> user plane
  -> physical workload
  -> exact cleanup
```

Automatic R2Lab booking remains prohibited. PRACH/TDD parity with the OAI source is a separate review item and must not be changed ad hoc as part of the carrier/bandwidth correction.
