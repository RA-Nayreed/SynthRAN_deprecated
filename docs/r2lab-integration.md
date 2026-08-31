# R2Lab physical backend

R2Lab is SynthRAN’s physical-radio backend. It is selected through `synthran run --radio r2lab`; there is no separate R2Lab command family.

## Supported physical topology

The current executable radio profiles are:

```text
n300
n320
```

The current executable UEs are FR1 Quectel qfit/qhat profiles exposed by `synthran inspect --radio r2lab`.

Compute nodes come from the same reviewed SLICES node catalogue used by the virtual backend. Core and RAN nodes must differ. Hardware profiles that exist in R2Lab but do not satisfy the pinned SynthRAN path remain visible in capabilities with `executable=false` and a reason.

## Required authority

A physical run requires all of the following before mutation:

- an existing accessible SLICES project;
- an authenticated SLICES CLI session;
- an active R2Lab lease;
- the exact R2Lab slice identity;
- strict public-key access to Faraday;
- a strict known-hosts file for selected SLICES compute nodes;
- exact selected radio and UE resources;
- current selected-node allocation authority.

No historical run record substitutes for those current checks.

## Read-only check

```zsh
synthran doctor \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --slice "$SYNTHRAN_R2LAB_SLICE"
```

The doctor verifies only selection, Faraday access, and the active lease. The complete run performs deeper authority checks before each relevant mutation.

## Physical run

```zsh
synthran run \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --run-id "$RUN_ID" \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT"
```

The public command composes every physical boundary internally.

## Resource authority

The selected radio and UE are bound to the immutable run ID. Preparation:

1. proves the active R2Lab lease;
2. creates the exact run claim;
3. powers only the selected radio when required;
4. prepares only the selected UE management path;
5. rechecks the lease before accepting the resource boundary.

For qhat setup, SynthRAN delegates to the pinned `5g_ansible` R2Lab UE setup role. qfit management is prepared through its reviewed provider image/management boundary and later connection mechanics are delegated to the pinned connect role.

Broad power operations are prohibited.

## SLICES and Open5GS foundation

The run verifies the selected compute nodes, current allocation, Kubernetes state, required physical network attachments, and Open5GS ownership.

When an exact prior run owns recoverable Open5GS state, the run may perform a bounded ownership handoff. The prior run ID must be observed or supplied exactly; foreign/ambiguous state is not adopted.

Open5GS reconciliation uses the reviewed SynthRAN Ansible wrapper and shared sanitized Ansible streamer.

## N3xx gNB

The physical gNB uses pinned srsRAN Helm source and reviewed N3xx values. For the accepted N300 profile, the radio settings include band 78, 30 kHz SCS, and the pinned 20 MHz profile from the locked `srsran_helm` checkout.

The staging boundary:

- loads the selected hardware profile from the locked dependency;
- renders locally;
- validates the selected node, image identity, N3/RU attachments, and zero replicas;
- packages and hashes the chart inputs/render;
- transfers the exact artifacts to the selected core node;
- rechecks hashes remotely;
- verifies current authority again;
- applies the gNB at zero replicas;
- binds run/artifact identity to the deployment.

A physical radio is then started only after zero matching gNB pods are proven. The start boundary requires exactly one ready run-owned gNB and stable current N2 evidence.

## UE activation

UE setup/connect/stop mechanics use the pinned `fiveg_ansible` roles rather than custom duplicate modem scripts.

All role execution uses the same `run_streaming_ansible_command` implementation as virtual deployment and physical Open5GS work. This gives both backends the same sanitized task output, failure rendering, and heartbeats.

After connect, SynthRAN independently observes functional postconditions. For the current Quectel path this includes:

```text
management access
-> selected data interface
-> packet-service/runtime state
-> IPv4 PDU state
-> route-bound UPF reachability
```

Successful functional evidence advances the physical acceptance record through cell acquisition, registration, and PDU boundaries. If the expected postconditions are not proven, the selected UE receives bounded run-scoped stop/recovery rather than a broad host cleanup.

## User-plane proof

The physical user-plane boundary requires traffic through the selected UE data interface, normally `wwan0`, to the current measurement peer. Generic host reachability does not satisfy this boundary.

Before proof, SynthRAN refreshes:

- active lease;
- exact resource claim;
- selected radio state;
- UE management availability;
- selected compute allocation;
- current gNB/N2 evidence.

## Deterministic workload

Once the physical user plane is accepted, SynthRAN runs the same deterministic ten-sensor IoT workload used by the virtual reference path. Backend-specific radio/modem identifiers do not change the scientific telemetry contract.

Physical workload results are written under the physical experiment root and are linked back to the physical acceptance record.

## Cleanup

Unless `--keep-resources` is explicitly requested, an accepted physical run performs reverse cleanup:

```text
workload cleanup
-> stop run-owned gNB
-> selected UE cleanup
-> selected radio cleanup
-> prove exact off/clean state
-> release the run claim
```

If cleanup cannot prove exact ownership/state while the lease is current, the run fails closed instead of expanding the target set.

An interrupted or retained run can be cleaned with:

```zsh
synthran stop \
  --run-id "$RUN_ID" \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

### Claim retirement after lease expiry

A local physical claim is authority only while the corresponding R2Lab lease is current. If a run leaves `.synthran/r2lab/active.json` behind and the lease later expires, `synthran stop` first verifies that Faraday is reachable and checks `rhubarbe leases --check` for the configured slice.

If the slice no longer holds a current lease, SynthRAN does **not** attempt gNB, UE, radio, PDU, or Kubernetes cleanup. It instead:

1. verifies that the active claim exactly matches the requested run and stored topology;
2. archives the original claim as `.synthran/r2lab/<run-id>/retired-claim.json`;
3. writes `.synthran/r2lab/<run-id>/claim-retirement.json` with `hardware_mutated=false` and the retirement reason;
4. removes only the workspace-level `active.json` marker;
5. preserves all existing run and acceptance evidence.

A Faraday transport/access failure does not retire anything. A later physical run still requires a fresh valid lease and all normal live authority checks before any mutation. Claim retirement therefore removes stale local bookkeeping without asserting that SynthRAN itself proved the post-lease hardware state.

## Logs and evidence

The physical backend writes the same public run event stream as RFSIM:

```text
.synthran/events/<run-id>.jsonl
```

Use:

```zsh
synthran logs --run-id "$RUN_ID" --follow
synthran inspect --run-id "$RUN_ID"
```

Detailed physical acceptance is stored in:

```text
.synthran/r2lab/<run-id>/physical-run.json
```

with additional sanitized physical artifacts below the run directory.

## Evidence order

Physical acceptance is conservative and ordered:

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

A failed boundary blocks later acceptance. Resume is allowed only after current authority is refreshed and persisted topology/evidence match the requested run.

## Research boundary

The physical backend now reaches the deterministic workload boundary. The currently published controlled-load campaign implementation remains validated on RFSIM. Physical campaign parity requires a separately reviewed external-peer/load path, measurement timing contract, and accepted physical campaign evidence before it can be claimed.

## Safety invariants

- exact current observation authorizes mutation;
- run evidence does not become current authority;
- one selected hardware topology belongs to one immutable run ID;
- no global radio/UE power-off;
- no wildcard Kubernetes cleanup;
- no guessed prior-run or allocation identity;
- one run-owned physical gNB for the selected radio;
- UE modem mechanics stay in pinned upstream roles;
- functional postconditions are independently verified by SynthRAN;
- with a current lease, cleanup releases a claim only after exact off/clean state is proven;
- without a current lease, SynthRAN performs no cleanup mutation and may retire only the exact stale local claim with explicit retirement evidence.
