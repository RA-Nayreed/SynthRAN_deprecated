# R2Lab smoke 002 development log

This log records how the live `r2lab-smoke-002` evidence was translated into code on the smoke-gate pull request. It complements the run chronology in `docs/r2lab-smoke-002.md` by documenting the implementation sequence, the reason for each change, and problems discovered while validating the patch.

## Development boundary

The R2Lab work continues to accumulate against `r2lab-integration`, which is the temporary integration main while `main` remains the accepted RFSIM truth. The current checkpoint is `r2lab-smoke-gate`. The already-open pull request retains its older remote head name until that PR is completed, but new branch/documentation naming does not use that prefix.

The smoke-gate PR remains draft. Live smoke 002 was not a complete physical acceptance because UE cell acquisition, registration, PDU session, user plane, and the research workload were not reached.

## 1. PDU state semantics were implemented first

### Observation

During exact N300 cleanup, the provider printed the N300 as `OFF`, but the `rhubarbe pdu off n300` process returned status `1`. An immediate exact `pdu status n300` query again printed `OFF`.

### Reasoning

The existing resource gate treated a non-zero mutation return code as failure. The live result showed that this would preserve a false failure even when the provider itself proved that the requested state had been reached.

At the same time, accepting mutation stdout alone would also be unsafe because a command can print intermediate output before failing or timing out.

### Code

`synthran/network/r2lab_power.py` now separates:

- requested state;
- mutation return code;
- status-query return code;
- exact observed provider state.

`synthran/network/r2lab_operations.py` performs one exact mutation followed by one exact status query. `ON` or `OFF` is accepted only from the selected resource's parsed status observation. Return codes remain diagnostic evidence.

Regression tests include the live `rc=1` plus `OFF` case.

## 2. Timeout semantics were made fail-closed

### Observation

Several live hardware/network operations had meaningful timeout risk. A timeout does not prove that the provider did nothing; the remote action can complete after the local transport stops waiting.

### Reasoning

Treating timeout as ordinary failure would allow the controller to infer a false clean state. Treating every timeout as permanently failed would also discard a safe way to resolve ambiguity when a subsequent exact state query is available.

### Code

Verified PDU and qfit operations now allow the mutation return code to be absent. After a mutation transport failure, they still issue the exact provider-state query. If the requested state is then proven, the state is accepted and the transport problem remains diagnostic evidence. If state is missing or contradictory, the result remains `UNKNOWN`.

The release assessment removes the local claim only when every selected physical resource is proven off.

## 3. Release was made stage-aware instead of all-or-nothing

### Observation

The safe manual cleanup sequence showed that independent exact cleanup could continue even after an earlier stage was ambiguous, as long as authority was still valid and cleanup scope did not widen.

### Reasoning

If UE cleanup is unresolved, abandoning radio cleanup can leave an independently controllable transmitter active. But replacing that with a global provider cleanup would violate the exact-resource safety boundary.

### Code

`synthran/network/r2lab_lifecycle.py` models `PROVEN_OFF`, `PROVEN_ON`, and `UNKNOWN` evidence per selected resource.

The controller now:

1. rechecks lease authority;
2. attempts exact UE cleanup and records its evidence;
3. rechecks authority;
4. attempts exact radio cleanup even if UE evidence is unresolved;
5. retains the claim unless both exact resources are proven off.

Sanitized cleanup assessment is persisted in the run manifest.

## 4. qfit became a first-class resource type

### Observation

The deeper physical run used `qfit07`, not the original qhat smoke target. The successful cleanup proof was the provider observation `reboot07:off` after the exact qfit power-off helper.

### Reasoning

A qfit resource should not be forced through the qhat PDU command model. The helper performs the qfit-specific mutation, while independent provider state should still decide whether the requested power state is accepted.

### Code

`synthran/network/r2lab_qfit.py` strictly maps `qfitNN` to its exact R2Lab node and parses only the corresponding `rebootNN:on|off` observation.

`synthran/network/r2lab_qfit_operations.py` performs `qfit on|off qfitNN` and then independently queries `rhubarbe status N`.

The public controller supports the reviewed qfit set and uses the same timeout/unknown-state policy as PDU-backed resources.

Controller-level qfit release regressions additionally verify that the local claim is dropped only after both qfit and N300 are proven off, and that an unresolved qfit still allows exact N300 cleanup while retaining the claim.

## 5. The controller was rewired to consume evidence-backed operations

The earlier `synthran.network.r2lab` API remains the public import surface so existing CLI code does not need to change at the same time as provider semantics. The implementation moved into `synthran/network/r2lab_controller.py`, while `synthran/network/r2lab.py` acts as a compatibility surface.

`prepare` now requires exact post-mutation proof for radio and UE power states. `release` persists unresolved cleanup evidence and keeps the active claim rather than interpreting command failure as known hardware state.

No broad cleanup primitive was introduced.

## 6. RF configuration semantics were separated offline

### Observation

The final live frequency experiment used an OAI reference value as a candidate srsRAN carrier ARFCN. A later inspection of the known R2Lab OAI configuration showed that the source value was `absoluteFrequencySSB`; the same configuration separately defines Point A, carrier bandwidth, and two transmit/two receive paths.

### Reasoning

The failed scan is valid evidence for the configuration actually transmitted, but it is not a valid test of a faithful OAI-to-srsRAN translation. The software must make it difficult to copy an ARFCN from one semantic field into another without review.

### Code

`synthran/network/r2lab_radio_profile.py` labels NR-ARFCNs as one of:

- `carrier-center`;
- `ssb`;
- `point-a`.

A physical srsRAN candidate refuses an SSB or Point-A ARFCN where a carrier-center value is required. The reference stores the observed OAI SSB/Point-A/bandwidth/2x2 facts without declaring a derived srsRAN profile live-accepted.

All candidate output remains explicitly offline-only.

## 7. Kubernetes rolling overlap became a deployment requirement

### Observation

During a live gNB configuration update, ordinary Kubernetes rolling behavior briefly allowed a replacement pod and a terminating pod to compete for the one N300 UHD device.

### Reasoning

A physical SDR is a singleton hardware owner, not a replica-friendly stateless service. Any physical adapter that allows overlapping gNB owners can fail even when both configurations are otherwise valid.

### Original requirement

The required sequence was recorded as:

```text
stop current gNB
  -> prove zero gNB pods
  -> allow UHD claim release
  -> apply configuration
  -> start exactly one gNB
```

This was initially left as a documented requirement rather than weakening the existing RFSIM-only adapter.

## 8. CI exposed a privacy-scanner false positive

### What failed

After the evidence-backed controller and tests were pushed, GitHub Actions showed:

- offline Python unit tests: pass;
- workbench typecheck/build/tests: pass;
- Git-history secret scan: pass;
- tracked-source privacy scan: fail.

The failure was therefore not a functional R2Lab regression.

### How it was discovered

The workflow definition was inspected to confirm the failing stage runs `python -m synthran privacy scan --worktree`. The privacy scanner implementation was then inspected to understand which assignment names are treated as credential-like.

The exact Actions log identified one finding in `synthran/network/r2lab_qfit.py`: a local parser variable named `token` triggered the scanner's credential-assignment rule. The variable held only the generated text `rebootNN`; no credential had been committed.

### Fix

The local variable was renamed from `token` to `status_prefix`. The scanner policy was not weakened or bypassed.

The follow-up Actions run then passed all stages: offline unit tests, workbench validation, tracked-source privacy scan, and Git-history secret scan.

This is retained in the development record because the correct resolution was to make the implementation unambiguous to the existing privacy policy rather than adding an exception.

## 9. The virtual adapter was deliberately left unchanged

### Observation

The accepted `fiveg_ansible` path still hard-rejects anything except `rru=rfsim`.

### Reasoning

Changing the existing `SUPPORTED_RADIO` constant from `rfsim` to include `n300` would mix two different deployment contracts. The virtual path assumes srsUE/RFSIM behavior, while the physical path needs UHD, singleton ownership, COTS UE semantics, different radio validation, and different evidence.

### Decision

The physical backend is being implemented as a separate boundary. The existing virtual adapter remains a regression oracle rather than becoming a conditional collection of physical exceptions.

## 10. The rolling-overlap rule was turned into executable code

### Observation used

During smoke 002, an ordinary rolling restart created a replacement gNB before the terminating gNB had fully released the N300. The live recovery that worked was scale to zero, wait until no gNB pod exists, allow UHD release, then restart one gNB.

### Implementation

`synthran/network/r2lab_gnb_lifecycle.py` now implements that exact ownership sequence as an injected Kubernetes controller.

It:

- scales only `deployment/srsran-gnb` in the `open5gs` namespace;
- queries all matching pods as JSON using the exact gNB selector;
- never relies on Kubernetes list ordering or `.items[0]`;
- counts a terminating pod as still present because it can still own the UHD device;
- invokes the configuration callback only after zero matching pods are proven;
- waits the explicit UHD-release interval before configuration;
- starts with exactly one desired replica;
- accepts startup only when exactly one matching pod is Running and every reported container is ready;
- treats a scale command return code as diagnostic rather than replica-state truth when the subsequent pod observation proves the state;
- requests exact scale-to-zero recovery if startup is ambiguous or if more than one gNB pod is observed.

### Tests

`tests/test_r2lab_gnb_lifecycle.py` covers:

- terminating pods preventing configuration;
- configuration occurring only after zero pods;
- state proof overriding a non-zero scale mutation return code;
- configuration failure leaving the Deployment stopped;
- overlapping startup owners triggering fail-closed scale-to-zero recovery;
- malformed pod JSON failing closed.

This turns the most important Kubernetes-specific live discovery into product behavior while still keeping Helm rendering separate.

## 11. The OAI reference was converted into a semantic offline candidate

### Observation used

The R2Lab OAI reference recorded:

- SSB ARFCN `621312`;
- Point-A ARFCN `620040`;
- 162 PRBs at 30 kHz SCS;
- two TX and two RX paths.

The smoke-002 final experiment had reused the SSB ARFCN as the srsRAN carrier-center ARFCN and had not reproduced the 60 MHz/2x2 reference semantics.

### Derivation now encoded

`r2lab_radio_profile.py` now derives the resource-grid center from Point A instead of copying the SSB value:

```text
occupied grid = 162 PRB x 12 subcarriers x 30 kHz
half grid     = 29.16 MHz
FR1 raster    = 15 kHz per ARFCN step above 3 GHz
ARFCN offset  = 29.16 MHz / 15 kHz = 1944
carrier       = 620040 + 1944 = 621984
```

The resulting carrier-center frequency is approximately 3329.76 MHz. The expected SSB remains a separate semantic field at ARFCN 621312. The 162-PRB/30-kHz grid maps to a nominal 60 MHz carrier, and the reference-aligned intent requires 2x2 antennas.

### Safety property

The helper is named and serialized as an `offline-reference-aligned-candidate`. It is not marked live accepted. Tests explicitly reject:

- reusing the SSB value as carrier center;
- using a 20 MHz profile against the 162-PRB reference;
- using a SISO profile against the reviewed 2x2 reference.

This is how the software now preserves the distinction that was missed during the manual frequency experiment.

## 12. A separate physical deployment plan was introduced

### Reasoning

Once the reference semantics and singleton lifecycle were executable, the next safe boundary was a physical plan that cannot accidentally fall through to the RFSIM adapter.

### Code

`synthran/network/r2lab_physical_deployment.py` defines a non-executing physical plan for the currently reviewed topology. It deliberately fails closed to:

- Open5GS on `sopnode-f2`;
- srsRAN on `sopnode-f3`;
- N300 as the physical radio;
- the reference-aligned radio intent;
- the reviewed TX/RX gain range;
- `Recreate`/maximum-one-gNB ownership;
- no inherited srsUE-specific CORESET or PRACH override.

The plan is `offline-plan-only` and states that the next step is to map these canonical values into the pinned physical Helm/chart representation and inspect the fully rendered result before execution.

### Why the topology is intentionally narrow

Smoke 002 gave strong evidence for one exact foundation/core/RAN layout. Generalizing to every supported SLICES node pair before a second physical acceptance would create untested degrees of freedom. The first physical adapter therefore encodes only what the live run actually exercised.

## 13. Physical acceptance became an ordered state machine

### Observation used

Smoke 002 simultaneously had successful lower-layer stages and an unsuccessful overall physical result. A single `success` boolean cannot represent that truthfully.

### Code

`synthran/network/r2lab_acceptance.py` now defines this ordered acceptance ladder:

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

Evidence must be contiguous. Stages cannot be skipped, a failed stage blocks all later acceptance, and all later stages remain explicitly `not-reached`.

This directly models the smoke-002 result: the run can retain PASS evidence through UE management, FAIL at cell acquisition, and must not accidentally report registration or user-plane success.

## 14. Current safe boundary

The smoke-gate branch now contains executable boundaries for the main lessons learned during the live run:

- provider state is independent of mutation return code;
- timeout means unknown until exact state resolves it;
- qfit is a first-class provider resource;
- cleanup is exact and claim-aware;
- a physical gNB is a singleton owner;
- the reviewed OAI fields have distinct frequency semantics;
- physical deployment has a separate adapter boundary;
- physical acceptance is staged and monotonic.

Still intentionally pending:

- mapping the canonical physical plan into the pinned physical srsRAN Helm/chart values;
- inspecting the complete rendered physical configuration before execution;
- wiring the non-overlapping lifecycle to that live physical chart adapter and strict SLICES SSH path;
- qfit cell-acquisition, registration, PDU-session, and user-plane probes with sanitized evidence;
- connecting those runtime observations to the ordered acceptance record;
- a second controlled physical acceptance run.

No additional RF mutation was needed to implement these boundaries. They were derived from the evidence already collected in smoke 002.
