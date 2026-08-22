# R2Lab qfit activation and physical workload handoff

This note records the mutating UE/session boundary added after the read-only runtime verifier. It documents what was inspected, why the upstream convenience wrappers are not used directly, how the new activation sequence is made fail-closed, and where the physical workload can safely take over.

The historical result of `r2lab-smoke-002` is unchanged: that run did not acquire a cell and therefore did not reach registration, PDU session, user plane, or workload acceptance. No new live RF or modem mutation was performed while implementing this code.

## Why activation is a separate subsystem from observation

`synthran/r2lab/runtime.py` is intentionally read-only. It proves current gNB/N2, qfit management reachability, cell/registration state, packet-service state, IPv4 presence, and optional user-plane traffic without changing modem or Kubernetes state.

Activation has a different safety contract. It can turn software radio on, request packet attachment, create an MBIM session, configure the UE interface, and request rollback. That mutation is now isolated in:

```text
synthran/r2lab/ue.py
```

The active path is therefore:

```text
fresh run/N300 authority
  -> current singleton gNB/N2 proof
  -> qfit management proof
  -> read-only cell acquisition
  -> read-only registration
  -> controlled MBIM activation
  -> PDU postcondition proof
  -> current user-plane proof
  -> explicit physical workload handoff
```

The accepted RFSIM experiment runtime is not part of this path.

## Upstream sources inspected

The Quectel utilities were reviewed at the same public `sopnode/oai5g-rru` revision used during the earlier investigation:

```text
commit: 9d7f2df8e98527c1a3b05c6352b167e8e9ce7c19
```

The relevant scripts were:

```text
quectel-utils/prepare-ue
quectel-utils/config-ue
quectel-utils/check-ue
quectel-utils/start.sh
quectel-utils/stop.sh
```

This review produced several implementation decisions.

## Discovery: `prepare-ue` is too broad for the acceptance mutation boundary

`prepare-ue` is a convenience workflow. It powers the modem software down/up, waits, calls `config-ue`, checks the device, and runs initialization. Its default DNN is `oai.ipv4`.

Smoke 002 used the `internet` DNN, so relying on that default would already be wrong for the current physical checkpoint. More importantly, the helper bundles configuration, resets, inspection, and initialization into one command, which makes it difficult to identify which state transition actually happened when a timeout occurs.

### Consequence

SynthRAN does not call `prepare-ue` from the acceptance path. Activation is performed one reviewed MBIM operation at a time.

## Discovery: `config-ue` changes persistent modem configuration and resets the modem

`config-ue` does much more than choose an APN. It can:

- rewrite PDP contexts;
- change preferred radio mode;
- inspect LTE/NR bands;
- change MBIM/QMI USB mode;
- issue a `CFUN=1,1` modem reset.

Those are preparation operations, not ordinary PDU-session activation. Automatically replaying them during every acceptance attempt would enlarge the mutation surface and could erase the distinction between a radio-configuration problem and an attach problem.

### Consequence

The production activation boundary assumes the reviewed qfit is already in its prepared MBIM form and uses the existing `/dev/cdc-wdm0` + `wwan0` contract. It refuses to mutate USB mode, persistent APN configuration, band preferences, or modem reset state.

## Discovery: `check-ue` is still excluded because it reads subscriber identity

The public diagnostic helper begins by issuing `AT+CIMI` and prints the returned IMSI. This was already identified while implementing the read-only runtime verifier.

### Consequence

Neither observation nor activation calls `check-ue`. The new mutating path does not issue subscriber-identity commands and does not persist raw modem output.

## Discovery: `start.sh` shows the right primitive MBIM sequence, but is not a safe evidence boundary by itself

The reviewed `start.sh` performs this core sequence:

```text
ip/ifconfig wwan0 up
mbimcli --set-radio-state=on
mbimcli --attach-packet-service
mbimcli --connect=session-id=0,apn=<DNN>,ip-type=ipv4
mbim-set-ip.sh /dev/cdc-wdm0 wwan0 0
```

It defaults to `oai.ipv4` but accepts `-F <DNN>`. Smoke 002 used `-F internet`.

The helper is useful as a reference for the device/interface/session contract, but one wrapper return code does not tell SynthRAN whether radio-on succeeded, packet attachment succeeded, a connection was created, or the interface received an address before a timeout.

### Consequence

`synthran/r2lab/ue.py` executes the same reviewed primitives explicitly with the current narrow contract:

```text
DNN:       internet
MBIM:      /dev/cdc-wdm0
interface: wwan0
session:   0
IP type:   ipv4
```

After radio-on, SynthRAN independently queries software-radio state. After packet attach it independently observes packet-service state. After connect/IP setup it requires the existing sanitized qfit classifier to prove:

```text
cell acquired
registration passed
packet service attached
IPv4 present on wwan0
```

Only that postcondition advances the PDU-session acceptance stage.

## Mutation return code is diagnostic, not session-state truth

The provider-side N300 cleanup already established that a process return code can disagree with the resulting physical state. The same principle is applied to the qfit activation sequence.

For each MBIM mutation SynthRAN records only sanitized diagnostic information:

```text
step name
return code or transport error
```

A non-zero return code does not automatically fail the session if the independent state observation proves that the requested transition happened. Conversely, a zero return code cannot advance acceptance when the required postcondition is absent.

Raw command stdout/stderr is not written to activation evidence.

## Fail-closed activation sequence

The controlled sequence is:

```text
prove current gNB/N2 + qfit management
  -> observe current cell + registration
  -> wwan0 up
  -> software radio on
  -> prove radio on and registration
  -> request packet attach
  -> prove packet service attached
  -> request MBIM session 0 with DNN internet
  -> apply MBIM IP configuration to wwan0
  -> prove attached + IPv4
```

If a PDU session is already proven before mutation, activation is idempotent and performs no write.

The high-level orchestration refreshes the active local claim, R2Lab lease, and exact N300=ON proof immediately before the first modem mutation. It also reproves current singleton gNB/N2 and qfit management reachability at that point.

## Discovery: upstream `stop.sh` deliberately avoids explicit PDU disconnect

The reviewed `stop.sh` contains disconnect commands, but they are commented out with a note about a gNB PDU-session release issue. The active cleanup behavior is software-radio off plus interface down.

### Consequence

SynthRAN does not introduce an automatic `--disconnect=0` step that the reviewed upstream environment itself avoids.

When activation cannot prove its intended postcondition, the exact rollback is:

```text
mbimcli --set-radio-state=off
ip link set dev wwan0 down
```

Cleanup is considered proven only when independent observation sees:

```text
software radio off
packet service detached
no IPv4 on wwan0
```

Otherwise the activation evidence is `failed-unresolved`. The code does not invent a clean state from a successful-looking mutation return code.

## Acceptance behavior before attach

The new active path intentionally separates pre-attach evidence from PDU evidence.

A registered UE whose packet service is still detached is not recorded as a failed PDU session merely because attach has not yet been attempted. The active orchestrator records:

```text
cell acquisition -> pass/fail
registration     -> pass/fail
```

and only then runs the controlled activation boundary. PDU success/failure is recorded from the post-activation observation.

This avoids converting a valid pre-attach state into an irreversible acceptance failure.

## User-plane proof after activation

After PDU acceptance, user-plane proof has its own high-level entry point. Before ICMP traffic it refreshes:

- active R2Lab claim/lease/N300 authority;
- current singleton gNB/N2 state;
- qfit management reachability;
- current attached + IPv4 PDU state.

The existing bounded probe is then executed through the selected qfit and explicitly bound to `wwan0`. Persistent evidence still contains packet counts and a peer SHA-256 fingerprint rather than the peer address or raw ping output.

## Discovery: the accepted virtual experiment runtime cannot be used as the physical workload executor

The existing integrated experiment is intentionally coupled to the accepted RFSIM topology. It discovers and patches the run-owned `srsUE` Deployment, reconciles RFSIM after that rollout, installs an MQTT sidecar in the srsUE pod, and measures `tun_srsue1` counters.

A qfit COTS UE has none of those ownership semantics. Silently calling the virtual runtime after physical user-plane proof would therefore mix two different backends and could produce misleading evidence.

### Consequence

The new workload boundary is an explicit handoff, not an implicit call to `synthran.experiment.runtime.execute_experiment`.

`execute_physical_workload_handoff()` requires:

```text
physical user-plane stage already passed
fresh R2Lab claim/lease/N300 authority
current singleton gNB/N2 proof
qfit management reachability
current PDU reproof
an explicitly supplied workload executor
```

The executor result must identify:

```text
backend = r2lab
interface = wwan0
matching physical run ID
sanitized workload evidence SHA-256
accepted state
cleanup-proven state
```

A virtual/RFSIM result is rejected before it can satisfy the physical `workload` acceptance stage.

This gives the next physical workload adapter a narrow contract without weakening or rewriting the accepted virtual experiment implementation.

## Evidence artifacts

The mutating UE boundary can persist two new sanitized artifacts independently of the main ordered physical-run record:

```text
qfit-activation.json
physical-workload-handoff.json
```

Activation evidence contains only categorical modem state and mutation diagnostics. Workload handoff evidence contains backend/interface identity, run/workload ID, an evidence digest, and acceptance/cleanup booleans.

The ordered `physical-run.json` remains the final acceptance truth.

## Regression coverage added before any new live run

`tests/test_r2lab_ue.py` covers:

- radio-state parsing, including conflicting observations;
- exact command allow-list and the fixed `internet`/MBIM/`wwan0` contract;
- rejection of `oai.ipv4` for the current physical checkpoint;
- absence of `prepare-ue`, `config-ue`, `check-ue`, `start.sh`, `stop.sh`, `AT+CIMI`, and explicit disconnect from the activation command set;
- postcondition acceptance despite non-zero mutation return codes;
- idempotent already-established PDU handling;
- exact rollback after attach failure;
- explicit unresolved rollback when cleanup cannot be proven;
- high-level stage progression through gNB/N2, management, cell, registration, and PDU;
- no activation mutation when cell acquisition itself fails;
- user-plane proof only after current PDU reproof;
- explicit physical workload completion;
- rejection of a virtual/RFSIM workload result;
- sanitized workload-executor failure handling.

No live R2Lab or SLICES mutation is part of these tests.

## Next live boundary

The code is now ready to attempt the activation sequence only after the corrected physical radio profile is brought up under fresh authority and a qfit actually proves cell acquisition and registration.

The next live acceptance still needs to establish, in order:

```text
corrected 60 MHz / 2x2 cell acquired
  -> registration
  -> controlled PDU activation
  -> wwan0 user plane
  -> physical-specific workload executor
  -> exact cleanup
```

Until that run is performed, the new activation/workload code is implemented and regression-tested but not live accepted.
