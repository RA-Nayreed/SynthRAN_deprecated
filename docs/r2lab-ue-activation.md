# R2Lab qfit activation

The qfit runtime separates read-only observation from modem mutation. This keeps
the physical path auditable and prevents a diagnostic command from changing the
state it is meant to measure.

## Boundaries

`synthran/r2lab/runtime.py` observes:

- singleton gNB and N2 state;
- qfit management reachability;
- cell, registration, packet-service, and IPv4 state;
- bounded user-plane traffic over `wwan0`.

`synthran/r2lab/ue.py` owns:

- MBIM activation and rollback;
- ordered UE acceptance evidence;
- user-plane authorization;
- the physical workload handoff.

The RFSIM experiment runtime is not used by this path.

## Fixed modem contract

Activation accepts only this reviewed configuration:

| Setting | Value |
|---|---|
| DNN | `internet` |
| MBIM device | `/dev/cdc-wdm0` |
| Interface | `wwan0` |
| Session | `0` |
| IP type | IPv4 |

SynthRAN does not call the broad Quectel `prepare-ue`, `config-ue`, `check-ue`,
`start.sh`, or `stop.sh` helpers. Those commands can combine persistent modem
configuration, reset, subscriber-identity inspection, and unrelated state
changes. The active path uses only the required MBIM operations and never issues
`AT+CIMI`.

## Activation order

Every mutation requires current run, lease, selected-resource, singleton gNB/N2,
and qfit management authority. The direct modem sequence is:

```text
observe current sanitized state
  -> set wwan0 up
  -> set software radio on
  -> prove software radio on
  -> allow modem state to settle
  -> request packet attachment
  -> prove packet service attached
  -> connect MBIM session 0 with DNN internet
  -> apply MBIM IPv4 settings to wwan0
  -> observe the final sanitized state
```

The provider image can expose registration only after packet attachment is
requested. The ordered acceptance record is therefore updated from the final
observation: cell acquisition, registration, and PDU session are still recorded
in that order even when the attach request precedes registration visibility.

If the initial observation already proves the complete PDU state, activation is
idempotent and performs no modem write.

## State truth and rollback

Mutation return codes are diagnostic. Independent state observation decides
whether the requested transition succeeded. Activation evidence stores step
names, return codes, transport-error flags, and categorical modem state; it does
not store raw modem output or subscriber identifiers.

An unresolved activation requests the reviewed rollback:

```text
set software radio off
  -> set wwan0 down
  -> prove radio off, packet service detached, and IPv4 absent
```

No explicit PDU disconnect is issued because the reviewed provider environment
does not use it. Cleanup remains unresolved unless every rollback postcondition
is observed.

## User plane and workload

User-plane proof requires a passed PDU stage plus fresh authority, gNB/N2, qfit
management, and PDU observations. The bounded traffic probe is tied to `wwan0`
and stores packet counts with a peer digest instead of an address or raw output.

The physical workload uses `execute_physical_workload_handoff()`. Its result must
identify the R2Lab backend, `wwan0`, the same physical run, accepted sanitized
workload evidence, and proven cleanup. A virtual RFSIM result cannot satisfy the
physical workload stage.

The optional artifacts are:

```text
qfit-activation.json
physical-workload-handoff.json
```

`physical-run.json` remains the ordered acceptance record.

## Verification

The qfit tests cover the fixed command set, provider-required attach ordering,
idempotence, state-based success, exact rollback, unresolved cleanup, ordered
acceptance, user-plane reproof, and rejection of virtual workload results. They
use simulated command results and perform no live R2Lab mutation.
