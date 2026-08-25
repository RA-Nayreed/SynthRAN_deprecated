# R2Lab physical runtime verification

The physical runtime verifier owns read-only observations between an artifact-bound singleton gNB and acceptance of the physical user plane. It does not power hardware, attach packet service, create a PDU session, change Helm state, scale Kubernetes objects, or create reservations.

## Runtime boundary

The verified sequence is:

```text
reviewed and staged physical artifact
-> exactly one ready gNB
-> current gNB/N2 proof
-> physical UE management proof
-> current cell and registration observation
-> current PDU observation
-> optional route-bound user-plane proof
-> workload eligibility
```

Observation and mutation are separate responsibilities. A failed or ambiguous observation cannot be repaired implicitly by the verifier.

## Modem evidence boundary

The acceptance path does not use broad diagnostic helpers that can expose subscriber identity or change modem state. For qfit, persistent evidence is derived only from an allow-listed set of read-only observations.

Cell and registration classification uses:

```text
AT+QNWINFO
AT+C5GREG?
```

Packet and interface classification uses read-only MBIM and operating-system queries for the reviewed modem device and data interface. Raw replies exist only long enough to be classified. Persisted evidence contains categorical state such as cell acquisition, registration, packet attachment, and IPv4 presence; it does not contain raw modem output or subscriber identifiers.

There is no generic arbitrary-AT-command acceptance API.

## Transport boundary

Physical UE hosts are reached through the reviewed R2Lab control path. Nested SSH is restricted to the selected physical UE and requires batch operation plus strict host-key verification. Host-key uncertainty, transport failure, or an unreviewed target fails closed.

The verifier does not use password fallback, `accept-new`, or a null known-hosts store.

## Runtime evidence

The verifier independently observes:

- current NR cell visibility;
- current 5G registration state;
- packet-service state;
- IPv4 presence on the selected data interface.

Acceptance remains ordered:

```text
cell acquisition
-> registration
-> packet service and IPv4
-> PDU session
```

A PDU session does not prove user-plane traffic. Unknown, malformed, contradictory, or transport-failed observations remain unknown and cannot advance acceptance.

## gNB and N2 identity

N2 evidence must belong to the current singleton gNB, not to a historical log line. Before accepting N2, the verifier rechecks the run-owned namespace, deployment binding, staged artifact identity, desired replica count, and the current ready non-terminating gNB pod.

Only bounded current logs from that verified gNB lifetime may contribute affirmative N2 evidence. Failure, timeout, disconnect, or unknown wording does not satisfy the proof. Raw logs and pod names are not persisted as acceptance evidence.

## Authority freshness

A successful earlier start does not create permanent physical authority. Runtime verification refreshes the active run claim, R2Lab lease, selected resource state, and relevant evidence bindings before advancing physical stages. Authority is refreshed again before user-plane proof when the operation crosses another live boundary.

A changed claim, expired lease, mismatched resource, or stale binding stops acceptance.

## User-plane proof

User-plane proof requires a passed PDU stage and a current accepted physical path. Traffic is bound to the selected physical data interface, normally `wwan0` for qfit, and uses bounded packet count and timeout behavior.

Persistent user-plane evidence records only the interface identity, packet counts, peer fingerprint, and accepted or failed state. The peer address and raw traffic output are not persisted.

## Persistent acceptance

Physical runtime verification advances the ordered `PhysicalRunEvidence` record. A failed gNB/N2 proof stops before UE management. A failed management proof stops before modem state. A failed cell proof leaves registration, PDU, user plane, and workload unaccepted. Successful cell, registration, and PDU observations still leave user plane unaccepted until route-bound traffic succeeds.

This keeps partial physical success truthful without promoting an incomplete run to end-to-end acceptance.

## Verification requirements

Offline tests cover the command allow-list, strict nested transport, sanitized classifier boundaries, reviewed target validation, singleton gNB/N2 binding, render-binding drift, negative N2 wording, stage ordering, and removal of raw modem, peer, and pod identity from persisted evidence.

Offline tests are not live RF acceptance. A physical stage is accepted only by current evidence from an authorized R2Lab run.
