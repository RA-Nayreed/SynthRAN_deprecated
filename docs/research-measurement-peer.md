# Research measurement peer

Controlled capacity calibration and background load require a peer outside the 5G core host. This prevents a nominal “user-plane” measurement from collapsing into a same-host or Kubernetes hairpin path.

## Accepted virtual path

For the current RFSIM research implementation:

```text
srsUE PDU interface
-> srsRAN/RFSIM
-> Open5GS UPF
-> core egress/NAT
-> prepared external measurement peer
```

In the reviewed two-node layout, the RAN-side prepared compute node is used as the external peer while the separate core node hosts Open5GS.

## Why the core node is rejected

Running the iperf3 server on the same host as the core can create a path that no longer measures the intended external UE-to-network traversal. Depending on routing/Kubernetes state, traffic can terminate locally or take a hairpin path that bypasses meaningful external transport.

A successful TCP/UDP connection is therefore insufficient evidence that the selected peer is scientifically valid.

## Peer selection requirements

A measurement peer must:

- be outside the selected 5G core host;
- have a current reachable address on the intended external path;
- be bound to the accepted network epoch/run;
- support exact run-owned iperf server lifecycle;
- avoid ambiguous same-host/container routing;
- remain independently observable during the measurement window.

## Calibration

After an accepted virtual run:

```zsh
export NETWORK_RUN='virtual-001'
export INVENTORY=".synthran/preparations/$NETWORK_RUN/hosts.ini"
export MEASUREMENT_PEER_IP='PEER_IPV4'

synthran research calibrate \
  --inventory "$INVENTORY" \
  --network-run-id "$NETWORK_RUN" \
  --target "$MEASUREMENT_PEER_IP" \
  --duration-seconds 10 \
  --out .synthran/research/capacity.json
```

The resulting reference capacity belongs to that accepted network context. Recalibrate after network/resource changes that can alter the path.

## Controlled load

Loaded conditions use either an absolute target bitrate or a fraction of the calibrated reference capacity. The research runtime records the requested target and transfer evidence so analysis can reject runs where the treatment was not actually sustained.

The load server/client lifecycle must be run-owned and exact. A previous iperf server process is not silently adopted as current measurement infrastructure.

## Routing proof

The research path should demonstrate that traffic is associated with the current UE/PDU network boundary rather than generic host reachability. Current route/PDU evidence is preferred over historical addresses.

Never reuse a PDU address merely because it appeared in a previous accepted run.

## Physical backend

The physical deterministic workload proves a route-bound user plane through the selected physical UE interface. That is necessary but not yet sufficient for controlled research-campaign parity.

Before physical campaigns are claimed, the physical research implementation must establish an accepted contract for:

- external peer selection relative to the physical PDU path;
- capacity calibration;
- load generation bound to the selected UE interface;
- achieved-load validation;
- counter/probe timing validity;
- run-owned server/client cleanup;
- post-measurement path reproof.

Until that evidence exists, the published `synthran research` controlled-load workflow remains the accepted virtual research path.

## Evidence to preserve

For every calibration or loaded run preserve enough information to reconstruct:

```text
network/run identity
peer identity/address at measurement time
UE/PDU path identity
server port and transport
calibration duration/result
configured load target
achieved transfer evidence
measurement timing
cleanup result
artifact hashes
```

The peer address may be private runtime evidence; public summaries should expose only what is required for reproducibility and privacy policy.
