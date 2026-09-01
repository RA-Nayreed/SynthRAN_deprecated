# R2Lab platform integration

R2Lab is a **5g-Ansible platform**, not a SynthRAN deployment backend. A physical experiment is requested through the same `synthran run` surface, but reservation, POS allocation, radio/RU setup, gNB deployment, UE setup/activation, registration, PDU establishment, and teardown are owned by 5g-Ansible.

## Boundary

```text
SynthRAN experiment request
        |
        v
fiveg/deployment/v1
        |
        v
5g-Ansible -- platform=r2lab
        |
        +--> reservation / POS
        +--> Kubernetes / core
        +--> physical RAN / RU
        +--> physical UE activation
        +--> deployment manifest + inventory
        |
        v
SynthRAN physical Amber experiment
```

SynthRAN no longer keeps a parallel R2Lab claim database, gNB staging engine, modem scripts, physical reconciliation state machine, or R2Lab-specific command family.

## Requesting a physical run

A representative request remains:

```zsh
synthran run \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --run-id "$RUN_ID"
```

The CLI translates this intent into the native 5g-Ansible deployment schema. Provider/R2Lab credentials and SSH policy required by the selected upstream platform are passed to the upstream request rather than reimplemented by SynthRAN.

Exact accepted radios, RANs, cores, and UE profiles are determined by the pinned 5g-Ansible capabilities/normalization contract. SynthRAN does not maintain another support list.

## Upstream artifacts

After deployment, SynthRAN consumes:

- `fiveg/deployment-manifest/v1` provenance;
- the generated `hosts.ini` inventory;
- physical UE entries and SSH parameters in that inventory;
- the upstream state directory as an opaque deployment-owned artifact.

SynthRAN does not generate or copy a second infrastructure inventory.

## Physical Amber path

The current physical workload path is experiment-local:

```text
Amber publishers
-> controller SSH local forward through the selected physical UE
-> UE route through wwan0
-> counted ingress bound to the exact core-node address
-> run-owned central Mosquitto on the core
-> collector
-> JSONL / deterministic Parquet
```

Before the workload, SynthRAN resolves the selected UE from the upstream inventory and proves that the core destination is routed through `wwan0`. During the run it records physical interface byte counters. Acceptance requires the expected Amber source/transport reconciliation and an increase in physical UE TX bytes.

The counted ingress is bound to the exact core IP, not core loopback and not `0.0.0.0`. The SSH forwarding connection is therefore opened by the physical UE toward the core over the already-provisioned user plane.

## Experiment-owned resources

SynthRAN may create only resources needed by the workload:

- the run-labelled central MQTT ConfigMap/Deployment;
- controller/SSH forwarding processes;
- counted-ingress process and run workspace;
- Amber publisher/collector processes;
- measurement-local state when a separately supported research mode requires it.

It must not change radio power, gNB replicas, Open5GS configuration, UE modem state, PDU setup, or upstream deployment ownership.

## Cleanup

Physical experiment cleanup is intentionally narrower than infrastructure teardown:

1. stop the run-owned publisher/collector/forward processes;
2. remove the run-owned remote experiment workspace;
3. delete Kubernetes objects carrying the exact experiment run label;
4. prove the controller and core experiment ports are closed;
5. re-prove the selected physical UE route.

Infrastructure teardown, reservation release, radio/UE release, and POS cleanup remain 5g-Ansible responsibilities (for example through its `down` machine verb when the enclosing SynthRAN lifecycle requests teardown).

## What SynthRAN does not claim

A successful physical Amber delivery proves the selected deterministic workload over the observed physical user plane. It does not by itself prove controlled-load research parity with RFSIM. Physical campaign/load generation requires its own reviewed measurement target, timing, capacity, and cleanup evidence.

## Safety invariants

- physical UE identity comes only from the upstream inventory;
- strict SSH settings are preserved from upstream facts;
- current route observation, not stale evidence, proves the experiment path;
- no global or guessed hardware cleanup;
- no direct Ansible role execution from SynthRAN;
- no local R2Lab ownership database;
- experiment cleanup targets only experiment-owned state;
- 5G infrastructure mutation stays behind the 5g-Ansible boundary.
