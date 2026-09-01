# Architecture

SynthRAN is an experiment-orchestration, measurement, and evidence layer. It does **not** own provider selection or deploy/repair 5G infrastructure. The pinned `5g-Ansible` machine API is the sole authority for SLICES provider context, reservation, POS, Kubernetes, core, RAN, radio/RU, UE activation, deployment progress, and deployment teardown.

## System boundary

```text
experiment request
        |
        v
     SynthRAN
  orchestration only
        |
        v
 thin FiveGAdapter
        |
        v
    5g-Ansible
 SLICES provider context
 reservation / POS / Kubernetes
 core / RAN / RU / UE / teardown
 semantic deployment progress
        |
        +-------------------------+
        |                         |
        v                         v
 upstream deployment manifest   generated inventory
        |                         |
        +------------+------------+
                     v
             SynthRAN observation
             + experiment runtime
                     |
          +----------+----------+
          |                     |
       RFSIM                 physical R2Lab
          |                     |
          +----------+----------+
                     v
              Amber + measurement
                     |
              JSONL / Parquet
              acceptance evidence
```

`rfsim` and `r2lab` are upstream platform selections, not separate SynthRAN deployment frameworks.

## Public command boundary

The installed interface remains intentionally small:

```text
synthran run ...
synthran doctor ...
synthran calibrate ...
synthran inspect ...
synthran analyze ...
synthran release ...
synthran deps ...
synthran dev ...
```

`run` describes provider intent and requested topology to 5g-Ansible, consumes upstream artifacts, observes the live path, executes the workload, records evidence, and cleans only experiment-owned state. `doctor` calls upstream `capabilities` and `plan`; it does not mutate provider or deployment state. `calibrate`, `inspect`, and `analyze` operate on accepted deployment/experiment evidence.

## Deployment boundary

SynthRAN talks to the pinned upstream machine interface:

```text
bin/fiveg capabilities
bin/fiveg plan
bin/fiveg up
bin/fiveg status
bin/fiveg down
bin/fiveg scenario
```

The native request schema is `fiveg/deployment/v1`. The upstream manifest schema is `fiveg/deployment-manifest/v1`.

Provider intent is part of the native request. With provider management enabled, 5g-Ansible selects the requested SLICES project, reuses or creates the named experiment, acquires the Post5G network identity, persists that identity in upstream state/manifest, and revalidates it on resume. SynthRAN consumes that evidence; it does not reproduce the provider lifecycle.

SynthRAN deliberately has no second support matrix for cores, RANs, radios, or physical UEs. Topology validation belongs to 5g-Ansible. SynthRAN may impose experiment-specific acceptance requirements after deployment—for example, the current RFSIM Amber experiment requires a live `tun_srsue1` PDU path—but that is not a deployment-support restriction.

## Deployment progress boundary

5g-Ansible is also the sole source of deployment progress semantics. A caller may request the upstream event channel, which emits versioned JSONL records using:

```text
fiveg/event/v1
```

The machine-process streams have distinct purposes:

```text
stdout  -> one final versioned machine result
stderr  -> optional fiveg/event/v1 progress + non-event failure diagnostics
logs    -> detailed Ansible/provider deployment evidence owned by 5g-Ansible
```

The thin `FiveGAdapter` relays recognized `fiveg/event/v1` records into the SynthRAN run event stream. SynthRAN does **not** parse Ansible `PLAY`, `TASK`, handler, host-change, or module-result text and does not maintain a parallel dictionary of Ansible task labels.

The upstream progress channel describes only upstream-owned work such as provider context, SLICES reservation, dependency preparation, physical preparation, 5G deployment, scenarios, and cleanup. Amber source generation, experiment transport, measurements, and scientific acceptance remain SynthRAN events.

Provider-assigned subnet/LB/expiration values are deployment evidence, not progress constants. They remain dynamically supplied in upstream state/manifest and are never hard-coded into the event renderer.

## RFSIM experiment path

After 5g-Ansible reports the deployment ready, SynthRAN observes exactly one current UE pod and its accepted PDU identity. It does not patch the UE Deployment, restart the gNB/UE, or reconcile radio processes.

The current experiment-local transport is:

```text
Amber publishers
-> controller loopback SSH forward
-> core-local kubectl port-forward
-> transient Python relay inside the existing UE container
-> outbound socket bound to tun_srsue1 and the accepted PDU address
-> exact core-address counted ingress
-> run-owned central MQTT broker
-> collector
```

The transient relay and forwards are experiment-owned processes. If the route to the central target does not already use `tun_srsue1`, SynthRAN may add one exact `/32` route with `ip route add`; it never replaces an existing route and removes the route only when the run created it.

## Physical R2Lab experiment path

5g-Ansible supplies the selected physical UE and its SSH facts in the generated inventory. SynthRAN does not maintain a separate R2Lab resource, gNB, or UE lifecycle.

The Amber path is:

```text
Amber publishers
-> controller SSH forward through the selected physical UE
-> UE route proven through wwan0
-> counted ingress bound to the exact core address
-> run-owned central MQTT broker
-> collector
```

The physical experiment records `wwan0` byte counters and re-proves the route after delivery. Provider context, reservation, radio configuration, modem activation, registration, and PDU establishment remain upstream responsibilities.

## Experiment-owned Kubernetes resources

SynthRAN creates only workload resources it actually owns. The shared Kubernetes resource is a run-labelled central Mosquitto Deployment and ConfigMap on the core node. Experiment code does not inject containers, volumes, labels, or annotations into 5g-Ansible-owned UE/RAN Deployments.

Cleanup selects the exact experiment run label and then re-observes the upstream network. Historical evidence never authorizes infrastructure mutation.

## Research boundary

Controlled measurements operate on an already accepted deployment. They may create bounded measurement-local state such as an exact target route or an owned iperf3 server, then must remove that state and re-prove the network identity. They never repair the 5G deployment.

Capacity calibration follows the same rule: verify the existing path, measure, and remove only measurement-owned state.

## Evidence model

```text
5g-Ansible
  provider identity/network
  deployment manifest
  generated inventory
  upstream state directory
  fiveg/event/v1 progress
  detailed deployment logs

SynthRAN
  network-evidence.json
  experiment-evidence.json
  research-summary-v2.json
  telemetry.jsonl / telemetry.parquet
  probe / load / network-sample evidence
  run event evidence
```

Upstream artifacts establish provider and deployment provenance. SynthRAN evidence establishes observed path state and scientific/workload acceptance. Neither substitutes for fresh observation when a live run begins.

## Source layout

```text
synthran/adapters/fiveg.py             thin 5g-Ansible machine/event adapter
synthran/lifecycle.py                  experiment orchestration
synthran/run_events.py                 experiment events + upstream event relay
synthran/network/runtime.py            read-only network verification/evidence
synthran/experiment/observe.py         read-only UE/PDU observation
synthran/experiment/rfsim.py           RFSIM Amber experiment
synthran/experiment/rfsim_transport.py experiment-local RFSIM transport
synthran/experiment/physical.py        physical Amber experiment
synthran/iot_edge_transport.py         physical UE experiment transport
synthran/research/                     measurement + analysis
synthran/privacy.py                    repository/privacy controls
```

There is no SynthRAN provider controller, no `synthran/r2lab/` controller, no `deploy/ansible/` wrapper tree, no local network resource-preparation layer, and no direct Ansible streaming executor.

## Design rules

- 5g-Ansible is the sole provider, 5G deployment, and deployment-progress authority.
- SynthRAN passes native provider/topology requests instead of maintaining parallel controller logic.
- Deployment artifacts and semantic progress are consumed, not reconstructed from Ansible text.
- Live infrastructure control is never inferred from historical evidence.
- Experiment mutation is bounded to run-owned workload/measurement state.
- Existing upstream Deployments are not patched to make an experiment work.
- Exact cleanup is followed by read-only reproof.
- Network readiness and workload/scientific acceptance are distinct claims.
