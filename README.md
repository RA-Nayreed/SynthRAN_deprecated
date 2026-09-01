<div align="center">

# SynthRAN

**Reproducible Amber experiments over 5G deployments owned by 5g-Ansible.**

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](pyproject.toml)
[![License](https://img.shields.io/github/license/RA-Nayreed/SynthRAN)](LICENSE)

</div>

SynthRAN is an experiment orchestrator, not a 5G deployment framework. It submits a native `fiveg/deployment/v1` request to the pinned `5g-Ansible` machine API, consumes the resulting manifest and generated inventory, observes the experiment path, runs deterministic Amber workloads and controlled measurements, and persists scientific evidence.

`5g-Ansible` owns SLICES provider context, reservation, POS, Kubernetes, core, RAN, RU, physical UE setup, deployment state, and teardown. SynthRAN does not keep a second provider controller, topology support matrix, or infrastructure lifecycle.

## Architecture

```text
experiment request
        ↓
SynthRAN orchestration
        ↓
FiveGAdapter
        ↓
5g-Ansible
  SLICES provider context
  reservation / POS / Kubernetes
  core / RAN / RU / UE / teardown
        ↓
upstream manifest + generated inventory
        ↓
SynthRAN read-only path observation
        ↓
Amber workload + measurement
        ↓
scientific evidence / acceptance
```

RFSIM and R2Lab are upstream platform selections. There is no separate `synthran/r2lab` subsystem.

## One command surface

```text
run        request an upstream deployment and execute an experiment
 doctor    validate a native deployment request through 5g-Ansible plan
calibrate  measure reference capacity on an accepted RFSIM path
inspect    show upstream capabilities or persisted run state
analyze    analyze a completed campaign
release    stop one deployment through 5g-Ansible
deps       synchronize pinned direct dependencies
dev        repository maintenance commands
```

## Install

```zsh
git clone https://github.com/RA-Nayreed/SynthRAN.git
cd SynthRAN
conda env create -f environment.yml
conda activate synthran
python -m pip install --no-deps -e .
synthran deps sync
synthran --help
```

Authenticate the provider tools required by the pinned 5g-Ansible checkout and prepare strict SSH host-key state. SynthRAN passes provider intent upstream; it does not select or create SLICES experiments itself.

```zsh
export SYNTHRAN_SLICES_PROJECT='PROJECT_NAME'
export SYNTHRAN_SLICES_KNOWN_HOSTS="$PWD/.synthran/known_hosts"
```

## Virtual run

A virtual run asks 5g-Ansible to own the SLICES project/experiment context and create the requested RFSIM deployment. SynthRAN then consumes `.synthran/runs/<run-id>/manifest.json` and `hosts.ini`, proves the current PDU path, runs Amber, and saves experiment evidence.

```zsh
export RUN_ID='virtual-001'

synthran doctor \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --slices-project "$SYNTHRAN_SLICES_PROJECT" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"

synthran run \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --run-id "$RUN_ID" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT"
```

Unless `--slices-experiment` is supplied, the upstream provider experiment name defaults to the run ID. The RFSIM deployment is retained after acceptance so controlled measurements can reuse the same upstream deployment. Use `synthran release --run-id "$RUN_ID"` when it should be stopped.

## Controlled run on an accepted RFSIM path

Controlled measurements reuse the upstream deployment and generated inventory. They may create and remove experiment-local routes, probes, MQTT resources, and bounded load instrumentation, but they do not redeploy or repair the 5G network.

```zsh
synthran run \
  --campaign-id ambient-study-01 \
  --network-run-id virtual-001 \
  --run-id ambient-baseline-01 \
  --condition baseline \
  --iot-profile ambient-v1 \
  --seed 424242 \
  --sensor-period 10 \
  --warmup-seconds 30 \
  --duration-seconds 180 \
  --sample-interval 1 \
  --probe-interval 1 \
  --probe-target 198.51.100.1 \
  --inventory .synthran/runs/virtual-001/hosts.ini
```

Use `--plan` to render a controlled request without executing it.

## Campaigns

```zsh
synthran run \
  --campaign-id ambient-ran-study-01 \
  --network-run-id virtual-001 \
  --seeds 424242,424243,424244 \
  --conditions baseline,load50=0.5,load80=0.8,load95=0.95 \
  --campaign-seed 20260830 \
  --iot-profile ambient-v1 \
  --inventory .synthran/runs/virtual-001/hosts.ini \
  --probe-target 198.51.100.1 \
  --reference-capacity-bps REFERENCE_CAPACITY
```

The persisted campaign schedule is immutable. Analysis consumes completed run summaries and does not execute infrastructure operations.

## Capacity calibration

```zsh
synthran calibrate \
  --inventory .synthran/runs/virtual-001/hosts.ini \
  --network-run-id virtual-001 \
  --target 198.51.100.1 \
  --out .synthran/capacity/virtual-001.json
```

Calibration verifies the accepted path, starts bounded measurement instrumentation, and removes only the route/server state created for that measurement.

## Analyze

```zsh
synthran analyze \
  --campaign .synthran/campaigns/ambient-ran-study-01.json \
  --out .synthran/analysis/ambient-ran-study-01.json
```

## Physical R2Lab run

For a physical run, the R2Lab lease must satisfy the selected upstream policy. SynthRAN passes provider intent, requested RU/UE, R2Lab identity, and strict SSH facts to 5g-Ansible. 5g-Ansible performs provider setup, reservation/POS work, physical deployment, and UE setup; SynthRAN then runs Amber through the selected UE using generated-inventory facts.

```zsh
export SYNTHRAN_R2LAB_SLICE='YOUR_R2LAB_SLICE'
export RUN_ID='physical-001'

synthran doctor \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"

synthran run \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --run-id "$RUN_ID" \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT"
```

Unless `--keep-resources` is supplied, a successful physical experiment ends with `5g-Ansible down` for that deployment ID. SynthRAN does not implement provider selection, reservation, radio power control, UE activation, gNB staging, or N2 convergence.

## Live progress and logs

`synthran run` emits one normalized operator stream and persists structured events to:

```text
.synthran/events/<run-id>.jsonl
```

Typical stages are:

```text
[synthran] → network
[synthran]   ✓ 5g-Ansible provider/deployment ready
[synthran] → workload
[synthran]   ✓ Amber transport accepted
[synthran] → acceptance
[synthran] → cleanup
```

Detailed upstream deployment artifacts and experiment artifacts remain under their run directories. `--quiet` suppresses terminal rendering while preserving evidence.

## Inspect and release

```zsh
synthran inspect --run-id "$RUN_ID"
synthran inspect --radio r2lab
synthran release --run-id "$RUN_ID"
```

`release` delegates to the pinned 5g-Ansible `down` machine verb. It does not reconstruct infrastructure ownership inside SynthRAN.

## Amber workload boundary

For `ambient-v1`, Amber models the Ambient-IoT source side, including energy state, framed access, link behavior, collisions, and capture/SIC. Decoded Amber events are transported through the deployed 5G user plane to the central collector. Amber tags are not represented as 5G NR UEs and SynthRAN does not claim that Amber injects an Ambient-IoT waveform into the RAN.

## Documentation

| Document | Purpose |
| --- | --- |
| [`docs/architecture.md`](docs/architecture.md) | system boundaries and data flow |
| [`docs/operator-guide.md`](docs/operator-guide.md) | operating procedure |
| [`docs/experiment.md`](docs/experiment.md) | deterministic workload and measurement protocol |
| [`docs/results.md`](docs/results.md) | accepted research evidence and interpretation limits |
| [`docs/dependencies.md`](docs/dependencies.md) | pinned dependency policy |
| [`docs/security.md`](docs/security.md) | credentials, privacy, and mutation safety |
| [`docs/development.md`](docs/development.md) | validation and contribution workflow |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | repository invariants |

## Capability boundary

5g-Ansible defines provider, core, RAN, platform, radio/RU, UE, reservation, and deployment capabilities. SynthRAN does not duplicate those capability tables.

SynthRAN defines experiment acceptance requirements. The current RFSIM Amber experiment requires a proven `tun_srsue1` PDU path. The current physical Amber experiment requires the selected upstream UE to expose the expected user-plane route. Those are experiment requirements, not claims about what 5g-Ansible can deploy.
