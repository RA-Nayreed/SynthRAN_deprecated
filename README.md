<div align="center">

# SynthRAN

**Reproducible IoT experiments across virtual and physical open 5G radio paths.**

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](pyproject.toml)
[![5G](https://img.shields.io/badge/5G-srsRAN%20%2B%20Open5GS-6C63FF)](docs/architecture.md)
[![IoT](https://img.shields.io/badge/IoT-Contiki--NG%20%2B%20Cooja-00A86B)](docs/experiment.md)
[![License](https://img.shields.io/github/license/RA-Nayreed/SynthRAN)](LICENSE)

</div>

SynthRAN turns a collection of provider, radio, 5G, IoT, and measurement tools into one evidence-producing experiment. It owns orchestration, exact resource authority, validation, deterministic workload execution, sanitized progress, cleanup, and reproducibility records. It does not reimplement Open5GS, srsRAN, Contiki-NG, Cooja, Mosquitto, iperf3, SLICES, or R2Lab.

## One command surface

There is one installed executable and one experiment execution verb:

```text
synthran run ...
```

The supported top-level interface is intentionally small:

```text
run        execute one experiment or an immutable campaign
doctor     perform read-only readiness checks
calibrate  measure reference RAN/UE-path capacity
inspect    show capabilities or persisted run evidence
logs       read or follow the unified run event stream
analyze    analyze a completed persisted campaign
release    release persistent resources owned by one physical run
deps       synchronize pinned external dependencies
dev        repository maintenance commands
```

RFSIM and R2Lab are backends of the same product, not separate workflows. Backend-specific resource, radio, measurement, and campaign functions remain internal implementation boundaries.

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

A SLICES project must already exist and the provider CLI must be authenticated. Export the project and owner once:

```zsh
export SYNTHRAN_SLICES_PROJECT='PROJECT_NAME'
export SYNTHRAN_OWNER='YOUR_SLICES_USERNAME'
```

A full lifecycle run creates or reuses its provider experiment and acquires the required Post5G prefix. By default the provider experiment name is the run ID.

## Virtual run

```zsh
export RUN_ID='virtual-001'

synthran doctor \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3

synthran run \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --run-id "$RUN_ID" \
  --owner "$SYNTHRAN_OWNER" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT"
```

The run prepares compute resources, verifies authority, deploys Open5GS and srsRAN/RFSIM, proves the user path, executes the selected IoT workload, and persists acceptance evidence.

## Controlled run on an accepted RFSIM path

A controlled measurement is still a `run`; it reuses a previously accepted network rather than creating another command hierarchy.

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
  --inventory .synthran/preparations/virtual-001/hosts.ini
```

`ambient-v1` additionally accepts explicit `--energy-power-scale` and `--energy-node-variation` treatments. The selected treatment is part of the immutable source identity and evidence.

Add `--plan` to a controlled single run to render its immutable request without execution.

## Campaigns

Campaign scheduling and execution use the same `run` verb. A new deterministic schedule is persisted automatically before execution:

```zsh
synthran run \
  --campaign-id ambient-ran-study-01 \
  --network-run-id virtual-001 \
  --seeds 424242,424243,424244 \
  --conditions baseline,load50=0.5,load80=0.8,load95=0.95 \
  --campaign-seed 20260830 \
  --iot-profile ambient-v1 \
  --inventory .synthran/preparations/virtual-001/hosts.ini \
  --probe-target 198.51.100.1 \
  --reference-capacity-bps REFERENCE_CAPACITY
```

Use `--plan` to persist and display the deterministic campaign schedule without executing it. Use `--campaign PATH` later to execute the exact persisted schedule rather than rebuilding it from command-line arguments.

## Capacity calibration

`calibrate` means RAN/UE-path capacity only. It is independent of AMBER energy treatment:

```zsh
synthran calibrate \
  --inventory .synthran/preparations/virtual-001/hosts.ini \
  --network-run-id virtual-001 \
  --target 198.51.100.1 \
  --out .synthran/capacity/virtual-001.json
```

The resulting capacity evidence can be supplied to fractional loaded conditions with `--reference-capacity-bps`.

## Analyze

Analysis consumes a persisted campaign schedule and completed run summaries; it does not execute an experiment:

```zsh
synthran analyze \
  --campaign .synthran/campaigns/ambient-ran-study-01.json \
  --out .synthran/analysis/ambient-ran-study-01.json
```

## Physical R2Lab run

R2Lab additionally requires an active lease, an allowed N3xx radio/UE pair, the R2Lab slice identity, and strict known-hosts state.

```zsh
export SYNTHRAN_R2LAB_SLICE='YOUR_R2LAB_SLICE'
export SYNTHRAN_SLICES_KNOWN_HOSTS="$PWD/.synthran/r2lab/known_hosts"
export RUN_ID='physical-001'

synthran doctor \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3

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

The physical path reuses the active R2Lab lease, claims only the selected radio and UE, reconciles the selected SLICES/Open5GS foundation, stages and starts the pinned N3xx gNB, proves N2, activates the selected UE through pinned `5g_ansible` roles, proves the PDU/user plane, runs the selected IoT workload, then releases exact run-owned physical resources unless `--keep-resources` was requested.

## Live progress and logs

All long Ansible work uses the same sanitized streaming implementation. RFSIM deployment, physical Open5GS work, and R2Lab UE setup/connect/stop therefore expose the same task filtering, failures, and heartbeats.

Every lifecycle run also writes the same messages to:

```text
.synthran/events/<run-id>.jsonl
```

Read or follow them with:

```zsh
synthran logs --run-id "$RUN_ID"
synthran logs --run-id "$RUN_ID" --follow
```

`--quiet` suppresses terminal progress but still records the event stream.

## Inspect and release

```zsh
synthran inspect --run-id "$RUN_ID"
synthran inspect --radio r2lab
synthran release --run-id "$RUN_ID"
```

Normal RFSIM runs clean up their transient experiment resources inside the run itself, so `release` is primarily for persistent physical/provider ownership after an interrupted or intentionally retained R2Lab run. Physical release is authority-bound and exact. SynthRAN does not use broad radio power-off, wildcard deletion, or guessed ownership.

The controlled-load implementation is currently validated against the accepted RFSIM network-evidence path. Physical deterministic workload execution is implemented, but physical controlled-load campaign parity is not claimed until it has its own accepted evidence.

Current accepted measurements and interpretation limits are in [`docs/results.md`](docs/results.md).

## Deterministic workload

The historical reference workload is:

```text
10 deterministic Contiki-NG/Cooja sensors
-> RPL/6LoWPAN border router
-> counted ingress
-> UE-side MQTT handoff
-> 5G user plane
-> Open5GS UPF
-> run-owned central MQTT collector
-> canonical JSONL
-> deterministic Parquet
```

AMBER profiles use the same experiment-level evidence boundary without claiming Contiki, RPL, or 6LoWPAN semantics. The virtual backend carries the selected workload through srsUE/RFSIM. The physical backend substitutes the selected physical UE and N3xx radio where that profile is supported.

## Documentation

| Document | Purpose |
| --- | --- |
| [`docs/architecture.md`](docs/architecture.md) | system boundaries and data flow |
| [`docs/backend-contract.md`](docs/backend-contract.md) | RFSIM/R2Lab parity and safety rules |
| [`docs/operator-guide.md`](docs/operator-guide.md) | complete operating procedure |
| [`docs/experiment.md`](docs/experiment.md) | deterministic workload and measurement protocol |
| [`docs/r2lab-integration.md`](docs/r2lab-integration.md) | physical backend details |
| [`docs/results.md`](docs/results.md) | canonical accepted research evidence |
| [`docs/dependencies.md`](docs/dependencies.md) | pinned upstream dependencies |
| [`docs/security.md`](docs/security.md) | credentials, privacy, and mutation safety |
| [`docs/development.md`](docs/development.md) | validation and contribution workflow |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | repository invariants |

## Capability boundary

Accepted virtual evidence includes Open5GS, srsRAN, RFSIM, deterministic telemetry, external-peer capacity calibration, controlled UDP load, fixed-window measurement, blocked campaigns, and offline paired analysis.

R2Lab implements the corresponding physical lifecycle through exact hardware authority, N3xx gNB/N2, selected Quectel UE activation, PDU/user-plane proof, deterministic workload execution, and exact cleanup. A physical capability is considered established only when current accepted evidence proves it; historical observations are not upgraded into current authority or scientific results.
