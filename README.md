<div align="center">

# SynthRAN

**Reproducible IoT experiments across virtual and physical open 5G radio paths.**

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](pyproject.toml)
[![5G](https://img.shields.io/badge/5G-srsRAN%20%2B%20Open5GS-6C63FF)](docs/architecture.md)
[![License](https://img.shields.io/github/license/RA-Nayreed/SynthRAN)](LICENSE)

</div>

SynthRAN turns provider, radio, 5G, IoT, and measurement tools into one evidence-producing experiment. It owns orchestration, exact resource authority, validation, deterministic workload execution, sanitized progress, cleanup, and reproducibility records. It does not reimplement Open5GS, srsRAN, AMBER, Mosquitto, iperf3, SLICES, or R2Lab.

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

The run prepares compute infrastructure, verifies provider/resource authority, deploys Open5GS and srsRAN/RFSIM, verifies the live 5G session state, executes the selected IoT workload, proves the workload transport through the live UE PDU path, and persists acceptance evidence.

## Controlled run on an accepted RFSIM network

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

The physical backend reuses the active R2Lab lease, claims only the selected radio and UE, reconciles the selected SLICES/Open5GS foundation, stages and starts the pinned N3xx gNB, proves N2, activates the selected UE through pinned `5g_ansible` roles, verifies registration/PDU/user-plane state, runs the selected IoT workload, then releases exact run-owned physical resources unless `--keep-resources` was requested.

## Canonical live progress

`synthran run` is the only live operator stream. Lifecycle state, meaningful Ansible progress, AMBER/research progress, failures, and acceptance all use the same prefix and renderer:

```text
[synthran] → infrastructure
[synthran]   … node bootstrap · 2m
[synthran] ✓ infrastructure
[synthran] → network
[synthran]   ✓ gNB cell active
[synthran]   ✓ PDU session · 12.1.0.x
[synthran] ✓ network: READY
[synthran] → workload
[synthran]   ✓ PDU-bound TCP transport gate passed
[synthran]   ✓ transport · published=... · received=... · loss=0 · duplicates=0
[synthran] ✓ experiment accepted
```

Raw Ansible PLAY/TASK chatter, skipped tasks, routine package/configuration details, and internal offline/live validator reports are not promoted to the operator stream. Long meaningful tasks produce heartbeats, and failures retain a concise sanitized reason. Detailed component logs remain available as forensic artifacts when a stage fails.

Every run also persists the canonical structured event evidence to:

```text
.synthran/events/<run-id>.jsonl
```

There is no separate public live-log command. `--quiet` suppresses terminal rendering while preserving run evidence.

## Inspect and release

```zsh
synthran inspect --run-id "$RUN_ID"
synthran inspect --radio r2lab
synthran release --run-id "$RUN_ID"
```

Normal RFSIM workloads clean up their transient experiment resources inside the run while the accepted network epoch may remain available for controlled measurements. `release` is primarily for persistent physical/provider ownership after an interrupted or intentionally retained R2Lab run. Physical release is authority-bound and exact. SynthRAN does not use broad radio power-off, wildcard deletion, or guessed ownership.

The controlled-load implementation is currently validated against the accepted RFSIM network-evidence state. Physical deterministic workload execution is implemented, but physical controlled-load campaign parity is not claimed until it has its own accepted evidence.

Current accepted measurements and interpretation limits are in [`docs/results.md`](docs/results.md).

## Ambient-IoT workload boundary

For `ambient-v1`, AMBER models the Ambient-IoT source side, including energy state, framed access, backscatter/link behavior, collisions, and capture/SIC. Decoded AMBER events are then transported through SynthRAN's 5G user plane to the central collector. AMBER tags are not represented as 5G NR UEs and SynthRAN does not claim that AMBER injects an Ambient-IoT waveform into srsRAN.

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

R2Lab implements the corresponding physical lifecycle through exact hardware authority, N3xx gNB/N2, selected Quectel UE activation, PDU/user-plane verification, deterministic workload execution, and exact cleanup. A physical capability is considered established only when current accepted evidence proves it; historical observations are not upgraded into current authority or scientific results.
