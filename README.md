<div align="center">

# SynthRAN

**Reproducible IoT experiments across virtual and physical open 5G radio paths.**

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](pyproject.toml)
[![5G](https://img.shields.io/badge/5G-srsRAN%20%2B%20Open5GS-6C63FF)](docs/architecture.md)
[![IoT](https://img.shields.io/badge/IoT-Contiki--NG%20%2B%20Cooja-00A86B)](docs/experiment.md)
[![License](https://img.shields.io/github/license/RA-Nayreed/SynthRAN)](LICENSE)

</div>

SynthRAN turns a collection of provider, radio, 5G, IoT, and measurement tools into one evidence-producing experiment. It owns orchestration, exact resource authority, validation, deterministic workload execution, sanitized progress, cleanup, and reproducibility records. It does not reimplement Open5GS, srsRAN, Contiki-NG, Cooja, Mosquitto, iperf3, SLICES, or R2Lab.

## One command surface

There is one installed executable and one lifecycle command:

```text
synthran run --radio rfsim ...
synthran run --radio r2lab ...
```

The supported top-level interface is intentionally small:

```text
run       execute a complete experiment lifecycle
 doctor    perform read-only readiness checks
inspect   show capabilities or persisted run evidence
logs      read or follow the unified run event stream
stop      release resources owned by one run
research  controlled measurement and campaign tools
deps      synchronize pinned external dependencies
dev       repository maintenance commands
```

RFSIM and R2Lab are backends of the same product, not separate workflows. Backend-specific resource and hardware functions remain internal implementation boundaries.

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

A run creates or reuses its provider experiment and acquires the required Post5G prefix. By default the provider experiment name is the run ID.

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

The run prepares compute resources, verifies authority, deploys Open5GS and srsRAN/RFSIM, proves the user path, executes the deterministic ten-sensor workload, and persists acceptance evidence.

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

The physical path reuses the active R2Lab lease, claims only the selected radio and UE, reconciles the selected SLICES/Open5GS foundation, stages and starts the pinned N3xx gNB, proves N2, activates the selected UE through pinned `5g_ansible` roles, proves the PDU/user plane, runs the same deterministic IoT workload, then releases exact run-owned physical resources unless `--keep-resources` was requested.

## Live progress and logs

All long Ansible work uses the same sanitized streaming implementation. RFSIM deployment, physical Open5GS work, and R2Lab UE setup/connect/stop therefore expose the same task filtering, failures, and heartbeats.

Every run also writes the same messages to:

```text
.synthran/events/<run-id>.jsonl
```

Read or follow them with:

```zsh
synthran logs --run-id "$RUN_ID"
synthran logs --run-id "$RUN_ID" --follow
```

`--quiet` suppresses terminal progress but still records the event stream.

## Inspect and cleanup

```zsh
synthran inspect --run-id "$RUN_ID"
synthran inspect --radio r2lab
synthran stop --run-id "$RUN_ID"
```

Physical cleanup is authority-bound and exact. SynthRAN does not use broad radio power-off, wildcard deletion, or guessed ownership.

## Research

The controlled research tools are top-level commands:

```text
synthran research plan
synthran research run
synthran research calibrate
synthran research campaign-plan
synthran research campaign-run
synthran research analyze
```

The published controlled-load campaign implementation is currently validated against the accepted RFSIM network-evidence path. Physical deterministic workload execution is implemented, but physical controlled-load campaign parity is not claimed until it has its own accepted evidence.

Current accepted measurements and interpretation limits are in [`docs/results.md`](docs/results.md).

## Deterministic workload

The reference workload is:

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

The virtual backend carries that path through srsUE/RFSIM. The physical backend substitutes the selected physical UE and N3xx radio while preserving experiment-level workload and evidence semantics.

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

Accepted virtual evidence includes Open5GS, srsRAN, RFSIM, deterministic Cooja telemetry, external-peer capacity calibration, controlled UDP load, fixed-window measurement, blocked campaigns, and offline paired analysis.

R2Lab implements the corresponding physical lifecycle through exact hardware authority, N3xx gNB/N2, selected Quectel UE activation, PDU/user-plane proof, deterministic workload execution, and exact cleanup. A physical capability is considered established only when current accepted evidence proves it; historical observations are not upgraded into current authority or scientific results.
