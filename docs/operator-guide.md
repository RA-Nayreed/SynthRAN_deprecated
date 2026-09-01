# Operator guide

This is the supported procedure for running SynthRAN over a deployment owned by the pinned 5g-Ansible machine API.

## 1. Install

```zsh
cd ~/SynthRAN
conda activate synthran
python -m pip install --no-deps -e .
synthran deps sync
synthran --help
```

For repository validation:

```zsh
python -m unittest discover -s tests -v
synthran dev privacy scan --worktree
git diff --check
git status --short
```

There is one installed executable. Do not operate infrastructure by calling historical backend helpers or deleted internal command groups.

## 2. Provider and SSH prerequisites

Authenticate the provider tooling used by 5g-Ansible and choose an existing SLICES project:

```zsh
slices auth login
slices project list
export SYNTHRAN_SLICES_PROJECT='PROJECT_NAME'
```

SynthRAN does not run `slices project use`, create SLICES experiments, acquire Post5G prefixes, or make POS reservations itself. A full `synthran run` sends provider intent in the native `fiveg/deployment/v1` request. The pinned 5g-Ansible machine API owns selection/reuse/creation and records provider evidence in its manifest.

Unless `--slices-experiment` is supplied, the provider experiment defaults to the SynthRAN run ID. `--slices-duration` controls the requested provider-experiment duration for first creation.

Prepare strict host-key state for experiment observation and physical access:

```zsh
export SYNTHRAN_SLICES_KNOWN_HOSTS='/absolute/path/to/sopnodes_known_hosts'
```

Do not disable strict host-key checking to make a run pass.

## 3. Full validation order on Duckburg

Before mutating live resources, prove the checked-out PR and direct dependencies:

```zsh
cd ~/SynthRAN
git switch purge/thin-fiveg-adapter
git pull --ff-only origin purge/thin-fiveg-adapter
git status --short
git rev-parse HEAD

conda activate synthran
python -m pip install --no-deps -e .
synthran deps sync
synthran --help

python -m unittest discover -s tests -v
synthran dev privacy scan --worktree
git diff --check
git status --short
```

Then validate the provider session and upstream machine request without deployment:

```zsh
slices auth login
slices project list

synthran doctor \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --slices-project "$SYNTHRAN_SLICES_PROJECT" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

Only after those checks pass should live RFSIM acceptance be attempted. Physical R2Lab acceptance comes after RFSIM acceptance.

## 4. Doctor

`doctor` is a structural upstream check. It calls 5g-Ansible `capabilities` and `plan`; it does not select provider state, reserve resources, deploy, or connect a UE.

RFSIM:

```zsh
synthran doctor \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --slices-project "$SYNTHRAN_SLICES_PROJECT" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

Physical R2Lab:

```zsh
export SYNTHRAN_R2LAB_SLICE='YOUR_R2LAB_SLICE'

synthran doctor \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

For the authoritative list of supported cores, RANs, platforms, RUs, and UEs, use:

```zsh
synthran inspect --radio r2lab
```

That output is the pinned 5g-Ansible capability response; SynthRAN has no hardware catalogue of its own.

## 5. Virtual RFSIM run

Use a fresh run ID for acceptance. Do not reuse a failed run ID with different intent.

```zsh
export RUN_ID="rfsim-acceptance-$(date +%Y%m%d-%H%M%S)"

synthran run \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --run-id "$RUN_ID" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

The control flow is:

```text
native deployment request
→ 5g-Ansible provider context
→ 5g-Ansible reservation/POS/deployment
→ upstream ready manifest + inventory
→ SynthRAN read-only PDU-path proof
→ Amber workload
→ experiment evidence
```

SynthRAN does not patch the upstream UE Deployment or restart/reconcile gNB/UE state. The accepted RFSIM deployment remains available for controlled measurements until `synthran release` is called.

Immediately inspect the accepted deployment and preserve its run directory:

```zsh
synthran inspect --run-id "$RUN_ID"
find ".synthran/runs/$RUN_ID" -maxdepth 2 -type f -print | sort
```

When the retained RFSIM deployment is no longer needed:

```zsh
synthran release --run-id "$RUN_ID"
```

## 6. Physical R2Lab run

A physical run delegates the RU, UE, R2Lab identity, provider intent, reservation policy, and deployment to 5g-Ansible. Confirm an appropriate R2Lab lease/reservation window before starting.

```zsh
export SYNTHRAN_R2LAB_SLICE='YOUR_R2LAB_SLICE'
export RUN_ID="r2lab-acceptance-$(date +%Y%m%d-%H%M%S)"

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
  --slices-project "$SYNTHRAN_SLICES_PROJECT" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

After the upstream deployment is ready, SynthRAN resolves the selected UE from the generated inventory, proves the expected user-plane route, runs Amber, and records experiment evidence. It does not implement radio power control, UE activation, gNB staging, N2 convergence, or physical resource ownership.

Unless `--keep-resources` is supplied, an accepted physical run ends by calling 5g-Ansible `down` for the exact deployment ID.

Inspect and preserve the run evidence after completion:

```zsh
synthran inspect --run-id "$RUN_ID"
find ".synthran/runs/$RUN_ID" -maxdepth 2 -type f -print | sort
```

## 7. Live progress

`synthran run` emits one normalized operator stream and persists canonical events to:

```text
.synthran/events/<run-id>.jsonl
```

Typical output is:

```text
[synthran] → network
[synthran]   ✓ 5g-Ansible provider/deployment ready
[synthran] → workload
[synthran]   ✓ Amber transport accepted
[synthran] → acceptance
[synthran] → cleanup
```

Upstream deployment logs remain in the 5g-Ansible state directory. SynthRAN does not re-stream or reinterpret Ansible tasks as a second deployment engine.

## 8. Inspect and release

```zsh
synthran inspect --run-id "$RUN_ID"
synthran release --run-id "$RUN_ID"
```

`inspect` reads persisted evidence and current upstream status where applicable. `release` calls the pinned 5g-Ansible `down` machine verb. It does not reconstruct resource ownership or execute local cleanup playbooks.

Historical evidence is not current mutation authority. Live deployment mutation always belongs to 5g-Ansible.

## 9. Controlled measurements

Controlled measurements operate on an accepted RFSIM deployment and its generated inventory:

```text
.synthran/runs/<network-run-id>/hosts.ini
```

Capacity calibration:

```zsh
export NETWORK_RUN='virtual-001'
export INVENTORY=".synthran/runs/$NETWORK_RUN/hosts.ini"
export MEASUREMENT_PEER_IP='PEER_IPV4'

synthran calibrate \
  --inventory "$INVENTORY" \
  --network-run-id "$NETWORK_RUN" \
  --target "$MEASUREMENT_PEER_IP" \
  --out .synthran/capacity/$NETWORK_RUN.json
```

One controlled Amber run:

```zsh
synthran run \
  --campaign-id ambient-study-01 \
  --network-run-id "$NETWORK_RUN" \
  --run-id ambient-baseline-01 \
  --condition baseline \
  --iot-profile ambient-v1 \
  --seed 424242 \
  --sensor-period 10 \
  --warmup-seconds 30 \
  --duration-seconds 180 \
  --sample-interval 1 \
  --probe-interval 1 \
  --probe-target "$MEASUREMENT_PEER_IP" \
  --inventory "$INVENTORY"
```

Controlled measurements may create bounded experiment-owned routes, probes, MQTT resources, or load instrumentation. They must remove what they create and never repair the 5G deployment.

## 10. Campaigns and analysis

```zsh
synthran run \
  --campaign-id campaign-001 \
  --network-run-id "$NETWORK_RUN" \
  --seeds 424242,424243,424244 \
  --conditions 'baseline,load50=0.5,load80=0.8,load95=0.95' \
  --campaign-seed 12345 \
  --iot-profile ambient-v1 \
  --inventory "$INVENTORY" \
  --probe-target "$MEASUREMENT_PEER_IP" \
  --reference-capacity-bps REFERENCE_CAPACITY
```

Use `--plan` to persist and inspect the deterministic campaign schedule without execution.

```zsh
synthran analyze \
  --campaign .synthran/campaigns/campaign-001.json \
  --out .synthran/reports/campaign-001-analysis.json
```

Physical Amber support does not imply physical controlled-load campaign acceptance. Do not claim that parity without reviewed implementation and accepted evidence.

## 11. Preserve evidence

Preserve complete raw run or campaign bundles outside normal Git history, including:

- upstream deployment manifest and generated inventory;
- provider identity/network evidence from the upstream manifest;
- SynthRAN network and experiment evidence;
- telemetry and sequence records;
- measurement windows and probes;
- network-counter samples and load records;
- dependency identities and artifact hashes;
- canonical structured run events;
- upstream and experiment forensic logs when relevant.

JSONL is the audit source. Parquet is a deterministic analysis derivative.

## Failure rules

- never reuse a run ID for different intent or topology;
- preserve partial evidence after failure;
- diagnose the smallest failing boundary first;
- do not repair infrastructure from experiment code;
- never infer current authority from historical evidence;
- fail closed when exact experiment cleanup cannot be proven;
- do not convert an infrastructure failure into a scientific result;
- do not convert an unexpected scientific outcome into an infrastructure failure.
