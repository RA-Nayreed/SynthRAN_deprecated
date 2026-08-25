# Operator guide

This is the supported procedure for running SynthRAN on SLICES with either the virtual RFSIM backend or the physical R2Lab backend.

## 1. Install and verify

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
```

There is one installed executable. Do not operate the network by calling internal Python helpers or historical backend command names.

## 2. Authenticate provider tools

Provider authentication and project creation remain outside SynthRAN.

```zsh
slices auth login
slices project list
```

Select a SLICES project that already exists and export the identity SynthRAN should use:

```zsh
export SYNTHRAN_SLICES_PROJECT='PROJECT_NAME'
export SYNTHRAN_OWNER='YOUR_SLICES_USERNAME'
```

A run selects that project, creates or reuses the provider experiment associated with its run ID, acquires the Post5G prefix, and verifies the resulting provider network.

You may override the provider experiment with `--slices-experiment`, but the normal path is to let it match the run ID.

## 3. Read-only readiness

### RFSIM

```zsh
synthran doctor \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3
```

The virtual doctor validates the selected node topology, pinned dependencies, and local deployment prerequisites. If an existing provider experiment is supplied with `--slices-experiment`, it also verifies that context.

### R2Lab

An active R2Lab lease is required before physical mutation.

```zsh
export SYNTHRAN_R2LAB_SLICE='YOUR_R2LAB_SLICE'

synthran doctor \
  --radio r2lab \
  --device n300 \
  --ue qfit07 \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --slice "$SYNTHRAN_R2LAB_SLICE"
```

The physical doctor is read-only. It validates the selected executable topology, strict public-key access to Faraday, and the active lease.

To inspect the physical hardware catalogue:

```zsh
synthran inspect --radio r2lab
```

## 4. Run the virtual backend

Use a new immutable run ID:

```zsh
export RUN_ID='virtual-001'

synthran run \
  --radio rfsim \
  --core-node sopnode-f2 \
  --ran-node sopnode-f3 \
  --run-id "$RUN_ID" \
  --owner "$SYNTHRAN_OWNER" \
  --slices-project "$SYNTHRAN_SLICES_PROJECT"
```

The command owns the complete sequence:

```text
provider context
-> SLICES reservation/allocation
-> live authority preflight
-> Open5GS + srsRAN/RFSIM deployment
-> srsUE/PDU path proof
-> deterministic ten-sensor workload
-> acceptance evidence
```

There is no supported need to call resource preparation, network deployment, network verification, or workload execution as separate CLI commands.

## 5. Run the physical backend

The physical path requires a strict known-hosts file for the selected SLICES compute nodes. Use a reviewed existing file or create it through the provider access workflow; do not disable host-key checking.

```zsh
export SYNTHRAN_SLICES_KNOWN_HOSTS='/absolute/path/to/sopnodes_known_hosts'
export RUN_ID='physical-001'

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

The physical command owns:

```text
provider context
-> exact radio/UE claim under the active lease
-> selected compute-node/Open5GS foundation
-> pinned N3xx gNB staging
-> singleton gNB + stable N2
-> selected UE setup/connect through pinned 5g_ansible roles
-> registration + PDU proof
-> route-bound user-plane proof
-> deterministic ten-sensor workload
-> acceptance evidence
-> exact physical cleanup
```

By default accepted physical resources are released at the end. `--keep-resources` is available only when the operator intentionally needs the run-owned hardware to remain active for immediate follow-up work.

If a previous run owns the current Open5GS namespace and automatic ownership handoff cannot be resolved safely, use `--previous-run-id` with the exact prior run ID. Never guess it.

## 6. Watch progress

Every run records a sanitized JSONL event stream:

```text
.synthran/events/<run-id>.jsonl
```

Live output and persisted logs use the same messages. Follow a run from another shell with:

```zsh
synthran logs --run-id "$RUN_ID" --follow
```

or inspect the latest persisted messages later:

```zsh
synthran logs --run-id "$RUN_ID" --tail 200
```

Long Ansible work uses one shared formatter across virtual deployment, physical Open5GS work, and physical UE setup/connect/stop. Routine Ansible chatter is suppressed; meaningful tasks, failures, and heartbeats remain visible.

`--quiet` suppresses terminal progress but does not disable event persistence.

## 7. Inspect evidence

```zsh
synthran inspect --run-id "$RUN_ID"
```

The command discovers the persisted evidence associated with the run and reports the available schemas/statuses. Use `--json` when another tool needs machine-readable output.

Do not use a historical PDU address, pod name, reservation ID, allocation ID, or lease observation as current mutation authority. Persisted evidence proves the historical run; live control always refreshes current state.

## 8. Stop or recover exact physical resources

If a physical run fails before its normal cleanup or was intentionally left active:

```zsh
synthran stop \
  --run-id "$RUN_ID" \
  --slice "$SYNTHRAN_R2LAB_SLICE" \
  --owner "$SYNTHRAN_OWNER" \
  --known-hosts "$SYNTHRAN_SLICES_KNOWN_HOSTS"
```

Cleanup is run-scoped. It may stop the run-owned gNB and release only the radio/UE resources bound to that run. If ownership cannot be proven, cleanup fails rather than broadening its target.

Never substitute wildcard Kubernetes deletion, global radio power-off, guessed allocation IDs, `pkill`, or `killall` for exact cleanup.

For RFSIM, workload cleanup is normally part of run execution and there is no persistent physical claim to release.

## 9. Research measurements

The current controlled measurement implementation is validated on accepted RFSIM network evidence.

After an accepted virtual run, the generated inventory is normally:

```text
.synthran/preparations/<run-id>/hosts.ini
```

and the accepted virtual network evidence uses the same run ID under `.synthran/runs/`.

### Calibrate the external peer

Choose a prepared peer outside the 5G core host. See `research-measurement-peer.md`.

```zsh
export NETWORK_RUN='virtual-001'
export INVENTORY=".synthran/preparations/$NETWORK_RUN/hosts.ini"
export MEASUREMENT_PEER_IP='PEER_IPV4'
export CALIBRATION='.synthran/research/capacity.json'

synthran research calibrate \
  --inventory "$INVENTORY" \
  --network-run-id "$NETWORK_RUN" \
  --target "$MEASUREMENT_PEER_IP" \
  --duration-seconds 10 \
  --out "$CALIBRATION"
```

### Build and execute a campaign

```zsh
export REFERENCE_BPS=$(jq -r '.reference_capacity_bps' "$CALIBRATION")
export CAMPAIGN_ID='campaign-001'
export CAMPAIGN_FILE=".synthran/campaigns/$CAMPAIGN_ID.json"

synthran research campaign-plan \
  --campaign-id "$CAMPAIGN_ID" \
  --network-run-id "$NETWORK_RUN" \
  --seeds 424242,424243,424244 \
  --conditions 'baseline,load50=0.5,load80=0.8,load95=0.95' \
  --campaign-seed 12345 \
  --out "$CAMPAIGN_FILE"

synthran research campaign-run \
  --campaign "$CAMPAIGN_FILE" \
  --inventory "$INVENTORY" \
  --target "$MEASUREMENT_PEER_IP" \
  --reference-capacity-bps "$REFERENCE_BPS" \
  --sensor-period 5 \
  --warmup-seconds 30 \
  --duration-seconds 180 \
  --sample-interval 1 \
  --probe-interval 1 \
  --parallel-flows 2 \
  --load-port 5220
```

Analyze persisted valid runs:

```zsh
mkdir -p .synthran/reports

synthran research analyze \
  --campaign "$CAMPAIGN_FILE" \
  --out ".synthran/reports/$CAMPAIGN_ID-analysis.json"
```

Physical deterministic workload support does not yet imply physical controlled-load campaign acceptance. Do not point the current research campaign commands at a physical run and claim parity without a reviewed physical measurement implementation and accepted evidence.

## 10. Preserve evidence

Keep complete raw run or campaign bundles outside normal Git history. Preserve:

- run and campaign specifications;
- provider/resource provenance;
- measurement windows;
- telemetry and sequence evidence;
- RTT probes;
- network-counter samples and timing evidence;
- load records;
- validity summaries;
- dependency identities;
- artifact hashes;
- unified run event logs.

JSONL is the audit source. Parquet is a deterministic analysis derivative. Checksum manifests must exclude the checksum file itself.

## 11. Provider release

Do not release a Post5G prefix while any active work still depends on it. Provider-level teardown outside a run remains an explicit provider action where required.

## Failure rules

- never reuse a run ID for different intent or topology;
- preserve partial evidence after failure;
- diagnose the smallest failing boundary first;
- refresh current authority before retrying live mutation;
- never infer later acceptance from an earlier successful boundary;
- fail closed when exact rollback/cleanup cannot be proven;
- do not convert an infrastructure failure into a scientific result;
- do not convert a legitimate scientific outcome into an infrastructure failure merely because it was unexpected.
