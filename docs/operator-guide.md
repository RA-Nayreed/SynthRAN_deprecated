# Operator guide

This is the supported path from a SLICES user context to a reproducible SynthRAN experiment and research campaign. Current evidence is in [`results.md`](results.md); experiment validity rules are in [`experiment.md`](experiment.md); durable boundaries are in [`architecture.md`](architecture.md).

## Execution boundary

The supported product interface is the installed command:

```text
synthran <arguments>
```

There is no interactive frontend or external workbench service. Provider mutations are performed only by explicit CLI commands and remain subject to authority, ownership, freshness, evidence, and cleanup rules.

## 1. Prepare the controller

Create and activate the reviewed environment, then install the repository command:

```bash
cd ~/SynthRAN
conda env create -f environment.yml
conda activate synthran
python -m pip install --no-deps -e .
python -c "import os; assert os.environ.get('CONDA_DEFAULT_ENV') == 'synthran'"
python -m unittest discover -s tests -v
synthran privacy scan --worktree
```

Synchronize pinned dependencies and verify the command:

```bash
synthran deps sync
synthran --help
```

## 2. Establish the SLICES provider context

A SLICES project is required. SynthRAN does not create or approve projects and does not silently change the active project.

```bash
slices auth login
slices project list
slices project use PROJECT_NAME
slices auth show
slices project show
```

Create the provider experiment explicitly and acquire its Post5G network prefix:

```bash
export PROJECT_NAME=PROJECT_NAME
export PROVIDER_EXPERIMENT=EXPERIMENT_NAME

slices project use "$PROJECT_NAME"
slices experiment create "$PROVIDER_EXPERIMENT" --duration 4h
post5g experiment prefix "$PROVIDER_EXPERIMENT"

export SYNTHRAN_SLICES_PROJECT="$PROJECT_NAME"
export SYNTHRAN_SLICES_EXPERIMENT="$PROVIDER_EXPERIMENT"
```

Keep the prefix active while live work depends on it.

## 3. Verify provider context

```bash
synthran slices doctor
```

This is read-only. Resolve controller, project, provider-experiment, prefix, dependency, or provider-tool failures before starting mutations.

## 4. Reserve and prepare virtual resources

For the accepted RFSIM topology, use a unique preparation ID and exact owner identity:

```bash
export SYNTHRAN_OWNER=YOUR_SLICES_USERNAME
export PREPARATION_RUN=prepare-001

synthran network prepare \
  --dry-run \
  --owner "$SYNTHRAN_OWNER" \
  --duration-minutes 120 \
  --run-id "$PREPARATION_RUN"

synthran network prepare \
  --owner "$SYNTHRAN_OWNER" \
  --duration-minutes 120 \
  --run-id "$PREPARATION_RUN"

source ".synthran/preparations/$PREPARATION_RUN/authority.env"
export INVENTORY=".synthran/preparations/$PREPARATION_RUN/hosts.ini"
```

`authority.env` contains live provider identifiers and must remain private and untracked.

## 5. Preflight, deploy, and prove the virtual 5G path

```bash
synthran doctor \
  --inventory "$INVENTORY" \
  --evidence-out .synthran/preflight.json

export NETWORK_RUN=network-001

synthran network deploy \
  --inventory "$INVENTORY" \
  --preflight-evidence .synthran/preflight.json \
  --run-id "$NETWORK_RUN"

synthran network verify \
  --inventory "$INVENTORY" \
  --run-id "$NETWORK_RUN" \
  --timeout 120
```

Successful deployment is weaker than path proof. Do not start a research campaign until verification accepts the current path. Never reuse a historical PDU address as current authority.

## 6. Run deterministic IoT acceptance

```bash
export IOT_RUN=iot-001

synthran experiment plan \
  --network-run-id "$NETWORK_RUN" \
  --run-id "$IOT_RUN"

synthran experiment run \
  --inventory "$INVENTORY" \
  --network-run-id "$NETWORK_RUN" \
  --run-id "$IOT_RUN"

synthran experiment verify --run-id "$IOT_RUN"
```

An experiment failure does not automatically justify base-network redeployment. Preserve its evidence, recover only exact SynthRAN-owned resources, and reverify the base path.

## 7. Choose the external research peer

Capacity calibration and controlled load must terminate outside the 5G core host. In the reviewed two-node virtual topology the prepared RAN node is the external peer.

```bash
ansible -i "$INVENTORY" ran_node -m shell -a 'ip -4 -o addr show; ip -4 route show default'
export MEASUREMENT_PEER_IP=PEER_IPV4
```

Do not substitute the core-node address or a same-host target that can collapse into a Kubernetes or hairpin path.

## 8. Calibrate the user plane

```bash
export CALIBRATION=.synthran/research/capacity.json

synthran experiment research calibrate \
  --inventory "$INVENTORY" \
  --network-run-id "$NETWORK_RUN" \
  --target "$MEASUREMENT_PEER_IP" \
  --duration-seconds 10 \
  --out "$CALIBRATION"

export REFERENCE_BPS=$(jq -r '.reference_capacity_bps' "$CALIBRATION")
```

Calibration belongs to the current network epoch; it is not a universal capacity claim.

## 9. Plan and run a controlled campaign

```bash
export CAMPAIGN_ID=campaign-001
export CAMPAIGN_FILE=".synthran/campaigns/$CAMPAIGN_ID.json"
export RUN_ROOT=.synthran/experiments

synthran experiment research campaign-plan \
  --campaign-id "$CAMPAIGN_ID" \
  --network-run-id "$NETWORK_RUN" \
  --seeds 424242,424243,424244 \
  --conditions baseline,load50:0.5,load80:0.8,load95:0.95 \
  --campaign-seed 12345 \
  --out "$CAMPAIGN_FILE"

synthran experiment research campaign-run \
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
  --load-port 5220 \
  --run-root "$RUN_ROOT"
```

A requested sampling interval is not proof of achieved cadence. Persisted timing evidence and current path validity determine whether a run is usable.

## 10. Analyze persisted valid runs

```bash
mkdir -p .synthran/reports

synthran experiment research analyze \
  --campaign "$CAMPAIGN_FILE" \
  --run-root "$RUN_ROOT" \
  --out ".synthran/reports/$CAMPAIGN_ID-analysis.json"
```

The analyzer uses persisted validity gates and pairs loaded treatments with the matching seed-block baseline. Failed or invalid runs remain diagnostic evidence and are not silently reclassified.

## 11. R2Lab physical operation

Physical operation uses the same installed `synthran` command but has additional authority and hardware boundaries. Use the focused procedure in [`r2lab-integration.md`](r2lab-integration.md).

Physical acceptance is progressive. Resource preparation or gNB/N2 success does not imply UE registration, PDU, user plane, workload, or experiment acceptance. Every physical mutation must bind current R2Lab authority and exact selected resources.

## 12. Preserve evidence

Preserve raw experiment or campaign bundles outside ordinary Git history. Include run specifications, measurement windows, telemetry, probes, network samples, load records, validity summaries, dependency provenance, and artifact digests. When building `SHA256SUMS`, exclude the checksum file itself.

Repository-tracked results must remain sanitized. Private credentials, authority files, kubeconfigs, dependency worktrees, generated live run directories, and unsanitized packet captures or logs do not belong in Git.

## 13. Finish provider use

Release the Post5G prefix only when no active experiment depends on it:

```bash
post5g experiment prefix "$PROVIDER_EXPERIMENT" --release
```

## Failure and recovery rules

- Never reuse preparation, deployment, experiment, campaign-run, or operation IDs.
- Never infer ownership from a resource name alone.
- Never use broad wildcard or process cleanup when an exact run-owned target is available.
- A measurement failure does not by itself justify redeploying a path-proven base network.
- Preserve partial evidence and diagnose the smallest failing boundary first.
- If clean rollback cannot be proven, fail closed and retain recovery-required state.
- Do not release provider network identity while active work still depends on it.
