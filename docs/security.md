# Security and privacy

SynthRAN is an experiment-orchestration and evidence layer. Infrastructure mutation belongs to the pinned 5g-Ansible machine API; SynthRAN supplies declarative intent, observes the resulting deployment, creates only bounded experiment-owned state, and records scientific evidence.

## Authority model

Current live state authorizes current mutation. The trust order is:

```text
current provider / upstream deployment state
> current direct runtime observation
> persisted evidence
> manifests
> cached information
```

Historical evidence is useful for audit and resume decisions, but it never substitutes for current provider authority, upstream deployment state, UE/PDU identity, route state, or experiment-owned process state.

Unknown, stale, foreign, expired, malformed, or ambiguous ownership fails closed.

## Infrastructure ownership

5g-Ansible owns SLICES provider context, reservation/POS work, Kubernetes preparation, core/RAN/RU/UE deployment, and infrastructure teardown. SynthRAN must not reconstruct those operations from logs or historical evidence.

SynthRAN may mutate only bounded experiment-owned resources such as run-labelled MQTT resources, transient forwards/relays, exact measurement routes, and owned measurement processes. Cleanup removes only state proven to have been created by that run.

Prohibited shortcuts include:

- wildcard Kubernetes deletion;
- global radio/UE power-off from SynthRAN;
- guessed reservation/allocation identifiers;
- broad `pkill`/`killall` cleanup;
- adopting provider resources from names alone;
- patching an upstream UE/RAN Deployment to make an experiment pass;
- disabling authority checks because a previous run succeeded.

Infrastructure cleanup is delegated to the exact upstream deployment ID through `fiveg down` / `synthran release`.

## SSH and host identity

SynthRAN-owned SSH observation uses public-key SSH with `StrictHostKeyChecking=yes`. A custom `--known-hosts` file is an optional reviewed trust-store override; when omitted, normal OpenSSH user/system known-hosts files are used.

Do not use `StrictHostKeyChecking=no` as a recovery shortcut in SynthRAN experiment code. Private SSH keys and custom known-host authority files are local runtime material and must not be committed.

The security policy of SSH performed internally by 5g-Ansible belongs to that pinned upstream dependency and must be reviewed there rather than duplicated or silently overridden in SynthRAN.

## Provider credentials

Never commit:

- SLICES/provider tokens;
- object-store credentials;
- private SSH keys;
- kubeconfigs containing credentials;
- private authority/environment files;
- subscriber authentication material;
- private modem/subscriber identifiers when they are not required in public evidence.

Authentication is performed through the reviewed provider tooling used by 5g-Ansible. SynthRAN does not store provider passwords or implement the SLICES provider lifecycle.

## Subscriber and modem data

Physical UE work may expose subscriber identifiers or modem output. Public evidence should retain only the minimum state needed to prove the experiment boundary: selected UE, mode, interface state, registration/PDU outcome as supplied/observed, route/probe result, dependency provenance, and hashes where appropriate.

Raw secret-bearing modem output is not a public evidence format.

## Deployment progress and Ansible logs

SynthRAN does not parse, sanitize, or prettify Ansible `PLAY`, `TASK`, handler, host-change, or module-result text.

When requested by the thin adapter, the pinned 5g-Ansible machine interface emits semantic JSONL progress records using:

```text
fiveg/event/v1
```

The machine contract keeps stdout for the final versioned result and uses the structured event channel only for progress. Provider-assigned network values such as subnet, load-balancer address, and expiration remain in upstream state/manifest rather than progress chatter.

Detailed deployment output remains in upstream run-owned evidence logs such as `collections.log`, `r2lab.log`, `deploy.log`, `down.log`, and scenario logs. Those logs are not automatically safe for publication merely because they were produced by Ansible.

## Unified experiment event stream

Every SynthRAN run writes:

```text
.synthran/events/<run-id>.jsonl
```

SynthRAN persists its own workload/measurement/acceptance events and relays recognized `fiveg/event/v1` records without inferring deployment truth from text. Unknown upstream event fields or future event kinds are ignored by the renderer unless explicitly supported.

The event stream belongs to generated run state rather than Git. New progress records must avoid credentials, secret values, private keys, and unnecessary raw provider payloads.

## Research evidence

Prefer the least sensitive evidence that proves the required scientific boundary:

- route/interface proof;
- byte/drop counters;
- broker receipt;
- sequence continuity;
- bounded RTT/load records;
- artifact hashes.

Packet capture is not the default evidence mechanism when counters or application-level proof are sufficient. If a capture is required, treat it as private raw evidence unless explicitly reviewed and sanitized.

## Repository privacy controls

Run the worktree scanner with:

```zsh
synthran dev privacy scan --worktree
```

Activate tracked hooks with:

```zsh
synthran dev hooks install
```

The scanner checks for private keys, provider tokens, kubeconfig secret material, subscriber secrets/identifiers, local home paths, private network context, and credential-like assignments.

False positives should be corrected narrowly. Do not weaken a whole rule merely to admit generated material into Git.

## Generated state

The following belong in ignored/private runtime storage rather than source control:

```text
.deps/
.synthran/runs/
.synthran/experiments/
.synthran/events/
private authority files
private captures/logs
```

Public documentation should use placeholders or reviewed values when examples require addresses or identities.

## Dependency integrity

Direct upstream repositories and runtime identities are pinned through `dependencies.lock.yml`. Before execution, SynthRAN verifies that the 5g-Ansible checkout is the exact locked detached commit and has no local modifications.

SynthRAN must not patch or overlay the upstream checkout to compensate for a deployment problem. A required 5g-Ansible change is made and validated upstream first, then pinned as a new immutable dependency identity.

## Preservation

Raw campaign/run bundles belong in durable research/object storage with explicit hashes and access controls. Public Git should contain only reviewed summaries, schemas, code, tests, and intentionally tracked analysis derivatives.

Checksum manifests must exclude themselves.

## Incident handling

If a live command fails:

1. preserve the existing upstream and SynthRAN evidence;
2. avoid broad cleanup or manual infrastructure repair from SynthRAN;
3. inspect current provider/upstream state and the recorded `failure.phase` where available;
4. use `synthran inspect --run-id <run-id>` for SynthRAN evidence/status;
5. use upstream run-owned logs for detailed deployment diagnosis;
6. use `synthran release --run-id <run-id>` only when the exact upstream deployment identity exists and release is appropriate;
7. use provider-native read-only inspection when authority remains ambiguous.

Do not erase failure evidence merely to make a later attempt appear clean.
