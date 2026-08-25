# Security and privacy

SynthRAN performs live provider and radio mutations, so safety depends on exact authority, strict transport identity, bounded cleanup, and disciplined evidence handling.

## Authority model

Current live state authorizes current mutation. The trust order is:

```text
current provider state
> current direct runtime observation
> persisted evidence
> manifests
> cached information
```

Historical evidence is useful for audit and resume decisions, but it never substitutes for a current lease, allocation, resource state, pod, registration, route, or PDU observation.

Unknown, stale, foreign, expired, malformed, or ambiguous ownership fails closed.

## Exact resource ownership

A run may mutate or clean up only resources it can prove are selected and owned by that run.

Prohibited shortcuts include:

- wildcard Kubernetes deletion;
- global radio/UE power-off;
- guessed reservation/allocation identifiers;
- broad `pkill`/`killall` cleanup;
- adopting a provider resource based only on a convenient name;
- disabling authority checks because a previous run succeeded.

Physical cleanup releases a claim only after exact off/clean state is proven.

## SSH and host identity

Live control uses public-key SSH with strict host-key verification. R2Lab/SLICES physical operations require an explicit known-hosts file. Do not use `StrictHostKeyChecking=no` as a recovery shortcut.

Private SSH keys and known-host authority files are local runtime material and must not be committed.

## Provider credentials

Never commit:

- SLICES/provider tokens;
- object-store credentials;
- private SSH keys;
- kubeconfigs containing credentials;
- private authority/environment files;
- subscriber authentication material;
- private modem/subscriber identifiers when they are not required in public evidence.

Authentication is performed through the reviewed provider tools. SynthRAN does not store provider passwords.

## Subscriber and modem data

Physical UE work may expose subscriber identifiers or modem output. Public evidence should retain only the minimum sanitized state required to prove the boundary: selected UE, mode, interface state, registration/PDU outcome, route/probe result, dependency provenance, and hashes where appropriate.

Raw secret-bearing modem output is not a public evidence format.

## Ansible output

All long Ansible operations use the shared sanitized streamer. Routine task chatter is suppressed and only reviewed task labels/failures/heartbeats are emitted to the public run stream.

The complete upstream command result may be used internally to determine success or to write an already-sanitized run log, but it must not bypass privacy filtering simply because it came from Ansible.

## Unified event stream

Every run writes:

```text
.synthran/events/<run-id>.jsonl
```

The event stream is intended to be safe enough for normal diagnostics, but it still belongs to generated run state rather than Git. New progress messages must avoid local credentials, secret values, private keys, and unnecessary raw provider payloads.

## Research evidence

Prefer the least sensitive evidence that proves the required scientific boundary:

- route/interface proof;
- byte/drop counters;
- broker receipt;
- sequence continuity;
- bounded RTT/load records;
- artifact hashes.

Packet capture is not the default evidence mechanism when counters or application-level proof are sufficient. If a capture is required, treat it as private raw evidence unless it has been explicitly reviewed and sanitized.

## Repository privacy controls

Run the worktree scanner with:

```zsh
synthran dev privacy scan --worktree
```

The repository also includes pre-push/CI privacy protection. Activate tracked hooks with:

```zsh
synthran dev hooks install
```

The scanner checks for classes including private keys, provider tokens, kubeconfig secret material, subscriber secrets/identifiers, local home paths, private network context, and credential-like assignments.

False positives should be corrected narrowly. Do not weaken a whole rule just to allow one generated file into Git.

## Generated state

The following belong in ignored/private runtime storage rather than source control:

```text
.deps/
.synthran/runs/
.synthran/preparations/
.synthran/experiments/
.synthran/experiments-r2lab/
.synthran/r2lab/
.synthran/events/
private authority files
private captures/logs
```

Public documentation should use placeholders or already-sanitized values when examples require addresses or identities.

## Dependency integrity

Upstream repositories and runtime images are pinned through `dependencies.lock.yml`. Live code should verify the expected commit/image/profile before applying run-local overlays or mutations.

An unexpected upstream tree is an integrity failure; do not silently adapt a live deployment to whatever happens to be checked out.

## Preservation

Raw campaign/run bundles belong in durable research/object storage with explicit hashes and access controls. Public Git should contain only reviewed summaries, schemas, code, tests, and intentionally tracked analysis derivatives.

Checksum manifests must exclude themselves.

## Incident handling

If a live command fails and exact rollback cannot be proven:

1. preserve the existing evidence;
2. avoid broad cleanup;
3. inspect current provider/runtime state;
4. use `synthran inspect` and `synthran logs` for run evidence;
5. use `synthran stop` only when exact run authority can be supplied/proven;
6. escalate to provider-native read-only inspection when authority remains ambiguous.

Do not erase evidence simply to make a later retry appear clean.
