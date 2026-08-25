# Controller initialization contract

SynthRAN initialization establishes durable local controller identity and workspace configuration without changing provider resources. It is an internal service boundary, not a separate product interface.

## Inputs

A controller profile can contain:

- profile name;
- SLICES username;
- SLICES project association;
- optional R2Lab slice and SSH identity reference;
- stable workspace defaults such as reservation duration and placement policy.

The private SSH key is never copied. SynthRAN stores only a normalized identity reference and public-key fingerprint.

## Verify before persist

Before persistent state is created, initialization validates local names and existing workspace state, fingerprints the selected identity when configured, and can perform read-only provider access checks. It does not create reservations, allocations, leases, provider experiments, deployments, or workloads.

Provider failures remain boundary-specific. Raw credential-bearing provider output must not be copied into durable state.

Only after verification succeeds may initialization persist the new profile, workspace files, and bounded access-cache records. Rollback removes only local objects created by the failed initialization attempt and preserves reused profiles and pre-existing research evidence.

If local state changes between verification and persistence, initialization fails closed rather than overwriting it.

## Existing experiment evidence

An existing `.synthran` directory can contain accepted preparation, network, experiment, and research artifacts. Initialization preserves those paths exactly and does not rewrite them into newer workspace records.

Adoption fails closed when the directory contains ambiguous partial persistent-workspace state without the required workspace authority file.

## Access caches

SLICES and optional R2Lab gateway access records are caches with explicit freshness boundaries. They do not authorize reservation, allocation, lease, radio, network, UE, or workload mutation merely because they exist.

Current provider and physical authority must be reverified at the operation boundary that needs it.

## Experiment setup

Creating a local SynthRAN experiment allocates durable local identity and desired state. It does not create a provider experiment or mutate provider resources. A provider binding, when present, refers to an existing provider object and remains subject to current verification before live work.

## Product boundary

The supported operator surface is the installed `synthran` command. No interactive startup flow or external initialization protocol is part of the product contract. Initialization helpers remain internal until an explicit CLI command is added and tested.
