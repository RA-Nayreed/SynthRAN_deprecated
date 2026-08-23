# R2Lab code architecture

Physical R2Lab support is one subsystem under `synthran/r2lab/`. The package
keeps resource control, deployment, observation, modem mutation, and acceptance
separate without duplicating them under `synthran.network`.

## Modules

| Module | Responsibility |
|---|---|
| `controller.py` | Resource selection, Faraday transport, claims, prepare, release, and start authority |
| `provider.py` | Exact PDU/qfit provider state, verified transitions, and cleanup assessment |
| `radio.py` | Reviewed RF profile plus sanitized cell, registration, packet, IP, and traffic evidence |
| `deployment.py` | Physical chart intent, rendering, packaging, stopped staging, and singleton gNB lifecycle |
| `acceptance.py` | Immutable staging/start bindings and ordered physical acceptance |
| `readiness.py` | Read-only FIT host, USB, modem-device, and interface readiness |
| `n2.py` | Sanitized AMF-side N2 evidence |
| `runtime.py` | Read-only gNB/N2, qfit, and user-plane observation |
| `ue.py` | MBIM activation, rollback, user-plane authorization, and workload handoff |
| `guards.py` | Combined mutation preconditions |
| `handoff.py` | Exact namespace ownership transfer |

`__init__.py` declares the package and performs no runtime patching or public
re-export. Callers import the module that owns the operation.

## Dependency direction

The main direction is:

```text
provider + radio
       -> controller + deployment + readiness + n2
       -> runtime + ue + guards + handoff
       -> CLI and physical experiment adapter
```

Acceptance evidence is shared by deployment, runtime, and UE orchestration. It
does not execute provider or modem operations.

## Runtime boundaries

The controller owns the only general Faraday SSH builder and the logical qfit to
physical FIT-host mapping. Runtime and UE operations use that transport instead
of reconstructing nested SSH commands.

Observation and mutation remain distinct:

- `runtime.py` reduces allow-listed command output to categorical evidence and
  never powers hardware, attaches packet service, creates a session, or changes
  Kubernetes state.
- `ue.py` performs the reviewed modem mutations only after fresh authority,
  singleton gNB/N2, and qfit management checks.
- `deployment.py` owns every step from reviewed physical intent through immutable
  stopped staging and exact singleton start.

## Acceptance order

```text
resource authority
  -> SLICES foundation
  -> Kubernetes
  -> Open5GS
  -> gNB/N2
  -> UE management
  -> cell acquisition
  -> registration
  -> PDU session
  -> user plane
  -> workload
```

Stages cannot be skipped. A failure blocks later stages, and physical workload
acceptance cannot be satisfied by the RFSIM backend.

## Safety invariants

- No automatic reservation or broad resource cleanup.
- Every write is scoped to the active run, lease, and selected resources.
- Provider observation, not a command return code, decides physical state.
- Unknown or contradictory state fails closed and retains the local claim.
- Strict SSH host verification is required at every hop.
- Zero matching gNB pods is proven before N300 release or reconfiguration.
- Exactly one ready gNB pod is required after physical start.
- Subscriber identity and raw modem output are excluded from persisted evidence.
- Failed activation requests the exact reviewed rollback and proves its
  postconditions before cleanup is accepted.
- RFSIM remains an independent accepted backend.

New behavior belongs in the existing module that owns its state and safety
contract. A new module is justified only for a distinct cohesive boundary, not
for an individual command or observation.
