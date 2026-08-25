# Backend Contract

SynthRAN exposes one experiment lifecycle through the `synthran` executable. RFSIM and R2Lab are backend implementations of that lifecycle. RFSIM remains the accepted virtual reference path; R2Lab provides the corresponding physical-radio path. Backend choice may change how a stage is realized, but it must not change the meaning of experiment acceptance, research data, or cleanup.

A backend is conformant only for stages it can prove from current observations. Historical evidence does not authorize a current operation, and an implementation must not report a later stage when an earlier required stage is unproven.

## Lifecycle contract

| Stage | Required proof | RFSIM realization | R2Lab realization |
| --- | --- | --- | --- |
| Access | Current provider identity and authority are valid for the requested run. | Current SLICES project and experiment context. | Current SLICES context plus current R2Lab lease and physical-resource authority. |
| Resources | Exact run-owned resources required by the selected backend are available. | Prepared compute resources for core and RAN/UE execution. | Prepared compute resources plus the selected radio and UE resources. |
| Kubernetes | The cluster is reachable and the expected nodes are ready. | Prepared SLICES cluster. | Prepared SLICES cluster. |
| Core | Open5GS is ready and bound to the current run context. | Open5GS on the selected core node. | Open5GS on the selected core node. |
| gNB | The current gNB instance is ready and bound to the intended backend. | srsRAN gNB using RFSIM. | Physical gNB using the selected R2Lab radio. |
| N2 | The current gNB has a stable AMF association. | Current N2 observation from the virtual gNB/core path. | Current N2 observation from the physical gNB/core path. |
| UE management | The selected UE can be observed and controlled through the backend's management boundary. | srsUE process and pod state. | Selected qfit/qhat management path and modem state. |
| Cell | The UE observes the intended NR cell through the selected radio path. | RFSIM cell visibility. | Physical NR cell acquisition. |
| Registration | The UE is currently registered on the intended 5G network. | Current srsUE registration state. | Current modem registration state. |
| PDU | A current PDU session exists and has a usable address. | `tun_srsue1` with the accepted PDU address. | `wwan0` or the selected physical UE data interface with the accepted PDU address. |
| User plane | Traffic is proven to traverse the accepted PDU path. | Route, reachability, and counters through `tun_srsue1`. | Route, reachability, and counters through the physical UE data interface. |
| Workload | The canonical IoT workload traverses the accepted 5G path. | Deterministic Cooja workload through the RFSIM user plane. | The same deterministic Cooja workload through the physical user plane. |
| Data | Canonical telemetry and derived research artifacts satisfy the same schemas and validity rules. | JSONL audit data and deterministic Parquet derivative. | JSONL audit data and deterministic Parquet derivative. |
| Acceptance | Persisted evidence binds the run, backend, resources, network path, workload, and validity checks. | Virtual-path evidence. | Physical-path evidence with the same experiment-level meaning. |
| Cleanup | Only exact run-owned state is removed or restored, and the backend can prove the resulting safe state. | Run-owned process, Kubernetes, tunnel, and experiment cleanup. | Run-owned process and Kubernetes cleanup plus exact physical-resource restoration or release. |

## Common semantics

The backend boundary ends below experiment semantics. Code above that boundary must not infer lifecycle behavior from backend-specific names such as `tun_srsue1`, `wwan0`, a qfit identifier, a pod name, or an R2Lab radio identifier. A backend supplies current accepted network and user-plane context; the experiment layer consumes that context.

The canonical IoT experiment keeps the same scientific inputs across backends where the hardware permits them: sensor count, Contiki-NG/Cooja source, seed, sensor period, MQTT topic semantics, collection window, minimum observation rules, integrity checks, and research validity gates. Physical RF measurements may differ from RFSIM measurements; data meaning and validation rules do not.

Backend-specific diagnostics may expose additional observations needed to establish or repair a stage. They are not a second lifecycle and must not create an alternative definition of experiment success.

## Evidence rules

Every accepted stage must be tied to the current run and to the resources that produced it. Evidence must distinguish observation from mutation and must include enough identity to reject stale or foreign state.

Current provider or direct observation outranks persisted evidence, manifests, and caches for mutation authority. Persisted evidence proves that an earlier event occurred; it does not make a lease, allocation, gNB, UE registration, PDU session, or user-plane route current.

A later stage may depend on evidence from an earlier stage only when that evidence is still current under the stage's freshness and identity rules. Physical mutations additionally require current physical authority immediately before the mutation boundary.

### Hash policy

Cryptographic digests are used where byte identity is the actual property being protected: pinned external revisions, downloaded tools, container images, rendered deployment artifacts, immutable data products, and persisted provenance. They are not a substitute for current ownership or live state.

A runtime stage must not repeatedly hash or propagate an unchanged artifact merely to authorize the next operation when exact run/resource identity and a fresh provider or direct observation already prove that boundary. Hash checks belong at artifact creation, transfer, loading, or provenance verification boundaries; live lease, allocation, radio, gNB, UE, PDU, route, and cleanup authority is established from current state.

## Ownership and cleanup

Both backends use exact ownership. SynthRAN must not infer ownership from a broad name pattern when a run identifier, provider identifier, allocation identifier, process identifier, namespace label, or other exact binding is available.

Cleanup is idempotent with respect to already-absent run-owned state. It must fail closed when ownership is unknown or when removing a target could affect another run. Physical cleanup must not use global radio or UE power-off operations when the selected resource can be addressed exactly.

If an operation fails after mutation and safe rollback cannot be proven, the result is a recovery condition rather than a successful cleanup.

## Interface invariants

The supported operator executable is `synthran`. Public lifecycle commands select or resolve a backend and then invoke the same lifecycle semantics. Backend implementation modules are internal integration boundaries, not independent products.

RFSIM is retained as the virtual reference backend. Physical support is conformant only when the same required stages are proven through R2Lab hardware. Static implementation capability does not itself claim that a physical run is currently accepted; live acceptance still requires current evidence from the selected hardware and provider resources.

## Conformance tests

Backend-neutral tests should assert the contract at the semantic boundary rather than compare implementation details. At minimum, conformance covers:

- stage ordering and fail-closed behavior;
- current authority and ownership checks;
- gNB and N2 identity binding;
- UE, registration, PDU, and user-plane acceptance;
- workload handoff through an accepted user plane;
- common telemetry and evidence schemas;
- immutable run identity and provenance;
- exact, idempotent cleanup.

Hardware integration tests may require an active R2Lab lease and are separate from offline unit tests. Passing offline tests alone is not evidence that a physical stage is live-accepted.
