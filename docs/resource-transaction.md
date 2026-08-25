# Composite resource transaction

SynthRAN can model experiments that combine resources from more than one provider. A physical request can combine SLICES compute with R2Lab radio and UE resources, while a virtual request combines SLICES compute with non-mutating RFSIM placement.

The generic transaction layer coordinates these provider boundaries without weakening provider-specific authority or ownership checks. Its existence does not imply that every modeled provider combination has a complete live adapter.

## Preconditions

A transaction starts only after desired state is persisted, fresh complete inventory produces a `ResourceDecision`, that exact decision is bound to an operation, authorization permits the operation, and every real provider in the decision has a concrete adapter.

## Provider order

Provider ordering is deterministic. Ordering is not authority: every adapter must still perform its own current live checks immediately before mutation.

Virtual RFSIM placement records selection scope but performs no provider acquisition.

## Receipts

An acquisition receipt distinguishes exact requested resources from exact resources created by the current operation. A pre-existing safe resource is not added to generic rollback scope merely because the transaction requested it.

A receipt that claims a resource outside its requested set is invalid.

## Rollback

Explicit provider failure rolls back only exact declared creations, in reverse provider order. Complete rollback permits clean failure and claim release. Incomplete rollback requires recovery and retains mutation authority.

An adapter exception is treated as unknown partial failure because the generic layer cannot know whether the provider changed state before the exception. Known earlier creations can be rolled back exactly; the uncertain provider is not guessed at or broadly cleaned.

## Application integration

`ApplicationController.execute_resource_operation()` coordinates the generic path when a current operation, inventory, decision, and concrete provider adapters are available. Unexpected exceptions after authorization are interruption or recovery conditions rather than proof that providers were unchanged.

## Adapter contract

A provider adapter acquires exact requested IDs and releases only exact resource IDs it proved were created by the current operation. The generic layer does not itself implement POS ownership, SLICES reservation/allocation, R2Lab lease handling, radio power, node imaging, or network deployment.

Concrete adapters wrap reviewed provider-specific safety logic and recheck authority at the mutation boundary.

## Product boundary

This transaction engine is an internal orchestration primitive. Current live provider-specific executors remain authoritative for the paths they implement until concrete generic adapters are deliberately connected and accepted. The installed `synthran` command remains the only operator interface.
