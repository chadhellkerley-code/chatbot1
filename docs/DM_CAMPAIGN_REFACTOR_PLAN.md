# DM Campaign Refactor Plan

## Goal

Refactor the campaign outbound and inbox runtime in phases without breaking the
current working flow. Each phase must be additive first, with compatibility
adapters removed only after tests and runtime validation are green.

## Rules

- No public behavior changes without tests.
- No string-based state sharing between layers when a typed contract exists.
- No direct UI/runtime retry policy inside low-level transport code.
- Every phase must end with targeted verification and a rollback boundary.

## Current Status

Phase 4 is completed.

Next step:

- Phase 5: session lifecycle unification

Already in place:

- `CampaignSendResult` and `CampaignSendStatus` in
  `src/dm_campaign/contracts.py`
- `WorkerExecutionState` in `src/dm_campaign/contracts.py`
- `WorkerExecutionStage` in `src/dm_campaign/contracts.py`
- Proxy worker supervision reads typed worker state and stage snapshots
- Inbox reader supervision uses explicit task leases, timeout detection and worker replacement
- Legacy `_parse_send_result` now delegates to the typed contract
- Targeted tests cover sender-result compatibility, worker state machine, scheduler supervision and inbox reader recovery

## Phase 1: Contracts And Worker State

Scope:

- Introduce typed send-result contracts
- Introduce explicit worker execution states
- Keep all existing public runner and sender interfaces stable

Deliverables:

- `CampaignSendResult`
- `CampaignSendStatus`
- `WorkerExecutionState`
- Compatibility wrapper for legacy tuple results
- Tests for ambiguous, skipped, sent and failed outcomes

Exit criteria:

- Existing campaign tests green
- No behavior regression in current campaign runner
- Worker idle diagnostics read state from typed execution state

## Phase 2: Sender Decomposition

Scope:

- Split `HumanInstagramSender` into smaller runtime services

Target modules:

- `session_manager.py`
- `inbox_navigator.py`
- `thread_resolver.py`
- `message_composer.py`
- `delivery_verifier.py`

Deliverables:

- Facade compatible with current sender API
- Unit tests per component
- Shared normalized result contracts instead of ad-hoc tuples

Exit criteria:

- Existing caller code unchanged
- Sender internals no longer mix navigation, compose, verify and policy logic

## Phase 3: Campaign Worker State Machine

Scope:

- Replace implicit timing heuristics with an explicit worker state machine

States:

- `idle`
- `waiting_account`
- `opening_session`
- `resolving_thread`
- `sending`
- `cooldown`
- `stopping`

Deliverables:

- Worker state transitions isolated in one module
- Scheduler reads formal state instead of inferring from timestamps only
- Structured events for transitions

Exit criteria:

- No false idle detection while a lead is being processed
- Retry and cooldown policy driven by state + contract, not raw strings

## Phase 4: Inbox Reader Supervision

Scope:

- Add watchdog, heartbeat and timeout ownership to inbox workers

Deliverables:

- `reader_supervisor.py`
- Per-task timeout and cancellation boundary
- Worker restart policy for stuck readers
- Health events for frozen reader detection

Exit criteria:

- A blocked inbox task cannot freeze the whole reader pool
- Reader restart path is tested

## Phase 5: Session Lifecycle Unification

Scope:

- Centralize browser/session ownership per account

Deliverables:

- One lifecycle owner per account session
- TTL or recycle policy for degraded contexts
- Shared lock policy across campaign and inbox runtimes

Exit criteria:

- No duplicate ownership of the same account context
- Session recovery path is deterministic and logged

## Phase 6: Observability And Cleanup

Scope:

- Remove compatibility shims that were needed during migration
- Standardize structured logs and encoding

Deliverables:

- UTF-8 logging cleanup
- Structured event ids per attempt
- Removal of dead compatibility branches

Exit criteria:

- Runtime incidents can be traced by attempt id
- Legacy tuple-only internal paths removed where no longer needed

## Suggested Execution Order

1. Finish Phase 1 and keep it green.
2. Refactor sender internals behind a compatibility facade.
3. Move worker orchestration to a formal state machine.
4. Fix inbox freeze with supervised workers.
5. Unify session lifecycle last, once both runtimes share typed contracts.
