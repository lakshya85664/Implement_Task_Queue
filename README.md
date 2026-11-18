# Implement Task Queue in Code

## Project Overview
This project implements an in-memory order-processing task queue. A producer validates an order and publishes a task. A worker claims the task, processes it through an injectable adapter, validates the result, and acknowledges it only after successful processing. The project also demonstrates bounded retries, dead-letter handling, duplicate-run protection, idempotent replay, observability, and evidence validation.

## Objectives
- Define typed order, payload, task, workflow run, event, result, and trace models.
- Enforce legal workflow transitions and distinguish workflow state from task status.
- Implement producer, FIFO queue, worker, task claiming, processing, and safe acknowledgement.
- Handle transient errors, timeouts, permanent errors, and malformed results.
- Prevent duplicate run triggers and repeated protected side effects.
- Add an injectable adapter, metrics, status lookup, and recursive secret redaction.
- Generate an operator runbook and validate required evidence files.

## Workflow and Task States
| Workflow state | Meaning | Terminal |
|---|---|---|
| created | Workflow initialized | No |
| running | Queue processing active | No |
| waiting | Workflow paused | No |
| completed | Processing completed | Yes |
| needs_review | Operator intervention required | Yes |

| Task status | Meaning |
|---|---|
| queued | Waiting for a worker |
| running | Claimed by a worker |
| acknowledged | Successfully processed and acknowledged |
| failed | Processing failed |
| dead_letter | Retry limit exhausted or invalid task |

## Project Structure
```text
Implement_Task_Queue/
├── task_queue.py
├── task_queue_engine.py
├── task_queue_reliability.py
├── task_queue_observability.py
├── task_queue_runbook.py
├── tests/
├── docs/
├── outputs/
├── traces/
└── reports/
```

## Setup
```cmd
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Run the Demonstrations
```cmd
python task_queue.py
python task_queue_engine.py
python task_queue_reliability.py
python task_queue_observability.py
python task_queue_runbook.py
python -m pytest -v
```

## Implementation Details

### Task 1 — Workflow contract and state model
Pydantic models validate order inputs and represent task payloads, queue tasks, workflow runs, events, results, and traces. A transition table enforces legal workflow state changes. Task status is represented separately from workflow state.

### Task 2 — Producer, queue, worker, and acknowledgement
The producer validates an order before publishing a task to an in-memory FIFO queue. The worker claims the next queued task, increments its attempt count, processes the order, and acknowledges it only after successful processing. Queue snapshots and task-status lookup expose the task lifecycle.

### Task 3 — Failure, retry, and replay safety
Transient errors and timeouts are retried within a configured attempt limit. Permanent errors and malformed or mismatched results are moved to dead-letter handling. A run registry blocks duplicate workflow triggers. An idempotency ledger caches protected processing results so replay does not repeat the side effect. Recovery events include explicit next actions.

### Task 4 — Integration and observability
An injectable adapter contract isolates order processing from the queue implementation. The fake adapter provides deterministic local behavior. Structured traces record task identity, attempt, outcome, latency, and next action. Measurements include poll counts, success and error counts, queue counts, latency, success rate, and oldest queue age. Status lookup returns current task visibility. Recursive redaction protects API keys and authorization-related values.

### Task 5 — Operator runbook and evidence
The operator runbook documents queue lifecycle, acknowledgement safety, failure recovery, integration, observability, operator checks, and known limitations. The evidence validator checks five required JSON files and generates a machine-readable report.

## Evidence Artifacts
- `outputs/task2_task_queue.json`
- `outputs/task3_queue_recovery.json`
- `outputs/task4_queue_measurements.json`
- `outputs/task5_operator_report.json`
- `traces/task3_queue_recovery_trace.json`
- `traces/task4_queue_observability.json`
- `reports/OPERATOR_RUNBOOK.md`
- `reports/evidence_report.json`

Evidence validation: 5 expected files, 5 found files, 5 valid JSON files, and `evidence_complete: true`.

## Verification Results
| Task | Tests passed |
|---|---:|
| Task 1 — Queue contract | 7 |
| Task 2 — Queue implementation | 8 |
| Task 3 — Failure and replay safety | 8 |
| Task 4 — Integration and observability | 8 |
| Task 5 — Operator runbook | 9 |
| **Total** | **40** |

Final verification: `python -m pytest -q` — **40 passed in 0.40s**.

## Known Limitations
- The queue is in-memory and does not survive process restarts.
- The implementation demonstrates a local worker, not distributed worker coordination.
- There is no distributed locking or cross-process visibility timeout.
- Retry scheduling is immediate in the local demonstration.
- The order-processing adapter is a fake implementation.
- Production use requires durable queue storage, worker leases, persistent idempotency records, secure credentials, and monitoring.

## Reflection
This project helped me understand how a task queue separates task creation from task execution. The producer validates an order and publishes a task, while the worker claims and processes it independently. Modeling workflow state separately from task status made the lifecycle clearer, especially the distinction between a running workflow and an individual task that is queued, running, or acknowledged.

The acknowledgement rules were an important learning point. A task should not be acknowledged merely because a worker has received it. Processing must succeed and the returned result must be validated first. This protects the queue from silently losing work when processing fails. The reliability task added another important layer: transient errors and timeouts can be retried within a limit, while permanent errors and malformed results need dead-letter handling and operator attention.

I also learned how duplicate triggers and replay can cause repeated side effects. The run registry and idempotency ledger demonstrate ways to reject duplicate runs and reuse a cached result for a repeated protected action. The observability task showed how structured traces, queue measurements, status lookup, and secret redaction support debugging and operations.

The main production limitation is that this project uses an in-memory queue and local fake adapter. A production system would need durable storage, worker leases or visibility timeouts, persistent idempotency records, and real integration security. Overall, this assessment strengthened my understanding of queue-based processing, acknowledgement safety, failure recovery, replay protection, and operational evidence.
