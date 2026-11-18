# Task Queue — Operator Runbook

## 1. Purpose

This project implements an in-memory order-processing task queue.
A producer validates and publishes an order task. A worker claims
the task, processes it through an adapter, and acknowledges it only
after a valid successful result.

## 2. Workflow states

| Workflow state | Meaning | Terminal |
|---|---|---|
| created | Workflow initialized | No |
| running | Queue processing active | No |
| waiting | Workflow paused | No |
| completed | Processing completed | Yes |
| needs_review | Operator intervention required | Yes |

## 3. Task status values

| Task status | Meaning |
|---|---|
| queued | Waiting for a worker |
| running | Claimed by a worker |
| acknowledged | Successfully processed and acknowledged |
| failed | Processing failed |
| dead_letter | Retry limit exhausted or invalid task |

## 4. Setup

```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 5. Execution commands

Run the workflow contract:

```cmd
python task_queue.py
```

Run producer, queue, and worker:

```cmd
python task_queue_engine.py
```

Run retry and replay recovery:

```cmd
python task_queue_reliability.py
```

Run integration and observability:

```cmd
python task_queue_observability.py
```

Generate the operator report:

```cmd
python task_queue_runbook.py
```

Run all tests:

```cmd
python -m pytest -v
```

## 6. Queue lifecycle

1. Validate the incoming order payload.
2. Create a task with a unique task ID and run ID.
3. Publish the task into the pending queue.
4. Worker claims the next available task.
5. Task status changes from queued to running.
6. Worker calls the processing adapter.
7. Validate the returned processing result.
8. Acknowledge only after successful validation.
9. Expose the final task status through status lookup.

## 7. Acknowledgement safety

Acknowledgement is permitted only for a task currently held in the
claimed collection with running status. A task cannot be acknowledged
before worker claiming.

The worker records the processing result and acknowledgement time.
Malformed or unsuccessful processing results must not be acknowledged.

## 8. Failure and recovery

- Transient errors are eligible for bounded retry.
- Timeouts are treated as retryable errors.
- Permanent errors are not retried.
- Malformed results are moved to dead-letter handling.
- Retry exhaustion moves the task to dead-letter status.
- Duplicate workflow run identifiers are rejected.
- The idempotency ledger caches protected processing results to
  prevent repeated side effects during replay.

## 9. Integration and observability

The order-processing dependency is isolated behind an injectable
adapter contract. The local fake adapter is deterministic and does
not connect to an external order system.

Structured traces record run ID, task ID, event, status, attempt,
outcome, and next action.

Measurements include poll count, successes, errors, acknowledgement
count, pending count, claimed count, dead-letter count, average
latency, maximum latency, success rate, and oldest queue age.

Sensitive values such as API keys, authorization values, tokens,
passwords, and credentials are redacted from trace details.

## 10. Evidence artifacts

- outputs/task2_task_queue.json
- outputs/task3_queue_recovery.json
- outputs/task4_queue_measurements.json
- outputs/task5_operator_report.json
- traces/task3_queue_recovery_trace.json
- traces/task4_queue_observability.json
- reports/evidence_report.json

## 11. Operator checklist

- [ ] Validate order identifiers and quantity.
- [ ] Confirm the task was published.
- [ ] Check pending, claimed, and acknowledged counts.
- [ ] Confirm the worker owns the claimed task.
- [ ] Verify processing succeeded before acknowledgement.
- [ ] Review retry attempts and dead-letter tasks.
- [ ] Confirm duplicate run triggers are blocked.
- [ ] Verify replay does not repeat protected side effects.
- [ ] Review structured traces and queue measurements.
- [ ] Confirm secrets are redacted.
- [ ] Validate all required evidence files.

## 12. Known limitations

- The queue is in-memory and does not survive process restarts.
- The implementation demonstrates one local worker.
- There is no distributed locking or cross-process visibility timeout.
- Retry scheduling is immediate in this local demonstration.
- The adapter is a fake local implementation.
- Production use requires durable queue storage, worker leases,
  persistent idempotency records, secure credentials, and monitoring.
