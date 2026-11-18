
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel, Field, ValidationError

from task_queue import (
    QueueTask,
    TaskStatus,
)

from task_queue_engine import InMemoryTaskQueue


# ---------------------------------------------------------
# 1. Error classification
# ---------------------------------------------------------

class TransientTaskError(Exception):
    """A temporary processing error that may succeed on retry."""


class PermanentTaskError(Exception):
    """A non-retryable processing error."""


class TaskTimeoutError(TransientTaskError):
    """A task processing timeout."""


class QuarantinedTaskError(Exception):
    """A malformed task result that must not be acknowledged."""


class DuplicateRunError(Exception):
    """A duplicate workflow trigger was rejected."""


# ---------------------------------------------------------
# 2. Validate worker results
# ---------------------------------------------------------

class ProcessingResponse(BaseModel):
    order_id: str = Field(min_length=1)
    processing_status: str
    total_amount: float = Field(gt=0)
    currency: str = Field(min_length=1)


# ---------------------------------------------------------
# 3. Run registry
# ---------------------------------------------------------

class RunRegistry:

    def __init__(self):
        self.run_ids: set[str] = set()

    def claim(self, run_id: str) -> None:
        if run_id in self.run_ids:
            raise DuplicateRunError(
                f"Duplicate workflow trigger blocked: {run_id}"
            )

        self.run_ids.add(run_id)


# ---------------------------------------------------------
# 4. Idempotency ledger
# ---------------------------------------------------------

class IdempotencyLedger:

    def __init__(self):
        self.results: dict[str, dict[str, Any]] = {}

    def execute_once(
        self,
        key: str,
        action: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:

        if key in self.results:
            return self.results[key]

        result = action()
        self.results[key] = result

        return result


# ---------------------------------------------------------
# 5. Fake protected order processor
# ---------------------------------------------------------

class FakeOrderProcessor:

    def __init__(self):
        self.side_effect_count = 0

    def process(self, task: QueueTask) -> dict[str, Any]:
        self.side_effect_count += 1

        order = task.payload.order

        return {
            "order_id": order.order_id,
            "processing_status": "processed",
            "total_amount": round(
                order.quantity * order.unit_price,
                2,
            ),
            "currency": order.currency,
        }


# ---------------------------------------------------------
# 6. Reliable queue worker
# ---------------------------------------------------------

class ReliableQueueWorker:

    def __init__(
        self,
        queue: InMemoryTaskQueue,
        processor: Any,
        ledger: IdempotencyLedger,
    ):
        self.queue = queue
        self.processor = processor
        self.ledger = ledger
        self.trace: list[dict[str, Any]] = []
        self.dead_letters: dict[str, QueueTask] = {}

    def _record(
        self,
        task: QueueTask,
        event: str,
        outcome: str,
        next_action: str,
        **details: Any,
    ) -> None:

        self.trace.append({
            "run_id": task.run_id,
            "task_id": task.task_id,
            "event": event,
            "attempt": task.attempts,
            "task_status": task.status.value,
            "outcome": outcome,
            "next_action": next_action,
            "details": details,
        })

    def _retry(self, task: QueueTask, reason: str) -> None:

        if task.task_id in self.queue.claimed:
            del self.queue.claimed[task.task_id]

        if task.attempts < task.max_attempts:
            task.status = TaskStatus.QUEUED
            self.queue.pending.append(task)

            self._record(
                task,
                "task.retry_scheduled",
                "retryable_error",
                "retry",
                reason=reason,
            )
        else:
            task.status = TaskStatus.DEAD_LETTER
            self.dead_letters[task.task_id] = task

            self._record(
                task,
                "task.retry_exhausted",
                "retry_limit_reached",
                "needs_review",
                reason=reason,
            )

    def _dead_letter(self, task: QueueTask, reason: str) -> None:

        if task.task_id in self.queue.claimed:
            del self.queue.claimed[task.task_id]

        task.status = TaskStatus.DEAD_LETTER
        self.dead_letters[task.task_id] = task

        self._record(
            task,
            "task.dead_lettered",
            "permanent_or_invalid_error",
            "needs_review",
            reason=reason,
        )

    def process_next(self) -> QueueTask | None:

        task = self.queue.claim()

        if task is None:
            return None

        self._record(
            task,
            "task.claimed",
            "claimed",
            "process",
        )

        try:
            # Process once per stable task ID.
            raw_result = self.ledger.execute_once(
                f"{task.run_id}:{task.task_id}:order_processing",
                lambda: self.processor.process(task),
            )

            # Validate before acknowledgement.
            response = ProcessingResponse.model_validate(raw_result)

            if response.processing_status != "processed":
                raise QuarantinedTaskError(
                    "Worker result is not marked processed"
                )

            if response.order_id != task.payload.order.order_id:
                raise QuarantinedTaskError(
                    "Worker result order ID does not match task"
                )

            self.queue.acknowledge(
                task.task_id,
                response.model_dump(),
            )

            self._record(
                task,
                "task.acknowledged",
                "success",
                "complete",
                total_amount=response.total_amount,
            )

            return task

        except (TransientTaskError, TimeoutError) as exc:
            self._retry(task, type(exc).__name__)
            return task

        except PermanentTaskError as exc:
            self._dead_letter(task, type(exc).__name__)
            return task

        except (ValidationError, QuarantinedTaskError) as exc:
            self._dead_letter(task, type(exc).__name__)
            return task

    def replay_acknowledgement(
        self,
        task: QueueTask,
    ) -> dict[str, Any]:

        key = f"{task.run_id}:{task.task_id}:order_processing"

        return self.ledger.execute_once(
            key,
            lambda: self.processor.process(task),
        )


# ---------------------------------------------------------
# 7. Failure fixtures
# ---------------------------------------------------------

class SequenceProcessor:

    def __init__(self, outcomes: list[Any]):
        self.outcomes = outcomes
        self.call_count = 0

    def process(self, task: QueueTask) -> dict[str, Any]:

        outcome = self.outcomes[self.call_count]
        self.call_count += 1

        if isinstance(outcome, Exception):
            raise outcome

        if callable(outcome):
            return outcome(task)

        return outcome


# ---------------------------------------------------------
# 8. Save recovery evidence
# ---------------------------------------------------------

def save_recovery_evidence(
    worker: ReliableQueueWorker,
    processor: Any,
) -> Path:

    output_dir = Path("outputs")
    trace_dir = Path("traces")

    output_dir.mkdir(exist_ok=True)
    trace_dir.mkdir(exist_ok=True)

    evidence = {
        "trace_count": len(worker.trace),
        "trace": worker.trace,
        "dead_letter_count": len(worker.dead_letters),
        "dead_letter_task_ids": list(worker.dead_letters),
        "processor_side_effect_count": getattr(
            processor,
            "side_effect_count",
            getattr(processor, "call_count", None),
        ),
    }

    output_path = output_dir / "task3_queue_recovery.json"
    trace_path = trace_dir / "task3_queue_recovery_trace.json"

    output_path.write_text(
        json.dumps(evidence, indent=4),
        encoding="utf-8",
    )

    trace_path.write_text(
        json.dumps(worker.trace, indent=4),
        encoding="utf-8",
    )

    return output_path


# ---------------------------------------------------------
# 9. Runnable recovery demonstration
# ---------------------------------------------------------

def run_recovery_demo() -> None:

    from task_queue_engine import OrderProducer

    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)

    task = producer.publish_order(
        {
            "order_id": "ORD-RECOVERY-001",
            "customer_id": "CUST-RECOVERY-001",
            "product_id": "PROD-501",
            "quantity": 2,
            "unit_price": 500.0,
            "currency": "INR",
        },
        "RUN-RECOVERY-001",
    )

    processor = SequenceProcessor([
        TransientTaskError("Temporary database issue"),
        TaskTimeoutError("Processing timed out"),
        {
            "order_id": "ORD-RECOVERY-001",
            "processing_status": "processed",
            "total_amount": 1000.0,
            "currency": "INR",
        },
    ])

    ledger = IdempotencyLedger()

    worker = ReliableQueueWorker(
        queue,
        processor,
        ledger,
    )

    # First attempt: transient error.
    worker.process_next()

    # Second attempt: timeout.
    worker.process_next()

    # Third attempt: success and acknowledgement.
    worker.process_next()

    # Replay the protected action; it must use the cached result.
    replay_result = worker.replay_acknowledgement(task)

    output_path = save_recovery_evidence(worker, processor)

    print("=" * 60)
    print("TASK 3 - TASK QUEUE FAILURE AND RECOVERY")
    print("=" * 60)
    print(f"Task ID: {task.task_id}")
    print(f"Final task status: {task.status.value}")
    print(f"Attempts: {task.attempts}")
    print(f"Processor calls: {processor.call_count}")
    print(f"Replay result: {replay_result['processing_status']}")
    print(f"Trace entries: {len(worker.trace)}")
    print(f"Evidence saved: {output_path}")

    print("\nRecovery trace:")
    print(json.dumps(worker.trace, indent=4))


if __name__ == "__main__":
    run_recovery_demo()
