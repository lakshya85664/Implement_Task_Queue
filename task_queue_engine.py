
from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from task_queue import (
    OrderInput,
    TaskPayload,
    QueueTask,
    WorkflowRun,
    WorkflowState,
    TaskStatus,
    TaskResult,
    transition,
)


# ---------------------------------------------------------
# 1. In-memory task queue
# ---------------------------------------------------------

class InMemoryTaskQueue:

    def __init__(self):
        self.pending: deque[QueueTask] = deque()
        self.tasks: dict[str, QueueTask] = {}
        self.claimed: dict[str, QueueTask] = {}
        self.acknowledged: dict[str, QueueTask] = {}

    def publish(self, task: QueueTask) -> None:

        if task.task_id in self.tasks:
            raise ValueError(
                f"Duplicate task ID: {task.task_id}"
            )

        if task.status != TaskStatus.QUEUED:
            raise ValueError(
                "Only queued tasks can be published"
            )

        self.tasks[task.task_id] = task
        self.pending.append(task)

    def claim(self) -> QueueTask | None:

        if not self.pending:
            return None

        task = self.pending.popleft()

        if task.status != TaskStatus.QUEUED:
            raise ValueError(
                "Only queued tasks can be claimed"
            )

        task.status = TaskStatus.RUNNING
        task.attempts += 1

        self.claimed[task.task_id] = task

        return task

    def acknowledge(
        self,
        task_id: str,
        result: dict[str, Any],
    ) -> QueueTask:

        task = self.claimed.get(task_id)

        if task is None:
            raise ValueError(
                f"Task is not currently claimed: {task_id}"
            )

        if task.status != TaskStatus.RUNNING:
            raise ValueError(
                "Only running tasks can be acknowledged"
            )

        task.result = result
        task.status = TaskStatus.ACKNOWLEDGED
        task.acknowledged_at = datetime.now(timezone.utc)

        del self.claimed[task_id]
        self.acknowledged[task_id] = task

        return task

    def get_status(self, task_id: str) -> dict[str, Any]:

        task = self.tasks.get(task_id)

        if task is None:
            return {
                "task_id": task_id,
                "status": "not_found",
            }

        return {
            "task_id": task.task_id,
            "run_id": task.run_id,
            "order_id": task.payload.order.order_id,
            "status": task.status.value,
            "attempts": task.attempts,
            "acknowledged_at": (
                task.acknowledged_at.isoformat()
                if task.acknowledged_at
                else None
            ),
        }

    def queue_snapshot(self) -> dict[str, int]:

        return {
            "pending": len(self.pending),
            "claimed": len(self.claimed),
            "acknowledged": len(self.acknowledged),
            "total": len(self.tasks),
        }


# ---------------------------------------------------------
# 2. Producer
# ---------------------------------------------------------

class OrderProducer:

    def __init__(self, queue: InMemoryTaskQueue):
        self.queue = queue
        self.sequence = 0

    def publish_order(
        self,
        order_data: dict[str, Any],
        run_id: str,
    ) -> QueueTask:

        # Validate before creating or publishing a task.
        order = OrderInput.model_validate(order_data)

        payload = TaskPayload(
            order=order,
            operation="process_order",
            priority=3,
        )

        self.sequence += 1

        task = QueueTask(
            task_id=f"TASK-{self.sequence:04d}",
            run_id=run_id,
            payload=payload,
        )

        self.queue.publish(task)

        return task


# ---------------------------------------------------------
# 3. Worker
# ---------------------------------------------------------

class OrderWorker:

    def __init__(self, queue: InMemoryTaskQueue):
        self.queue = queue

    def process_next(
        self,
        run: WorkflowRun,
    ) -> TaskResult | None:

        task = self.queue.claim()

        if task is None:
            return None

        # Record the worker claim.
        run.record(
            task.task_id,
            "worker.task_claimed",
            attempt=task.attempts,
        )

        order = task.payload.order

        # Deterministic local order-processing behavior.
        total_amount = round(
            order.quantity * order.unit_price,
            2,
        )

        processing_result = {
            "order_id": order.order_id,
            "processing_status": "processed",
            "quantity": order.quantity,
            "unit_price": order.unit_price,
            "total_amount": total_amount,
            "currency": order.currency,
        }

        # Acknowledge only after processing has succeeded.
        self.queue.acknowledge(
            task.task_id,
            processing_result,
        )

        run.record(
            task.task_id,
            "worker.task_acknowledged",
            total_amount=total_amount,
            task_status=task.status.value,
        )

        result = TaskResult(
            task_id=task.task_id,
            order_id=order.order_id,
            status=task.status,
            message="Order processed and acknowledged successfully.",
            processed=True,
        )

        run.results.append(result)

        # Complete workflow after successful acknowledgement.
        if not self.queue.pending and not self.queue.claimed:
            transition(run, "complete")

        return result


# ---------------------------------------------------------
# 4. Evidence generation
# ---------------------------------------------------------

def save_task_queue_evidence(
    run: WorkflowRun,
    queue: InMemoryTaskQueue,
    result: TaskResult,
) -> Path:

    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / "task2_task_queue.json"

    evidence = {
        "run_id": run.run_id,
        "business_key": run.business_key,
        "workflow_state": run.state.value,
        "task": run.tasks[0].model_dump(mode="json"),
        "result": result.model_dump(mode="json"),
        "queue_snapshot": queue.queue_snapshot(),
        "task_visibility": queue.get_status(
            result.task_id
        ),
        "events": [
            event.model_dump(mode="json")
            for event in run.events
        ],
        "traces": [
            trace.model_dump(mode="json")
            for trace in run.traces
        ],
    }

    output_path.write_text(
        json.dumps(evidence, indent=4),
        encoding="utf-8",
    )

    return output_path


# ---------------------------------------------------------
# 5. Runnable happy path
# ---------------------------------------------------------

def run_happy_path() -> None:

    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)
    worker = OrderWorker(queue)

    run = WorkflowRun(
        run_id="RUN-QUEUE-2001",
        business_key="ORD-2001",
    )

    transition(run, "start")

    order_data = {
        "order_id": "ORD-2001",
        "customer_id": "CUST-2001",
        "product_id": "PROD-501",
        "quantity": 2,
        "unit_price": 750.0,
        "currency": "INR",
    }

    task = producer.publish_order(
        order_data,
        run.run_id,
    )

    run.tasks.append(task)

    run.record(
        task.task_id,
        "producer.task_published",
        order_id=task.payload.order.order_id,
    )

    print(f"Published task: {task.task_id}")
    print(f"Queue before worker: {queue.queue_snapshot()}")

    result = worker.process_next(run)

    if result is None:
        raise RuntimeError("Worker did not receive a task")

    output_path = save_task_queue_evidence(
        run,
        queue,
        result,
    )

    print("=" * 60)
    print("TASK 2 - TASK QUEUE EXECUTION")
    print("=" * 60)
    print(f"Run ID: {run.run_id}")
    print(f"Order ID: {task.payload.order.order_id}")
    print(f"Task ID: {task.task_id}")
    print(f"Task status: {task.status.value}")
    print(f"Workflow state: {run.state.value}")
    print(f"Attempts: {task.attempts}")
    print(f"Total amount: {result.model_dump()['message']}")
    print(f"Queue snapshot: {queue.queue_snapshot()}")
    print(f"Evidence saved: {output_path}")

    print("\nTask visibility:")
    print(json.dumps(queue.get_status(task.task_id), indent=4))

    print("\nProcessing result:")
    print(json.dumps(task.result, indent=4))


if __name__ == "__main__":
    run_happy_path()
