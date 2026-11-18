
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field

from task_queue import QueueTask, TaskStatus
from task_queue_engine import InMemoryTaskQueue


# ---------------------------------------------------------
# 1. Adapter contract
# ---------------------------------------------------------

class OrderProcessingAdapter(Protocol):

    def process(
        self,
        task: QueueTask,
    ) -> dict[str, Any]:
        ...


# ---------------------------------------------------------
# 2. Fake external dependency
# ---------------------------------------------------------

class FakeOrderAdapter:

    def __init__(self):
        self.call_count = 0

    def process(
        self,
        task: QueueTask,
    ) -> dict[str, Any]:

        self.call_count += 1

        order = task.payload.order

        return {
            "order_id": order.order_id,
            "processing_status": "processed",
            "total_amount": round(
                order.quantity * order.unit_price,
                2,
            ),
            "currency": order.currency,
            "api_key": "FAKE-ORDER-API-KEY",
        }


# ---------------------------------------------------------
# 3. Recursive secret redaction
# ---------------------------------------------------------

SECRET_MARKERS = (
    "api_key",
    "authorization",
    "token",
    "password",
    "secret",
    "credential",
)


def redact_secrets(value: Any) -> Any:

    if isinstance(value, dict):
        cleaned = {}

        for key, item in value.items():
            if any(
                marker in str(key).lower()
                for marker in SECRET_MARKERS
            ):
                cleaned[key] = "[REDACTED]"
            else:
                cleaned[key] = redact_secrets(item)

        return cleaned

    if isinstance(value, list):
        return [
            redact_secrets(item)
            for item in value
        ]

    if isinstance(value, tuple):
        return [
            redact_secrets(item)
            for item in value
        ]

    return value


# ---------------------------------------------------------
# 4. Measurement model
# ---------------------------------------------------------

class QueueMeasurements(BaseModel):
    poll_count: int = 0
    success_count: int = 0
    error_count: int = 0
    acknowledged_count: int = 0
    pending_count: int = 0
    claimed_count: int = 0
    dead_letter_count: int = 0
    average_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    success_rate: float = 0.0
    oldest_queue_age_ms: float = 0.0


# ---------------------------------------------------------
# 5. Observable adapter wrapper
# ---------------------------------------------------------

class ObservableOrderAdapter:

    def __init__(
        self,
        adapter: OrderProcessingAdapter,
        queue: InMemoryTaskQueue,
    ):
        self.adapter = adapter
        self.queue = queue
        self.traces: list[dict[str, Any]] = []
        self.latencies: list[float] = []

    def process(
        self,
        task: QueueTask,
    ) -> dict[str, Any]:

        started = time.perf_counter()

        try:
            result = self.adapter.process(task)

            latency_ms = (
                time.perf_counter() - started
            ) * 1000

            self.latencies.append(latency_ms)

            self._record(
                task,
                "adapter.process_succeeded",
                "success",
                "continue_acknowledgement",
                latency_ms=round(latency_ms, 4),
                response=result,
            )

            return result

        except Exception as exc:
            latency_ms = (
                time.perf_counter() - started
            ) * 1000

            self.latencies.append(latency_ms)

            self._record(
                task,
                "adapter.process_failed",
                "error",
                "classify_and_recover",
                latency_ms=round(latency_ms, 4),
                error_type=type(exc).__name__,
            )

            raise

    def _record(
        self,
        task: QueueTask,
        event: str,
        outcome: str,
        next_action: str,
        **details: Any,
    ) -> None:

        trace = {
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "run_id": task.run_id,
            "task_id": task.task_id,
            "event": event,
            "task_status": task.status.value,
            "attempt": task.attempts,
            "outcome": outcome,
            "next_action": next_action,
            "details": details,
        }

        self.traces.append(
            redact_secrets(trace)
        )

    def measurements(self) -> QueueMeasurements:

        poll_count = len(self.latencies)

        success_count = sum(
            1 for trace in self.traces
            if trace["outcome"] == "success"
        )

        error_count = sum(
            1 for trace in self.traces
            if trace["outcome"] == "error"
        )

        latencies = self.latencies

        average_latency = (
            sum(latencies) / len(latencies)
            if latencies else 0.0
        )

        max_latency = (
            max(latencies)
            if latencies else 0.0
        )

        success_rate = (
            success_count / poll_count
            if poll_count else 0.0
        )

        now = datetime.now(timezone.utc)

        pending_tasks = list(self.queue.pending)

        oldest_age = 0.0

        if pending_tasks:
            oldest = min(
                task.created_at
                for task in pending_tasks
            )
            oldest_age = max(
                0.0,
                (now - oldest).total_seconds() * 1000,
            )

        return QueueMeasurements(
            poll_count=poll_count,
            success_count=success_count,
            error_count=error_count,
            acknowledged_count=len(
                self.queue.acknowledged
            ),
            pending_count=len(self.queue.pending),
            claimed_count=len(self.queue.claimed),
            dead_letter_count=sum(
                1 for task in self.queue.tasks.values()
                if task.status == TaskStatus.DEAD_LETTER
            ),
            average_latency_ms=round(average_latency, 4),
            max_latency_ms=round(max_latency, 4),
            success_rate=round(success_rate, 4),
            oldest_queue_age_ms=round(oldest_age, 4),
        )


# ---------------------------------------------------------
# 6. Status lookup
# ---------------------------------------------------------

def lookup_task_status(
    queue: InMemoryTaskQueue,
    task_id: str,
) -> dict[str, Any]:

    return queue.get_status(task_id)


# ---------------------------------------------------------
# 7. Evidence generation
# ---------------------------------------------------------

def save_observability_evidence(
    observer: ObservableOrderAdapter,
    measurements: QueueMeasurements,
) -> tuple[Path, Path]:

    output_dir = Path("outputs")
    trace_dir = Path("traces")

    output_dir.mkdir(exist_ok=True)
    trace_dir.mkdir(exist_ok=True)

    trace_path = (
        trace_dir / "task4_queue_observability.json"
    )

    measurement_path = (
        output_dir / "task4_queue_measurements.json"
    )

    trace_path.write_text(
        json.dumps(observer.traces, indent=4),
        encoding="utf-8",
    )

    measurement_path.write_text(
        measurements.model_dump_json(indent=4),
        encoding="utf-8",
    )

    return trace_path, measurement_path


# ---------------------------------------------------------
# 8. Demonstration
# ---------------------------------------------------------

def run_observability_demo() -> None:

    from task_queue_engine import OrderProducer

    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)

    adapter = FakeOrderAdapter()

    observer = ObservableOrderAdapter(
        adapter,
        queue,
    )

    task = producer.publish_order(
        {
            "order_id": "ORD-OBS-001",
            "customer_id": "CUST-OBS-001",
            "product_id": "PROD-501",
            "quantity": 3,
            "unit_price": 400.0,
            "currency": "INR",
        },
        "RUN-OBS-001",
    )

    # Claim task using the queue's normal worker claim.
    claimed_task = queue.claim()

    if claimed_task is None:
        raise RuntimeError("No task available for worker")

    # Call the injected adapter.
    result = observer.process(claimed_task)

    # Acknowledge only after adapter success.
    queue.acknowledge(
        claimed_task.task_id,
        redact_secrets(result),
    )

    metrics = observer.measurements()

    trace_path, measurement_path = (
        save_observability_evidence(
            observer,
            metrics,
        )
    )

    print("=" * 60)
    print("TASK 4 - TASK QUEUE OBSERVABILITY")
    print("=" * 60)
    print(f"Task ID: {task.task_id}")
    print(f"Adapter calls: {adapter.call_count}")
    print(f"Final status: {task.status.value}")
    print(
        "Status lookup:",
        json.dumps(
            lookup_task_status(queue, task.task_id)
        ),
    )

    print("\nMeasurements:")
    print(metrics.model_dump_json(indent=4))

    print(f"\nTrace saved: {trace_path}")
    print(f"Measurements saved: {measurement_path}")

    print("\nRedacted traces:")
    print(json.dumps(observer.traces, indent=4))


if __name__ == "__main__":
    run_observability_demo()
