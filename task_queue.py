
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ConfigDict


# ---------------------------------------------------------
# 1. Workflow and task states
# ---------------------------------------------------------

class WorkflowState(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    NEEDS_REVIEW = "needs_review"


class TaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    ACKNOWLEDGED = "acknowledged"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


# ---------------------------------------------------------
# 2. Typed order input
# ---------------------------------------------------------

class OrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    quantity: int = Field(ge=1, le=1000)
    unit_price: float = Field(gt=0)
    currency: str = "INR"


# ---------------------------------------------------------
# 3. Task payload model
# ---------------------------------------------------------

class TaskPayload(BaseModel):
    order: OrderInput
    operation: str = "process_order"
    priority: int = Field(default=5, ge=1, le=10)


# ---------------------------------------------------------
# 4. Queue task model
# ---------------------------------------------------------

class QueueTask(BaseModel):
    task_id: str
    run_id: str
    payload: TaskPayload
    status: TaskStatus = TaskStatus.QUEUED
    attempts: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1)
    owner: str = "task_queue_worker"
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    acknowledged_at: datetime | None = None
    result: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------
# 5. Event model
# ---------------------------------------------------------

class QueueEvent(BaseModel):
    event_id: str
    run_id: str
    task_id: str
    event_type: str
    state: str
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------
# 6. Result model
# ---------------------------------------------------------

class TaskResult(BaseModel):
    task_id: str
    order_id: str
    status: TaskStatus
    message: str
    processed: bool = False


# ---------------------------------------------------------
# 7. Trace model
# ---------------------------------------------------------

class QueueTrace(BaseModel):
    run_id: str
    task_id: str
    event_type: str
    workflow_state: WorkflowState
    task_status: TaskStatus
    attempt: int
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------
# 8. Workflow run model
# ---------------------------------------------------------

class WorkflowRun(BaseModel):
    run_id: str
    business_key: str
    state: WorkflowState = WorkflowState.CREATED
    tasks: list[QueueTask] = Field(default_factory=list)
    events: list[QueueEvent] = Field(default_factory=list)
    traces: list[QueueTrace] = Field(default_factory=list)
    results: list[TaskResult] = Field(default_factory=list)

    def record(
        self,
        task_id: str,
        event_type: str,
        **details: Any,
    ) -> None:

        task = next(
            (item for item in self.tasks if item.task_id == task_id),
            None,
        )

        task_status = (
            task.status if task else TaskStatus.QUEUED
        )

        attempt = task.attempts if task else 0

        self.events.append(
            QueueEvent(
                event_id=f"EVT-{len(self.events) + 1:04d}",
                run_id=self.run_id,
                task_id=task_id,
                event_type=event_type,
                state=self.state.value,
                details=details,
            )
        )

        self.traces.append(
            QueueTrace(
                run_id=self.run_id,
                task_id=task_id,
                event_type=event_type,
                workflow_state=self.state,
                task_status=task_status,
                attempt=attempt,
                details=details,
            )
        )


# ---------------------------------------------------------
# 9. Legal workflow transitions
# ---------------------------------------------------------

LEGAL_TRANSITIONS = {
    (WorkflowState.CREATED, "start"): WorkflowState.RUNNING,
    (WorkflowState.RUNNING, "pause"): WorkflowState.WAITING,
    (WorkflowState.WAITING, "resume"): WorkflowState.RUNNING,
    (WorkflowState.RUNNING, "complete"): WorkflowState.COMPLETED,
    (WorkflowState.RUNNING, "fail"): WorkflowState.NEEDS_REVIEW,
    (WorkflowState.WAITING, "fail"): WorkflowState.NEEDS_REVIEW,
}


def transition(
    run: WorkflowRun,
    event: str,
) -> WorkflowRun:

    next_state = LEGAL_TRANSITIONS.get((run.state, event))

    if next_state is None:
        raise ValueError(
            f"Illegal transition: {run.state.value} + {event}"
        )

    previous_state = run.state
    run.state = next_state

    task_id = (
        run.tasks[-1].task_id
        if run.tasks
        else "NO-TASK"
    )

    run.record(
        task_id,
        "workflow.transition",
        event=event,
        previous_state=previous_state.value,
        next_state=next_state.value,
    )

    return run


# ---------------------------------------------------------
# 10. Runnable Task 1 fixture
# ---------------------------------------------------------

def run_topic_fixture() -> dict[str, Any]:

    order = OrderInput(
        order_id="ORD-1001",
        customer_id="CUST-1001",
        product_id="PROD-501",
        quantity=2,
        unit_price=750.0,
    )

    payload = TaskPayload(
        order=order,
        operation="process_order",
        priority=3,
    )

    task = QueueTask(
        task_id="TASK-1001",
        run_id="RUN-QUEUE-001",
        payload=payload,
    )

    run = WorkflowRun(
        run_id="RUN-QUEUE-001",
        business_key=order.order_id,
        tasks=[task],
    )

    transition(run, "start")

    run.record(
        task.task_id,
        "queue.task_created",
        order_id=order.order_id,
        task_status=task.status.value,
    )

    return {
        "run_id": run.run_id,
        "business_key": run.business_key,
        "workflow_state": run.state.value,
        "task": task.model_dump(mode="json"),
        "events": [
            event.model_dump(mode="json")
            for event in run.events
        ],
        "traces": [
            trace.model_dump(mode="json")
            for trace in run.traces
        ],
    }


if __name__ == "__main__":
    import json

    print("=" * 60)
    print("TOPIC 7 - TASK QUEUE CONTRACT")
    print("=" * 60)

    print(json.dumps(run_topic_fixture(), indent=4))
