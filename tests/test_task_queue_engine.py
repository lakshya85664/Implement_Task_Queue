
import pytest

from pydantic import ValidationError

from task_queue import (
    WorkflowRun,
    WorkflowState,
    TaskStatus,
)

from task_queue_engine import (
    InMemoryTaskQueue,
    OrderProducer,
    OrderWorker,
)


def create_order(order_id="ORD-TEST-2001"):
    return {
        "order_id": order_id,
        "customer_id": "CUST-TEST-2001",
        "product_id": "PROD-501",
        "quantity": 2,
        "unit_price": 500.0,
        "currency": "INR",
    }


def create_run():
    return WorkflowRun(
        run_id="RUN-TEST-2001",
        business_key="ORD-TEST-2001",
        state=WorkflowState.RUNNING,
    )


def test_producer_publishes_task_to_queue():
    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)

    task = producer.publish_order(
        create_order(),
        "RUN-TEST-2001",
    )

    assert task.status == TaskStatus.QUEUED
    assert queue.queue_snapshot()["pending"] == 1
    assert queue.queue_snapshot()["total"] == 1


def test_worker_processes_and_acknowledges_task():
    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)
    worker = OrderWorker(queue)

    run = create_run()

    task = producer.publish_order(
        create_order(),
        run.run_id,
    )
    run.tasks.append(task)

    result = worker.process_next(run)

    assert result is not None
    assert result.processed is True
    assert task.status == TaskStatus.ACKNOWLEDGED
    assert task.acknowledged_at is not None
    assert task.result["total_amount"] == 1000.0
    assert run.state == WorkflowState.COMPLETED


def test_worker_returns_none_when_queue_is_empty():
    queue = InMemoryTaskQueue()
    worker = OrderWorker(queue)

    result = worker.process_next(create_run())

    assert result is None


def test_task_cannot_be_acknowledged_before_claim():
    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)

    task = producer.publish_order(
        create_order(),
        "RUN-TEST-2001",
    )

    with pytest.raises(ValueError, match="not currently claimed"):
        queue.acknowledge(
            task.task_id,
            {"processing_status": "processed"},
        )


def test_invalid_order_is_not_published():
    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)

    invalid_order = create_order()
    invalid_order["quantity"] = 0

    with pytest.raises(ValidationError):
        producer.publish_order(
            invalid_order,
            "RUN-TEST-2001",
        )

    assert queue.queue_snapshot()["total"] == 0


def test_duplicate_task_id_is_rejected():
    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)

    task = producer.publish_order(
        create_order(),
        "RUN-TEST-2001",
    )

    with pytest.raises(ValueError, match="Duplicate task ID"):
        queue.publish(task)


def test_task_visibility_changes_after_acknowledgement():
    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)
    worker = OrderWorker(queue)

    run = create_run()

    task = producer.publish_order(
        create_order(),
        run.run_id,
    )
    run.tasks.append(task)

    before = queue.get_status(task.task_id)

    assert before["status"] == "queued"

    worker.process_next(run)

    after = queue.get_status(task.task_id)

    assert after["status"] == "acknowledged"
    assert after["acknowledged_at"] is not None


def test_queue_snapshot_tracks_task_lifecycle():
    queue = InMemoryTaskQueue()
    producer = OrderProducer(queue)
    worker = OrderWorker(queue)

    run = create_run()

    task = producer.publish_order(
        create_order(),
        run.run_id,
    )
    run.tasks.append(task)

    before = queue.queue_snapshot()

    assert before == {
        "pending": 1,
        "claimed": 0,
        "acknowledged": 0,
        "total": 1,
    }

    worker.process_next(run)

    after = queue.queue_snapshot()

    assert after == {
        "pending": 0,
        "claimed": 0,
        "acknowledged": 1,
        "total": 1,
    }
