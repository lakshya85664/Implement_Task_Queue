
import pytest

from task_queue import TaskStatus

from task_queue_engine import (
    InMemoryTaskQueue,
    OrderProducer,
)

from task_queue_reliability import (
    TransientTaskError,
    TaskTimeoutError,
    PermanentTaskError,
    DuplicateRunError,
    QuarantinedTaskError,
    RunRegistry,
    IdempotencyLedger,
    FakeOrderProcessor,
    ReliableQueueWorker,
    SequenceProcessor,
)


def create_task(queue, max_attempts=3):
    producer = OrderProducer(queue)

    task = producer.publish_order(
        {
            "order_id": "ORD-TEST-3001",
            "customer_id": "CUST-TEST-3001",
            "product_id": "PROD-501",
            "quantity": 2,
            "unit_price": 500.0,
            "currency": "INR",
        },
        "RUN-TEST-3001",
    )

    task.max_attempts = max_attempts
    return task


def test_transient_error_and_timeout_then_success():
    queue = InMemoryTaskQueue()
    task = create_task(queue)

    processor = SequenceProcessor([
        TransientTaskError("Temporary error"),
        TaskTimeoutError("Timeout"),
        {
            "order_id": "ORD-TEST-3001",
            "processing_status": "processed",
            "total_amount": 1000.0,
            "currency": "INR",
        },
    ])

    worker = ReliableQueueWorker(
        queue,
        processor,
        IdempotencyLedger(),
    )

    worker.process_next()
    assert task.status == TaskStatus.QUEUED

    worker.process_next()
    assert task.status == TaskStatus.QUEUED

    worker.process_next()

    assert task.status == TaskStatus.ACKNOWLEDGED
    assert task.attempts == 3
    assert processor.call_count == 3


def test_retry_exhaustion_moves_task_to_dead_letter():
    queue = InMemoryTaskQueue()
    task = create_task(queue, max_attempts=2)

    processor = SequenceProcessor([
        TransientTaskError("Temporary error"),
        TransientTaskError("Still failing"),
    ])

    worker = ReliableQueueWorker(
        queue,
        processor,
        IdempotencyLedger(),
    )

    worker.process_next()
    worker.process_next()

    assert task.status == TaskStatus.DEAD_LETTER
    assert task.task_id in worker.dead_letters
    assert worker.trace[-1]["next_action"] == "needs_review"


def test_permanent_failure_is_not_retried():
    queue = InMemoryTaskQueue()
    task = create_task(queue)

    processor = SequenceProcessor([
        PermanentTaskError("Invalid order account"),
    ])

    worker = ReliableQueueWorker(
        queue,
        processor,
        IdempotencyLedger(),
    )

    worker.process_next()

    assert task.status == TaskStatus.DEAD_LETTER
    assert processor.call_count == 1


def test_malformed_result_is_dead_lettered():
    queue = InMemoryTaskQueue()
    task = create_task(queue)

    processor = SequenceProcessor([
        {
            "order_id": "ORD-TEST-3001",
            "processing_status": "processed",
            # Missing total_amount and currency.
        },
    ])

    worker = ReliableQueueWorker(
        queue,
        processor,
        IdempotencyLedger(),
    )

    worker.process_next()

    assert task.status == TaskStatus.DEAD_LETTER
    assert worker.trace[-1]["next_action"] == "needs_review"


def test_wrong_order_result_is_quarantined():
    queue = InMemoryTaskQueue()
    task = create_task(queue)

    processor = SequenceProcessor([
        {
            "order_id": "ORD-WRONG",
            "processing_status": "processed",
            "total_amount": 1000.0,
            "currency": "INR",
        },
    ])

    worker = ReliableQueueWorker(
        queue,
        processor,
        IdempotencyLedger(),
    )

    worker.process_next()

    assert task.status == TaskStatus.DEAD_LETTER
    assert worker.trace[-1]["details"]["reason"] == "QuarantinedTaskError"


def test_duplicate_run_trigger_is_blocked():
    registry = RunRegistry()

    registry.claim("RUN-3001")

    with pytest.raises(DuplicateRunError):
        registry.claim("RUN-3001")


def test_idempotency_prevents_repeated_processing_side_effect():
    queue = InMemoryTaskQueue()
    task = create_task(queue)

    processor = FakeOrderProcessor()
    ledger = IdempotencyLedger()

    worker = ReliableQueueWorker(
        queue,
        processor,
        ledger,
    )

    worker.process_next()

    first_count = processor.side_effect_count

    replay = worker.replay_acknowledgement(task)

    assert replay["processing_status"] == "processed"
    assert processor.side_effect_count == first_count == 1


def test_retry_trace_contains_explicit_next_action():
    queue = InMemoryTaskQueue()
    create_task(queue)

    processor = SequenceProcessor([
        TransientTaskError("Temporary error"),
    ])

    worker = ReliableQueueWorker(
        queue,
        processor,
        IdempotencyLedger(),
    )

    worker.process_next()

    assert worker.trace[0]["event"] == "task.claimed"
    assert worker.trace[1]["event"] == "task.retry_scheduled"
    assert worker.trace[1]["next_action"] == "retry"
