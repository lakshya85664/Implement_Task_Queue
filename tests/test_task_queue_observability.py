
import json
import pytest

from task_queue import TaskStatus

from task_queue_engine import (
    InMemoryTaskQueue,
    OrderProducer,
)

from task_queue_observability import (
    FakeOrderAdapter,
    ObservableOrderAdapter,
    lookup_task_status,
    redact_secrets,
    save_observability_evidence,
)


def create_task(queue):
    producer = OrderProducer(queue)

    return producer.publish_order(
        {
            "order_id": "ORD-OBS-TEST",
            "customer_id": "CUST-OBS-TEST",
            "product_id": "PROD-501",
            "quantity": 2,
            "unit_price": 500.0,
            "currency": "INR",
        },
        "RUN-OBS-TEST",
    )


def test_successful_adapter_call_creates_structured_trace():
    queue = InMemoryTaskQueue()
    task = create_task(queue)
    adapter = FakeOrderAdapter()

    observer = ObservableOrderAdapter(adapter, queue)

    claimed = queue.claim()
    result = observer.process(claimed)

    assert result["processing_status"] == "processed"
    assert len(observer.traces) == 1
    assert observer.traces[0]["outcome"] == "success"
    assert observer.traces[0]["next_action"] == "continue_acknowledgement"


def test_adapter_failure_is_traced_and_reraised():
    class FailingAdapter:
        def process(self, task):
            raise RuntimeError("Adapter unavailable")

    queue = InMemoryTaskQueue()
    create_task(queue)

    observer = ObservableOrderAdapter(
        FailingAdapter(),
        queue,
    )

    claimed = queue.claim()

    with pytest.raises(RuntimeError, match="Adapter unavailable"):
        observer.process(claimed)

    assert len(observer.traces) == 1
    assert observer.traces[0]["outcome"] == "error"
    assert observer.traces[0]["next_action"] == "classify_and_recover"


def test_measurements_report_success_counts_and_latency():
    queue = InMemoryTaskQueue()
    create_task(queue)

    observer = ObservableOrderAdapter(
        FakeOrderAdapter(),
        queue,
    )

    claimed = queue.claim()
    observer.process(claimed)

    metrics = observer.measurements()

    assert metrics.poll_count == 1
    assert metrics.success_count == 1
    assert metrics.error_count == 0
    assert metrics.success_rate == 1.0
    assert metrics.average_latency_ms >= 0
    assert metrics.max_latency_ms >= 0


def test_status_lookup_returns_latest_task_status():
    queue = InMemoryTaskQueue()
    task = create_task(queue)

    before = lookup_task_status(queue, task.task_id)

    assert before["status"] == "queued"

    claimed = queue.claim()
    queue.acknowledge(
        claimed.task_id,
        {"processing_status": "processed"},
    )

    after = lookup_task_status(queue, task.task_id)

    assert after["status"] == "acknowledged"


def test_unknown_task_returns_not_found():
    queue = InMemoryTaskQueue()

    result = lookup_task_status(queue, "UNKNOWN-TASK")

    assert result["status"] == "not_found"


def test_recursive_secret_redaction():
    source = {
        "api_key": "secret-value",
        "nested": {
            "authorization": "Bearer secret",
            "items": [
                {"password": "hidden"},
                {"name": "safe"},
            ],
        },
    }

    cleaned = redact_secrets(source)

    assert cleaned["api_key"] == "[REDACTED]"
    assert cleaned["nested"]["authorization"] == "[REDACTED]"
    assert cleaned["nested"]["items"][0]["password"] == "[REDACTED]"
    assert cleaned["nested"]["items"][1]["name"] == "safe"


def test_trace_does_not_expose_adapter_secret():
    queue = InMemoryTaskQueue()
    create_task(queue)

    observer = ObservableOrderAdapter(
        FakeOrderAdapter(),
        queue,
    )

    claimed = queue.claim()
    observer.process(claimed)

    serialized = json.dumps(observer.traces)

    assert "FAKE-ORDER-API-KEY" not in serialized
    assert "[REDACTED]" in serialized


def test_evidence_files_are_generated(tmp_path, monkeypatch):
    import task_queue_observability

    monkeypatch.setattr(
        task_queue_observability,
        "Path",
        lambda value=".": tmp_path / value,
    )

    queue = InMemoryTaskQueue()
    create_task(queue)

    observer = ObservableOrderAdapter(
        FakeOrderAdapter(),
        queue,
    )

    claimed = queue.claim()
    observer.process(claimed)

    metrics = observer.measurements()

    trace_path, measurement_path = save_observability_evidence(
        observer,
        metrics,
    )

    assert trace_path.exists()
    assert measurement_path.exists()

    trace_data = json.loads(trace_path.read_text(encoding="utf-8"))
    metric_data = json.loads(measurement_path.read_text(encoding="utf-8"))

    assert len(trace_data) == 1
    assert metric_data["poll_count"] == 1
