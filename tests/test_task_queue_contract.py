
import pytest

from task_queue import (
    OrderInput,
    TaskPayload,
    QueueTask,
    WorkflowRun,
    WorkflowState,
    TaskStatus,
    transition,
)


def create_test_run():

    order = OrderInput(
        order_id="ORD-TEST-001",
        customer_id="CUST-TEST-001",
        product_id="PROD-001",
        quantity=2,
        unit_price=500.0,
    )

    payload = TaskPayload(order=order)

    task = QueueTask(
        task_id="TASK-TEST-001",
        run_id="RUN-TEST-001",
        payload=payload,
    )

    return WorkflowRun(
        run_id="RUN-TEST-001",
        business_key=order.order_id,
        tasks=[task],
    )


def test_legal_start_transition():

    run = create_test_run()

    transition(run, "start")

    assert run.state == WorkflowState.RUNNING
    assert len(run.events) == 1
    assert len(run.traces) == 1


def test_illegal_transition_is_rejected():

    run = create_test_run()

    with pytest.raises(
        ValueError,
        match="Illegal transition",
    ):
        transition(run, "complete")


def test_pause_and_resume_transitions():

    run = create_test_run()

    transition(run, "start")
    transition(run, "pause")

    assert run.state == WorkflowState.WAITING

    transition(run, "resume")

    assert run.state == WorkflowState.RUNNING


def test_order_input_validation():

    with pytest.raises(ValueError):
        OrderInput(
            order_id="",
            customer_id="CUST-001",
            product_id="PROD-001",
            quantity=1,
            unit_price=100.0,
        )


def test_quantity_must_be_positive():

    with pytest.raises(ValueError):
        OrderInput(
            order_id="ORD-002",
            customer_id="CUST-002",
            product_id="PROD-002",
            quantity=0,
            unit_price=100.0,
        )


def test_task_starts_in_queued_status():

    run = create_test_run()

    assert run.tasks[0].status == TaskStatus.QUEUED
    assert run.tasks[0].attempts == 0


def test_task_payload_contains_order():

    run = create_test_run()

    assert run.tasks[0].payload.order.order_id == "ORD-TEST-001"
    assert run.business_key == "ORD-TEST-001"
