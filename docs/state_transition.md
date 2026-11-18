
# Task Queue — State Transition Model

## Workflow States

| State | Description | Terminal |
|---|---|---|
| created | Queue workflow initialized | No |
| running | Worker is processing tasks | No |
| waiting | Workflow is paused or waiting | No |
| completed | Workflow completed successfully | Yes |
| needs_review | Operator intervention required | Yes |

## Task Status Values

| Status | Description |
|---|---|
| queued | Task is waiting in the queue |
| running | Worker has claimed the task |
| acknowledged | Task completed and acknowledgement recorded |
| failed | Task processing failed |
| dead_letter | Task exhausted its permitted attempts |

## Legal Workflow Transitions

| Current State | Event | Next State |
|---|---|---|
| created | start | running |
| running | pause | waiting |
| waiting | resume | running |
| running | complete | completed |
| running | fail | needs_review |
| waiting | fail | needs_review |

## State Diagram

```text
CREATED
   |
  start
   |
   v
RUNNING <------------- WAITING
   |                      ^
   | pause                | resume
   v                      |
WAITING ------------------

RUNNING -- complete --> COMPLETED
RUNNING -- fail ------> NEEDS_REVIEW
WAITING -- fail ------> NEEDS_REVIEW
```

## Queue Processing Lifecycle

```text
ORDER INPUT
     |
     v
TASK CREATED
     |
     v
QUEUED
     |
     v
WORKER CLAIMS TASK
     |
     v
RUNNING
     |
     +---- success ----> ACKNOWLEDGED
     |
     +---- failure ----> RETRY / DEAD_LETTER
```

## Design Notes

- Each queue task has a unique task ID and run ID.
- Business keys identify the order associated with the workflow.
- Task status is represented independently from workflow state.
- Events and traces record task and workflow progress.
- Invalid workflow transitions raise a ValueError.
- Acknowledgement is represented as an explicit task status.
- Retry, worker claiming, and safe acknowledgement behavior will be
  implemented in the subsequent tasks.
