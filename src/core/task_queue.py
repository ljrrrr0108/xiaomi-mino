"""
Task Queue — priority-based, thread-safe queue for agent work items.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("TaskQueue")


class TaskPriority(IntEnum):
    """Task scheduling priority levels (lower value = higher priority)."""

    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


class TaskStatus(str):
    """Task lifecycle states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


@dataclass(order=True)
class Task:
    """Represents a unit of work to be executed by an agent.

    Attributes:
        name: Human-readable task name.
        agent_type: Target agent type (e.g. ``"content_agent"``).
        payload: Arbitrary data required by the agent to process this task.
        priority: Scheduling priority (see :class:`TaskPriority`).
        task_id: Unique identifier (auto-generated).
        created_at: Unix creation timestamp.
        status: Current lifecycle state (see :class:`TaskStatus`).
        result: Populated with the agent's output on completion.
        error: Populated with error information on failure.
        retries: How many times the task has been retried.
        max_retries: Maximum retry attempts before marking as failed.
        timeout: Optional execution timeout in seconds.
    """

    priority: int = field(default=TaskPriority.NORMAL)
    name: str = field(default="unnamed_task", compare=False)
    agent_type: str = field(default="", compare=False)
    payload: Any = field(default=None, compare=False)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
    created_at: float = field(default_factory=time.time, compare=False)
    status: str = field(default=TaskStatus.PENDING, compare=False)
    result: Any = field(default=None, compare=False)
    error: Optional[str] = field(default=None, compare=False)
    retries: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    timeout: Optional[float] = field(default=None, compare=False)


class TaskQueue:
    """Async-friendly priority queue for :class:`Task` objects.

    Tasks with lower :attr:`Task.priority` values (i.e. CRITICAL < HIGH < …)
    are dequeued first.  Tasks with equal priority are served FIFO.

    Usage::

        queue = TaskQueue()
        task = Task(name="generate_report", agent_type="data_agent", payload={...})
        await queue.put(task)
        next_task = await queue.get()
    """

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._all_tasks: Dict[str, Task] = {}
        self._sequence: int = 0  # tiebreaker for equal-priority tasks

    async def put(self, task: Task) -> None:
        """Enqueue *task*.

        Args:
            task: The task to add. Its status is set to :attr:`TaskStatus.PENDING`.
        """
        task.status = TaskStatus.PENDING
        self._all_tasks[task.task_id] = task
        # Use (priority, sequence_counter) as the sort key so equal-priority
        # tasks maintain insertion order.
        await self._queue.put((task.priority, self._sequence, task))
        self._sequence += 1
        logger.debug("Enqueued task '%s' (priority=%s)", task.name, task.priority)

    async def get(self) -> Task:
        """Dequeue and return the highest-priority task.

        Blocks until a task is available.
        """
        _, _, task = await self._queue.get()
        task.status = TaskStatus.RUNNING
        logger.debug("Dequeued task '%s' for execution", task.name)
        return task

    def task_done(self) -> None:
        """Signal that the most recently dequeued task has been processed."""
        self._queue.task_done()

    def get_task(self, task_id: str) -> Optional[Task]:
        """Look up a task by its ID (regardless of queue position)."""
        return self._all_tasks.get(task_id)

    def get_all_tasks(self, status: Optional[str] = None) -> List[Task]:
        """Return all tracked tasks, optionally filtered by *status*."""
        tasks = list(self._all_tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        return tasks

    @property
    def size(self) -> int:
        """Number of items currently waiting in the queue."""
        return self._queue.qsize()

    @property
    def empty(self) -> bool:
        """True when no tasks are waiting."""
        return self._queue.empty()
