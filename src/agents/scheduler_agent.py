"""
Scheduler Agent — manages recurring and one-shot task scheduling.

Supported operations (set in ``task.payload["operation"]``):

* ``schedule``    — register a new scheduled job
* ``cancel``      — cancel a scheduled job
* ``list_jobs``   — return all registered jobs
* ``run_now``     — immediately trigger a scheduled job by ID
"""

import asyncio
import time
import uuid
from typing import Any, Callable, Coroutine, Dict, List, Optional

from src.agents.base_agent import BaseAgent
from src.core.event_system import EventSystem
from src.core.message_bus import MessageBus
from src.core.task_queue import Task, TaskPriority, TaskQueue
from src.utils.logger import get_logger


class ScheduledJob:
    """Represents a recurring or one-shot scheduled job.

    Args:
        job_id: Unique identifier.
        name: Human-readable job name.
        interval: Execution interval in seconds (None for one-shot).
        next_run: Unix timestamp of the next scheduled run.
        callback: Async callable to invoke when the job fires.
        max_runs: Maximum number of executions (None = unlimited).
        enabled: Whether the job is currently active.
    """

    def __init__(
        self,
        name: str,
        interval: Optional[float],
        next_run: float,
        callback: Callable[[], Coroutine],
        max_runs: Optional[int] = None,
        enabled: bool = True,
    ) -> None:
        self.job_id: str = str(uuid.uuid4())
        self.name = name
        self.interval = interval
        self.next_run = next_run
        self.callback = callback
        self.max_runs = max_runs
        self.run_count: int = 0
        self.enabled = enabled
        self.created_at: float = time.time()
        self.last_run: Optional[float] = None
        self.last_error: Optional[str] = None


class SchedulerAgent(BaseAgent):
    """Agent that manages timed and recurring task execution.

    The internal scheduler loop polls every second and fires any due jobs.

    Args:
        message_bus: Shared message bus.
        task_queue: Shared task queue.
        event_system: Shared event system.
        config: Scheduler configuration (``config["agents"]["scheduler_agent"]``).
    """

    def __init__(
        self,
        message_bus: MessageBus,
        task_queue: TaskQueue,
        event_system: EventSystem,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            name="任务调度Agent",
            agent_type="scheduler_agent",
            message_bus=message_bus,
            task_queue=task_queue,
            event_system=event_system,
            config=config or {},
        )
        self._jobs: Dict[str, ScheduledJob] = {}
        self._max_retries: int = self.config.get("max_retries", 3)
        self._retry_delay: float = float(self.config.get("retry_delay", 60))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        asyncio.create_task(self._scheduler_loop())
        self.logger.info("Scheduler loop started")

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    async def process_task(self, task: Task) -> Any:
        """Handle scheduler management operations.

        Args:
            task: Work item with ``payload["operation"]``.

        Returns:
            Operation-specific result.
        """
        payload: Dict[str, Any] = task.payload or {}
        operation = payload.get("operation", "list_jobs")

        dispatch = {
            "schedule": self._schedule,
            "cancel": self._cancel,
            "list_jobs": self._list_jobs,
            "run_now": self._run_now,
        }
        handler = dispatch.get(operation)
        if handler is None:
            raise ValueError(f"Unknown scheduler operation: '{operation}'")

        self.logger.info("Processing scheduler operation '%s' (task=%s)", operation, task.name)
        return await handler(payload)

    # ------------------------------------------------------------------
    # Operation handlers
    # ------------------------------------------------------------------

    async def _schedule(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Register a new scheduled job that submits tasks to the task queue.

        Payload keys:
            job_name (str): Human-readable job name.
            target_agent (str): Agent type to submit tasks to.
            task_name (str): Name used when creating the task.
            task_payload (dict): Payload forwarded to the target task.
            interval (float|None): Seconds between runs; None for one-shot.
            delay (float): Seconds to wait before the first run (default 0).
            max_runs (int|None): Maximum run count.
        """
        job_name = payload.get("job_name", "unnamed_job")
        target_agent = payload.get("target_agent", "data_agent")
        task_name = payload.get("task_name", job_name)
        task_payload = payload.get("task_payload", {})
        interval = payload.get("interval")
        delay = float(payload.get("delay", 0))
        max_runs = payload.get("max_runs")

        queue_ref = self.task_queue  # closure

        async def _callback() -> None:
            t = Task(
                name=task_name,
                agent_type=target_agent,
                payload=task_payload,
                priority=TaskPriority.NORMAL,
                max_retries=self._max_retries,
            )
            await queue_ref.put(t)
            self.logger.info("Scheduled job '%s' submitted task '%s'", job_name, task_name)

        job = ScheduledJob(
            name=job_name,
            interval=interval,
            next_run=time.time() + delay,
            callback=_callback,
            max_runs=max_runs,
        )
        self._jobs[job.job_id] = job
        self.logger.info("Registered job '%s' (id=%s, interval=%s)", job_name, job.job_id, interval)
        return {"job_id": job.job_id, "job_name": job_name, "next_run": job.next_run}

    async def _cancel(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel a scheduled job.

        Payload keys:
            job_id (str): ID of the job to cancel.
        """
        job_id = payload.get("job_id")
        if job_id not in self._jobs:
            raise KeyError(f"Job '{job_id}' not found")
        self._jobs[job_id].enabled = False
        del self._jobs[job_id]
        self.logger.info("Cancelled job id='%s'", job_id)
        return {"job_id": job_id, "status": "cancelled"}

    async def _list_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Return a summary of all registered jobs."""
        jobs = [
            {
                "job_id": j.job_id,
                "name": j.name,
                "interval": j.interval,
                "next_run": j.next_run,
                "run_count": j.run_count,
                "enabled": j.enabled,
                "last_run": j.last_run,
                "last_error": j.last_error,
            }
            for j in self._jobs.values()
        ]
        return {"jobs": jobs, "count": len(jobs)}

    async def _run_now(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Immediately fire a scheduled job.

        Payload keys:
            job_id (str): ID of the job to run immediately.
        """
        job_id = payload.get("job_id")
        if job_id not in self._jobs:
            raise KeyError(f"Job '{job_id}' not found")
        job = self._jobs[job_id]
        await self._fire_job(job)
        return {"job_id": job_id, "fired": True}

    # ------------------------------------------------------------------
    # Scheduler loop
    # ------------------------------------------------------------------

    async def _scheduler_loop(self) -> None:
        """Poll for due jobs and fire them."""
        while self._running:
            now = time.time()
            due: List[ScheduledJob] = [
                j for j in list(self._jobs.values()) if j.enabled and j.next_run <= now
            ]
            for job in due:
                asyncio.create_task(self._fire_job(job))
            await asyncio.sleep(1)

    async def _fire_job(self, job: ScheduledJob) -> None:
        """Execute *job* callback and reschedule or remove it."""
        job.last_run = time.time()
        job.run_count += 1
        try:
            await job.callback()
            job.last_error = None
        except Exception as exc:
            job.last_error = str(exc)
            self.logger.error("Job '%s' failed: %s", job.name, exc)

        if job.max_runs is not None and job.run_count >= job.max_runs:
            self.logger.info("Job '%s' reached max_runs=%d, removing", job.name, job.max_runs)
            job.enabled = False
            self._jobs.pop(job.job_id, None)
        elif job.interval:
            job.next_run = time.time() + job.interval
        else:
            # One-shot job: remove after firing
            job.enabled = False
            self._jobs.pop(job.job_id, None)
