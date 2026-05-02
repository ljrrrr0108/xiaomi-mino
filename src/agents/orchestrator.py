"""
Orchestrator — central coordinator that dispatches tasks to specialist agents
and tracks the overall system state.
"""

import asyncio
from typing import Any, Dict, List, Optional

from src.agents.base_agent import BaseAgent
from src.core.event_system import Event, EventSystem, EventType
from src.core.message_bus import Message, MessageBus, MessageType
from src.core.task_queue import Task, TaskPriority, TaskQueue, TaskStatus
from src.utils.logger import get_logger


class OrchestratorAgent(BaseAgent):
    """Central orchestrator responsible for:

    * Accepting task submissions from external callers or agents.
    * Routing tasks to the appropriate specialist agent.
    * Tracking task state and collecting results.
    * Retrying failed tasks (delegated to :class:`~src.agents.base_agent.BaseAgent`).

    Args:
        message_bus: Shared message bus.
        task_queue: Shared task queue.
        event_system: Shared event system.
        config: Orchestrator-specific configuration (``config["agents"]["orchestrator"]``).
        agents: Mapping of ``agent_type`` → :class:`BaseAgent` instances to coordinate.
    """

    def __init__(
        self,
        message_bus: MessageBus,
        task_queue: TaskQueue,
        event_system: EventSystem,
        config: Optional[Dict[str, Any]] = None,
        agents: Optional[Dict[str, BaseAgent]] = None,
    ) -> None:
        super().__init__(
            name="总调度Agent",
            agent_type="orchestrator",
            message_bus=message_bus,
            task_queue=task_queue,
            event_system=event_system,
            config=config or {},
        )
        self._agents: Dict[str, BaseAgent] = agents or {}
        self._max_concurrent = self.config.get("max_concurrent_tasks", 10)
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._pending_results: Dict[str, asyncio.Future] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the orchestrator and all managed agents."""
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        await super().start()
        for agent in self._agents.values():
            await agent.start()
        # Listen for task results from agents
        self.message_bus.subscribe("orchestrator", self._handle_message)
        await self.event_system.emit(
            Event(event_type=EventType.SYSTEM_READY, source=self.name, data={})
        )
        self.logger.info("Orchestrator ready — managing %d agents", len(self._agents))

    async def stop(self) -> None:
        """Stop the orchestrator and all managed agents."""
        for agent in self._agents.values():
            await agent.stop()
        await self.event_system.emit(
            Event(event_type=EventType.SYSTEM_SHUTDOWN, source=self.name, data={})
        )
        await super().stop()
        self.logger.info("Orchestrator stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_agent(self, agent: BaseAgent) -> None:
        """Register a specialist agent with the orchestrator.

        Args:
            agent: The agent to register.
        """
        self._agents[agent.agent_type] = agent
        self.logger.info("Registered agent '%s' (%s)", agent.name, agent.agent_type)

    async def submit_task(
        self,
        name: str,
        agent_type: str,
        payload: Any = None,
        priority: int = TaskPriority.NORMAL,
        max_retries: int = 3,
        timeout: Optional[float] = None,
    ) -> Task:
        """Create and queue a new task.

        Args:
            name: Human-readable task name.
            agent_type: Target agent type key.
            payload: Data passed to the agent's :meth:`process_task`.
            priority: Scheduling priority.
            max_retries: Maximum retry attempts on failure.
            timeout: Optional execution timeout in seconds.

        Returns:
            The created :class:`~src.core.task_queue.Task` object.
        """
        task = Task(
            name=name,
            agent_type=agent_type,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
            timeout=timeout,
        )
        await self.task_queue.put(task)
        await self.event_system.emit(
            Event(
                event_type=EventType.TASK_CREATED,
                source=self.name,
                data={"task_id": task.task_id, "task_name": name, "agent_type": agent_type},
            )
        )
        self.logger.info("Submitted task '%s' → agent '%s'", name, agent_type)
        return task

    async def run(self) -> None:
        """Main dispatch loop — dequeues tasks and routes them to agents.

        This coroutine runs until the orchestrator is stopped.
        """
        self.logger.info("Dispatch loop started")
        while self._running:
            if self.task_queue.empty:
                await asyncio.sleep(0.1)
                continue
            task = await self.task_queue.get()
            agent = self._agents.get(task.agent_type)
            if agent is None:
                self.logger.error(
                    "No agent registered for type '%s' (task '%s')",
                    task.agent_type,
                    task.name,
                )
                task.status = TaskStatus.FAILED
                task.error = f"Unknown agent type: {task.agent_type}"
                self.task_queue.task_done()
                continue

            # Dispatch with concurrency control
            asyncio.create_task(self._dispatch(agent, task))

    async def _dispatch(self, agent: BaseAgent, task: Task) -> None:
        """Execute *task* on *agent* under the concurrency semaphore."""
        async with self._semaphore:
            await agent.execute_task(task)
            self.task_queue.task_done()
            # Notify waiting callers
            future = self._pending_results.pop(task.task_id, None)
            if future and not future.done():
                future.set_result(task)

    async def submit_and_wait(
        self,
        name: str,
        agent_type: str,
        payload: Any = None,
        priority: int = TaskPriority.NORMAL,
        timeout: Optional[float] = None,
    ) -> Task:
        """Submit a task and block until it is completed (or fails).

        Args:
            name: Task name.
            agent_type: Target agent type.
            payload: Task payload.
            priority: Scheduling priority.
            timeout: Maximum wait time in seconds.

        Returns:
            The completed :class:`~src.core.task_queue.Task`.
        """
        task = await self.submit_task(name, agent_type, payload, priority, timeout=timeout)
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_results[task.task_id] = future

        if timeout:
            await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        else:
            await future
        return task

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def get_agent_status(self) -> Dict[str, Any]:
        """Return a status summary for all registered agents."""
        return {
            agent_type: {
                "name": agent.name,
                "running": agent.is_running,
                "stats": agent.get_stats(),
            }
            for agent_type, agent in self._agents.items()
        }

    def get_task_summary(self) -> Dict[str, List[Task]]:
        """Return tasks grouped by status."""
        statuses = [
            TaskStatus.PENDING,
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.RETRYING,
        ]
        return {s: self.task_queue.get_all_tasks(status=s) for s in statuses}

    # ------------------------------------------------------------------
    # BaseAgent abstract implementation
    # ------------------------------------------------------------------

    async def process_task(self, task: Task) -> Any:
        """The orchestrator itself does not process domain tasks."""
        self.logger.warning(
            "Orchestrator received a direct task '%s' — forwarding not implemented",
            task.name,
        )
        return {"status": "forwarded", "task_id": task.task_id}
