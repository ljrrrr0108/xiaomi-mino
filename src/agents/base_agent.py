"""
Base Agent — abstract foundation for all agents in the system.

Every concrete agent inherits from :class:`BaseAgent` and implements the
:meth:`process_task` method.  The base class provides:

* Lifecycle management (start / stop)
* Heartbeat loop
* Message-bus integration
* Event emission helpers
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from src.core.event_system import Event, EventSystem, EventType
from src.core.message_bus import Message, MessageBus, MessageType
from src.core.task_queue import Task, TaskQueue, TaskStatus
from src.utils.logger import get_logger


class BaseAgent(ABC):
    """Abstract base class for all system agents.

    Subclasses **must** implement :meth:`process_task`.

    Args:
        name: Human-readable agent name (also used as the message-bus topic).
        agent_type: Short machine-readable type key (e.g. ``"content_agent"``).
        message_bus: Shared :class:`~src.core.message_bus.MessageBus` instance.
        task_queue: Shared :class:`~src.core.task_queue.TaskQueue` instance.
        event_system: Shared :class:`~src.core.event_system.EventSystem` instance.
        config: Agent-specific configuration dictionary.
    """

    def __init__(
        self,
        name: str,
        agent_type: str,
        message_bus: MessageBus,
        task_queue: TaskQueue,
        event_system: EventSystem,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = name
        self.agent_type = agent_type
        self.message_bus = message_bus
        self.task_queue = task_queue
        self.event_system = event_system
        self.config = config or {}
        self.logger = get_logger(self.name)
        self._running = False
        self._heartbeat_interval: float = 30.0
        self._stats: Dict[str, Any] = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "started_at": None,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the agent: register with the message bus and emit started event."""
        self._running = True
        self._stats["started_at"] = time.time()
        self.message_bus.subscribe(self.agent_type, self._handle_message)
        self.message_bus.subscribe("broadcast", self._handle_message)
        await self.event_system.emit(
            Event(event_type=EventType.AGENT_STARTED, source=self.name, data={"agent": self.name})
        )
        self.logger.info("Agent '%s' started", self.name)
        # Kick off the heartbeat coroutine
        asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Gracefully stop the agent."""
        self._running = False
        self.message_bus.unsubscribe(self.agent_type, self._handle_message)
        self.message_bus.unsubscribe("broadcast", self._handle_message)
        await self.event_system.emit(
            Event(event_type=EventType.AGENT_STOPPED, source=self.name, data={"agent": self.name})
        )
        self.logger.info("Agent '%s' stopped", self.name)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def process_task(self, task: Task) -> Any:
        """Execute the given *task* and return its result.

        Args:
            task: The work item to process.

        Returns:
            Task result (any JSON-serialisable value).
        """

    # ------------------------------------------------------------------
    # Task execution helper
    # ------------------------------------------------------------------

    async def execute_task(self, task: Task) -> None:
        """Wrap :meth:`process_task` with retry logic and event emission.

        Args:
            task: Task to execute (mutated in-place with status/result).
        """
        max_retries = task.max_retries
        for attempt in range(max_retries + 1):
            try:
                self.logger.info(
                    "Executing task '%s' (attempt %d/%d)", task.name, attempt + 1, max_retries + 1
                )
                task.status = TaskStatus.RUNNING

                if task.timeout:
                    result = await asyncio.wait_for(self.process_task(task), timeout=task.timeout)
                else:
                    result = await self.process_task(task)

                task.result = result
                task.status = TaskStatus.COMPLETED
                self._stats["tasks_processed"] += 1
                await self.event_system.emit(
                    Event(
                        event_type=EventType.TASK_COMPLETED,
                        source=self.name,
                        data={"task_id": task.task_id, "task_name": task.name},
                    )
                )
                self.logger.info("Task '%s' completed successfully", task.name)
                return

            except asyncio.TimeoutError:
                task.error = f"Task timed out after {task.timeout}s"
                self.logger.warning("Task '%s' timed out", task.name)
            except Exception as exc:
                task.error = str(exc)
                self.logger.error("Task '%s' failed: %s", task.name, exc)

            if attempt < max_retries:
                task.retries += 1
                task.status = TaskStatus.RETRYING
                await self.event_system.emit(
                    Event(
                        event_type=EventType.TASK_RETRYING,
                        source=self.name,
                        data={"task_id": task.task_id, "attempt": attempt + 1},
                    )
                )
                await asyncio.sleep(2 ** attempt)  # exponential back-off

        task.status = TaskStatus.FAILED
        self._stats["tasks_failed"] += 1
        await self.event_system.emit(
            Event(
                event_type=EventType.TASK_FAILED,
                source=self.name,
                data={"task_id": task.task_id, "error": task.error},
            )
        )

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    async def _handle_message(self, message: Message) -> None:
        """Route incoming messages to appropriate handlers.

        Override in subclasses for custom message handling.
        """
        if message.msg_type == MessageType.COMMAND:
            await self._handle_command(message)
        elif message.msg_type == MessageType.HEARTBEAT:
            pass  # silently ignore heartbeats from other agents
        else:
            self.logger.debug(
                "Received message type '%s' from '%s'", message.msg_type, message.sender
            )

    async def _handle_command(self, message: Message) -> None:
        """Handle a COMMAND message. Override for agent-specific commands."""
        self.logger.info(
            "Received command from '%s': %s", message.sender, message.payload
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def send_message(
        self,
        recipient: str,
        msg_type: MessageType,
        payload: Any = None,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Publish a message on the shared message bus.

        Args:
            recipient: Target agent type or ``"broadcast"``.
            msg_type: Semantic message type.
            payload: Message data.
            correlation_id: Optional request/response correlation token.
        """
        message = Message(
            msg_type=msg_type,
            sender=self.name,
            recipient=recipient,
            payload=payload,
            correlation_id=correlation_id,
        )
        await self.message_bus.publish(message)

    async def _heartbeat_loop(self) -> None:
        """Periodically broadcast a heartbeat message."""
        while self._running:
            await asyncio.sleep(self._heartbeat_interval)
            if self._running:
                await self.send_message(
                    "broadcast",
                    MessageType.HEARTBEAT,
                    payload={"agent": self.name, "stats": self._stats},
                )

    def get_stats(self) -> Dict[str, Any]:
        """Return a snapshot of the agent's runtime statistics."""
        return dict(self._stats)

    @property
    def is_running(self) -> bool:
        """True if the agent is currently active."""
        return self._running
