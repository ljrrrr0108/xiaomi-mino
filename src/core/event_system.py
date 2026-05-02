"""
Event System — lightweight publish/subscribe mechanism for system-wide events.

Unlike the :class:`~src.core.message_bus.MessageBus` (which routes
structured inter-agent messages), the event system is used for broader
notifications (e.g. agent started/stopped, task completed, alert triggered).
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("EventSystem")


@dataclass
class Event:
    """A system-wide notification.

    Attributes:
        event_type: String identifier for the event category.
        source: Name of the component that emitted the event.
        data: Arbitrary event payload.
        timestamp: Unix creation timestamp.
    """

    event_type: str
    source: str
    data: Any = None
    timestamp: float = field(default_factory=time.time)


# Common event type constants
class EventType:
    AGENT_STARTED = "agent.started"
    AGENT_STOPPED = "agent.stopped"
    TASK_CREATED = "task.created"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_RETRYING = "task.retrying"
    ALERT_TRIGGERED = "alert.triggered"
    SYSTEM_READY = "system.ready"
    SYSTEM_SHUTDOWN = "system.shutdown"


class EventSystem:
    """Global publish/subscribe event dispatcher.

    Listeners register for specific event types (or the wildcard ``"*"`` to
    receive every event).  When :meth:`emit` is called, all matching listeners
    are invoked concurrently.

    Usage::

        es = EventSystem()

        async def on_task_done(event: Event):
            print(f"Task done: {event.data}")

        es.on(EventType.TASK_COMPLETED, on_task_done)
        await es.emit(Event(event_type=EventType.TASK_COMPLETED, source="data_agent", data={...}))
    """

    def __init__(self) -> None:
        self._listeners: Dict[str, List[Callable]] = {}
        self._event_log: List[Event] = []

    def on(self, event_type: str, listener: Callable[[Event], Any]) -> None:
        """Register *listener* for *event_type* (or ``"*"`` for all events).

        Args:
            event_type: Event category string or ``"*"``.
            listener: Async or sync callable accepting an :class:`Event`.
        """
        self._listeners.setdefault(event_type, [])
        if listener not in self._listeners[event_type]:
            self._listeners[event_type].append(listener)

    def off(self, event_type: str, listener: Callable[[Event], Any]) -> None:
        """Unregister *listener* from *event_type*."""
        if event_type in self._listeners:
            try:
                self._listeners[event_type].remove(listener)
            except ValueError:
                pass

    async def emit(self, event: Event) -> None:
        """Dispatch *event* to all registered listeners.

        Args:
            event: The event to dispatch.
        """
        self._event_log.append(event)
        listeners: List[Callable] = []
        listeners.extend(self._listeners.get(event.event_type, []))
        for lst in self._listeners.get("*", []):
            if lst not in listeners:
                listeners.append(lst)

        if not listeners:
            return

        tasks = []
        for listener in listeners:
            if asyncio.iscoroutinefunction(listener):
                tasks.append(listener(event))
            else:
                tasks.append(asyncio.to_thread(listener, event))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                logger.error("Event listener error for '%s': %s", event.event_type, res)

    def get_event_log(self, event_type: Optional[str] = None) -> List[Event]:
        """Return logged events, optionally filtered by *event_type*."""
        if event_type:
            return [e for e in self._event_log if e.event_type == event_type]
        return list(self._event_log)
