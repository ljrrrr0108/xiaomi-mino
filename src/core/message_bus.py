"""
Message Bus — asynchronous publish/subscribe communication channel
used by agents to exchange messages without tight coupling.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("MessageBus")


class MessageType(str, Enum):
    """Predefined message type constants."""

    TASK_REQUEST = "task_request"
    TASK_RESULT = "task_result"
    TASK_ERROR = "task_error"
    STATUS_UPDATE = "status_update"
    ALERT = "alert"
    HEARTBEAT = "heartbeat"
    COMMAND = "command"
    RESPONSE = "response"


@dataclass
class Message:
    """A single message exchanged between agents.

    Attributes:
        msg_type: Semantic type of the message (see :class:`MessageType`).
        sender: Name/ID of the sending agent.
        recipient: Name/ID of the target agent, or ``"broadcast"`` to fan-out.
        payload: Arbitrary data carried by the message.
        msg_id: Unique message identifier (auto-generated).
        timestamp: Unix timestamp of creation (auto-set).
        correlation_id: Optional ID linking request/response pairs.
    """

    msg_type: MessageType
    sender: str
    recipient: str
    payload: Any = None
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    correlation_id: Optional[str] = None


class MessageBus:
    """Central publish/subscribe message broker for inter-agent communication.

    Agents register topic subscriptions.  When a message is published to a
    topic, all registered handlers for that topic are called.  Direct
    (point-to-point) delivery uses the recipient name as the topic.

    Usage::

        bus = MessageBus()
        bus.subscribe("agent_a", handler_fn)
        await bus.publish(Message(msg_type=..., sender="agent_b", recipient="agent_a", ...))
    """

    def __init__(self, max_queue_size: int = 1000, message_ttl: float = 3600.0) -> None:
        self._subscribers: Dict[str, List[Callable]] = {}
        self._message_history: List[Message] = []
        self._max_queue_size = max_queue_size
        self._message_ttl = message_ttl
        self._lock = asyncio.Lock()

    def subscribe(self, topic: str, handler: Callable[[Message], Any]) -> None:
        """Register *handler* to receive messages published to *topic*.

        Args:
            topic: The topic (usually a recipient name or ``"broadcast"``).
            handler: Async or sync callable that accepts a :class:`Message`.
        """
        self._subscribers.setdefault(topic, [])
        if handler not in self._subscribers[topic]:
            self._subscribers[topic].append(handler)
            logger.debug("Subscribed handler to topic '%s'", topic)

    def unsubscribe(self, topic: str, handler: Callable[[Message], Any]) -> None:
        """Remove *handler* from *topic* subscriptions."""
        if topic in self._subscribers:
            try:
                self._subscribers[topic].remove(handler)
            except ValueError:
                pass

    async def publish(self, message: Message) -> None:
        """Publish *message* to its recipient topic (and to ``"broadcast"``).

        Handlers are invoked concurrently via :func:`asyncio.gather`.

        Args:
            message: The message to deliver.
        """
        async with self._lock:
            # Prune expired messages from history
            now = time.time()
            self._message_history = [
                m for m in self._message_history if now - m.timestamp < self._message_ttl
            ]
            if len(self._message_history) < self._max_queue_size:
                self._message_history.append(message)

        handlers: List[Callable] = []
        # Deliver to specific recipient
        handlers.extend(self._subscribers.get(message.recipient, []))
        # Deliver to broadcast subscribers (but avoid duplicates)
        for h in self._subscribers.get("broadcast", []):
            if h not in handlers:
                handlers.append(h)

        if not handlers:
            logger.debug(
                "No subscribers for topic '%s' (msg_id=%s)", message.recipient, message.msg_id
            )
            return

        tasks = []
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                tasks.append(handler(message))
            else:
                tasks.append(asyncio.to_thread(handler, message))

        await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug(
            "Published %s from '%s' to '%s'",
            message.msg_type,
            message.sender,
            message.recipient,
        )

    def get_history(self, topic: Optional[str] = None) -> List[Message]:
        """Return message history, optionally filtered by recipient *topic*."""
        if topic:
            return [m for m in self._message_history if m.recipient == topic]
        return list(self._message_history)

    @property
    def subscriber_count(self) -> int:
        """Total number of registered handler entries."""
        return sum(len(v) for v in self._subscribers.values())
