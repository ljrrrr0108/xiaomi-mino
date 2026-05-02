"""Tests for core infrastructure: MessageBus, TaskQueue, EventSystem."""

import asyncio

import pytest

from src.core.event_system import Event, EventSystem, EventType
from src.core.message_bus import Message, MessageBus, MessageType
from src.core.task_queue import Task, TaskPriority, TaskQueue, TaskStatus


# ── MessageBus ────────────────────────────────────────────────────────────────

class TestMessageBus:
    def setup_method(self):
        self.bus = MessageBus(max_queue_size=100, message_ttl=60)

    @pytest.mark.asyncio
    async def test_subscribe_and_publish(self):
        received = []

        async def handler(msg: Message):
            received.append(msg)

        self.bus.subscribe("agent_a", handler)
        msg = Message(msg_type=MessageType.TASK_REQUEST, sender="agent_b", recipient="agent_a", payload={"x": 1})
        await self.bus.publish(msg)

        assert len(received) == 1
        assert received[0].payload == {"x": 1}

    @pytest.mark.asyncio
    async def test_broadcast(self):
        received = []

        async def handler(msg: Message):
            received.append(msg)

        self.bus.subscribe("broadcast", handler)
        msg = Message(msg_type=MessageType.HEARTBEAT, sender="agent_x", recipient="broadcast")
        await self.bus.publish(msg)

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        received = []

        async def handler(msg: Message):
            received.append(msg)

        self.bus.subscribe("agent_z", handler)
        self.bus.unsubscribe("agent_z", handler)

        msg = Message(msg_type=MessageType.TASK_RESULT, sender="x", recipient="agent_z")
        await self.bus.publish(msg)

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_message_history(self):
        async def noop(msg):
            pass

        self.bus.subscribe("agent_h", noop)
        for i in range(5):
            m = Message(msg_type=MessageType.STATUS_UPDATE, sender="s", recipient="agent_h", payload=i)
            await self.bus.publish(m)

        history = self.bus.get_history("agent_h")
        assert len(history) == 5

    def test_subscriber_count(self):
        def h1(_): pass
        def h2(_): pass
        self.bus.subscribe("t1", h1)
        self.bus.subscribe("t2", h2)
        assert self.bus.subscriber_count == 2


# ── TaskQueue ─────────────────────────────────────────────────────────────────

class TestTaskQueue:
    def setup_method(self):
        self.q = TaskQueue()

    @pytest.mark.asyncio
    async def test_put_and_get(self):
        t = Task(name="test_task", agent_type="data_agent")
        await self.q.put(t)
        out = await self.q.get()
        assert out.name == "test_task"
        assert out.status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_priority_ordering(self):
        low = Task(name="low", agent_type="x", priority=TaskPriority.LOW)
        high = Task(name="high", agent_type="x", priority=TaskPriority.HIGH)
        critical = Task(name="critical", agent_type="x", priority=TaskPriority.CRITICAL)

        await self.q.put(low)
        await self.q.put(high)
        await self.q.put(critical)

        first = await self.q.get()
        second = await self.q.get()
        third = await self.q.get()

        assert first.name == "critical"
        assert second.name == "high"
        assert third.name == "low"

    @pytest.mark.asyncio
    async def test_size_and_empty(self):
        assert self.q.empty
        assert self.q.size == 0
        t = Task(name="t")
        await self.q.put(t)
        assert not self.q.empty
        assert self.q.size == 1

    @pytest.mark.asyncio
    async def test_get_task_by_id(self):
        t = Task(name="lookup_me")
        await self.q.put(t)
        found = self.q.get_task(t.task_id)
        assert found is not None
        assert found.name == "lookup_me"

    @pytest.mark.asyncio
    async def test_get_all_tasks_filter(self):
        t1 = Task(name="t1")
        t2 = Task(name="t2")
        await self.q.put(t1)
        await self.q.put(t2)
        pending = self.q.get_all_tasks(status=TaskStatus.PENDING)
        assert len(pending) == 2


# ── EventSystem ───────────────────────────────────────────────────────────────

class TestEventSystem:
    def setup_method(self):
        self.es = EventSystem()

    @pytest.mark.asyncio
    async def test_emit_and_receive(self):
        received = []

        async def handler(e: Event):
            received.append(e)

        self.es.on(EventType.TASK_COMPLETED, handler)
        evt = Event(event_type=EventType.TASK_COMPLETED, source="agent_x", data={"ok": True})
        await self.es.emit(evt)

        assert len(received) == 1
        assert received[0].data == {"ok": True}

    @pytest.mark.asyncio
    async def test_wildcard_listener(self):
        received = []

        async def catch_all(e: Event):
            received.append(e)

        self.es.on("*", catch_all)
        await self.es.emit(Event(event_type=EventType.AGENT_STARTED, source="s"))
        await self.es.emit(Event(event_type=EventType.AGENT_STOPPED, source="s"))

        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_off(self):
        received = []

        async def handler(e: Event):
            received.append(e)

        self.es.on(EventType.TASK_FAILED, handler)
        self.es.off(EventType.TASK_FAILED, handler)
        await self.es.emit(Event(event_type=EventType.TASK_FAILED, source="s"))

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_event_log(self):
        await self.es.emit(Event(event_type=EventType.SYSTEM_READY, source="main"))
        await self.es.emit(Event(event_type=EventType.TASK_CREATED, source="orch"))
        log = self.es.get_event_log()
        assert len(log) == 2
        filtered = self.es.get_event_log(event_type=EventType.SYSTEM_READY)
        assert len(filtered) == 1
