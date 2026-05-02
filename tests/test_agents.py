"""Tests for all specialist agents and the orchestrator."""

import asyncio

import pytest

from src.agents.content_agent import ContentAgent
from src.agents.data_agent import DataAgent
from src.agents.monitor_agent import MonitorAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.scheduler_agent import SchedulerAgent
from src.core.event_system import EventSystem
from src.core.message_bus import MessageBus
from src.core.task_queue import Task, TaskPriority, TaskQueue, TaskStatus


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def infra():
    """Return a fresh set of shared infrastructure components."""
    return MessageBus(), TaskQueue(), EventSystem()


def _make_content_agent(infra):
    bus, queue, es = infra
    return ContentAgent(bus, queue, es, config={"output_dir": "/tmp/test_content"})


def _make_data_agent(infra):
    bus, queue, es = infra
    return DataAgent(bus, queue, es, config={"report_dir": "/tmp/test_reports"})


def _make_scheduler_agent(infra):
    bus, queue, es = infra
    return SchedulerAgent(bus, queue, es, config={"max_retries": 1, "retry_delay": 1})


def _make_monitor_agent(infra):
    bus, queue, es = infra
    return MonitorAgent(
        bus, queue, es,
        config={"check_interval": 60, "alert_threshold": {"cpu_percent": 100, "memory_percent": 100}},
    )


# ── ContentAgent ──────────────────────────────────────────────────────────────

class TestContentAgent:
    @pytest.mark.asyncio
    async def test_generate(self, infra):
        agent = _make_content_agent(infra)
        task = Task(
            name="gen",
            agent_type="content_agent",
            payload={
                "operation": "generate",
                "title": "Test Title",
                "prompt": "Test prompt",
                "content_type": "article",
            },
        )
        result = await agent.process_task(task)
        assert result["title"] == "Test Title"
        assert result["status"] == "draft"
        assert "content_id" in result

    @pytest.mark.asyncio
    async def test_edit(self, infra):
        agent = _make_content_agent(infra)
        # First generate
        gen_task = Task(name="g", agent_type="content_agent", payload={
            "operation": "generate", "title": "Original", "prompt": "p", "content_type": "post"
        })
        gen_result = await agent.process_task(gen_task)
        cid = gen_result["content_id"]

        # Then edit
        edit_task = Task(name="e", agent_type="content_agent", payload={
            "operation": "edit", "content_id": cid, "updates": {"title": "Updated"}
        })
        edit_result = await agent.process_task(edit_task)
        assert edit_result["title"] == "Updated"

    @pytest.mark.asyncio
    async def test_publish(self, infra):
        agent = _make_content_agent(infra)
        gen = Task(name="g", agent_type="content_agent", payload={
            "operation": "generate", "title": "T", "prompt": "p", "content_type": "article"
        })
        gen_result = await agent.process_task(gen)
        cid = gen_result["content_id"]

        pub = Task(name="p", agent_type="content_agent", payload={
            "operation": "publish", "content_id": cid
        })
        pub_result = await agent.process_task(pub)
        assert pub_result["status"] == "published"

    @pytest.mark.asyncio
    async def test_summarise(self, infra):
        agent = _make_content_agent(infra)
        task = Task(name="s", agent_type="content_agent", payload={
            "operation": "summarise",
            "text": "这是第一句。这是第二句。这是第三句。这是第四句。",
            "max_sentences": 2,
        })
        result = await agent.process_task(task)
        assert "summary" in result
        assert result["original_length"] > 0

    @pytest.mark.asyncio
    async def test_unknown_operation(self, infra):
        agent = _make_content_agent(infra)
        task = Task(name="x", agent_type="content_agent", payload={"operation": "nonexistent"})
        with pytest.raises(ValueError, match="Unknown content operation"):
            await agent.process_task(task)


# ── DataAgent ─────────────────────────────────────────────────────────────────

class TestDataAgent:
    @pytest.mark.asyncio
    async def test_collect(self, infra):
        agent = _make_data_agent(infra)
        task = Task(name="c", agent_type="data_agent", payload={
            "operation": "collect",
            "dataset": "ds1",
            "records": [{"val": i} for i in range(5)],
        })
        result = await agent.process_task(task)
        assert result["added"] == 5
        assert result["total"] == 5

    @pytest.mark.asyncio
    async def test_analyse(self, infra):
        agent = _make_data_agent(infra)
        # collect first
        await agent.process_task(Task(name="c", agent_type="data_agent", payload={
            "operation": "collect", "dataset": "ds2",
            "records": [{"score": v} for v in [10, 20, 30]],
        }))
        result = await agent.process_task(Task(name="a", agent_type="data_agent", payload={
            "operation": "analyse", "dataset": "ds2", "field": "score",
        }))
        assert result["count"] == 3
        assert result["mean"] == 20.0
        assert result["min"] == 10
        assert result["max"] == 30

    @pytest.mark.asyncio
    async def test_query_with_filter(self, infra):
        agent = _make_data_agent(infra)
        await agent.process_task(Task(name="c", agent_type="data_agent", payload={
            "operation": "collect", "dataset": "ds3",
            "records": [{"region": "A"}, {"region": "B"}, {"region": "A"}],
        }))
        result = await agent.process_task(Task(name="q", agent_type="data_agent", payload={
            "operation": "query", "dataset": "ds3", "filters": {"region": "A"},
        }))
        assert result["count"] == 2

    @pytest.mark.asyncio
    async def test_report_creates_file(self, infra, tmp_path):
        bus, queue, es = infra
        agent = DataAgent(bus, queue, es, config={"report_dir": str(tmp_path)})
        await agent.process_task(Task(name="c", agent_type="data_agent", payload={
            "operation": "collect", "dataset": "rpt",
            "records": [{"kpi": v} for v in [5, 10, 15]],
        }))
        result = await agent.process_task(Task(name="r", agent_type="data_agent", payload={
            "operation": "report", "dataset": "rpt", "title": "Test Report",
        }))
        import os
        assert os.path.exists(result["file"])

    @pytest.mark.asyncio
    async def test_analyse_empty_dataset(self, infra):
        agent = _make_data_agent(infra)
        result = await agent.process_task(Task(name="a", agent_type="data_agent", payload={
            "operation": "analyse", "dataset": "empty_ds", "field": "x",
        }))
        assert result["count"] == 0
        assert "error" in result


# ── SchedulerAgent ────────────────────────────────────────────────────────────

class TestSchedulerAgent:
    @pytest.mark.asyncio
    async def test_schedule_and_list(self, infra):
        agent = _make_scheduler_agent(infra)
        await agent.start()

        task = Task(name="sched", agent_type="scheduler_agent", payload={
            "operation": "schedule",
            "job_name": "nightly_report",
            "target_agent": "data_agent",
            "task_name": "auto_report",
            "task_payload": {"operation": "report", "dataset": "x"},
            "interval": 3600,
            "delay": 3600,
        })
        sched_result = await agent.process_task(task)
        assert "job_id" in sched_result

        list_result = await agent.process_task(Task(name="ls", agent_type="scheduler_agent", payload={
            "operation": "list_jobs",
        }))
        assert list_result["count"] == 1

        await agent.stop()

    @pytest.mark.asyncio
    async def test_cancel_job(self, infra):
        agent = _make_scheduler_agent(infra)
        await agent.start()

        sched_result = await agent.process_task(Task(name="s", agent_type="scheduler_agent", payload={
            "operation": "schedule",
            "job_name": "job_to_cancel",
            "target_agent": "data_agent",
            "task_name": "t",
            "interval": 3600,
            "delay": 3600,
        }))
        job_id = sched_result["job_id"]

        cancel_result = await agent.process_task(Task(name="c", agent_type="scheduler_agent", payload={
            "operation": "cancel", "job_id": job_id,
        }))
        assert cancel_result["status"] == "cancelled"

        list_result = await agent.process_task(Task(name="ls", agent_type="scheduler_agent", payload={
            "operation": "list_jobs",
        }))
        assert list_result["count"] == 0

        await agent.stop()


# ── MonitorAgent ──────────────────────────────────────────────────────────────

class TestMonitorAgent:
    @pytest.mark.asyncio
    async def test_check(self, infra):
        agent = _make_monitor_agent(infra)
        task = Task(name="chk", agent_type="monitor_agent", payload={"operation": "check"})
        result = await agent.process_task(task)
        assert "metrics" in result
        assert "cpu_percent" in result["metrics"]
        assert "memory_percent" in result["metrics"]

    @pytest.mark.asyncio
    async def test_set_threshold_and_alert(self, infra):
        agent = _make_monitor_agent(infra)
        # Set threshold to 0 so any CPU value triggers an alert
        await agent.process_task(Task(name="st", agent_type="monitor_agent", payload={
            "operation": "set_threshold", "metric": "cpu_percent", "value": 0.0,
        }))
        result = await agent.process_task(Task(name="chk", agent_type="monitor_agent", payload={
            "operation": "check",
        }))
        # cpu_percent >= 0 should always trigger
        assert len(result["alerts"]) >= 1

    @pytest.mark.asyncio
    async def test_get_metrics_empty(self, infra):
        agent = _make_monitor_agent(infra)
        result = await agent.process_task(Task(name="gm", agent_type="monitor_agent", payload={
            "operation": "get_metrics",
        }))
        # Before any checks, metrics dict is empty
        assert isinstance(result, dict)


# ── OrchestratorAgent ─────────────────────────────────────────────────────────

class TestOrchestratorAgent:
    @pytest.mark.asyncio
    async def test_submit_and_dispatch(self, infra):
        bus, queue, es = infra
        content_agent = ContentAgent(bus, queue, es, config={"output_dir": "/tmp/test_orch_content"})
        orch = OrchestratorAgent(
            bus, queue, es,
            config={"max_concurrent_tasks": 5},
            agents={"content_agent": content_agent},
        )
        await orch.start()
        dispatch_task = asyncio.create_task(orch.run())

        task = await orch.submit_task(
            name="orch_gen",
            agent_type="content_agent",
            payload={
                "operation": "generate",
                "title": "Orch Test",
                "prompt": "test",
                "content_type": "post",
            },
        )
        # Poll until task reaches a terminal state (up to 5 s)
        for _ in range(50):
            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                break
            await asyncio.sleep(0.1)

        dispatch_task.cancel()
        await orch.stop()

        assert task.status == TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_unknown_agent_type(self, infra):
        bus, queue, es = infra
        orch = OrchestratorAgent(bus, queue, es, config={}, agents={})
        await orch.start()
        dispatch_task = asyncio.create_task(orch.run())

        task = await orch.submit_task(
            name="bad_task", agent_type="nonexistent_agent", payload={}
        )
        await asyncio.sleep(0.5)
        dispatch_task.cancel()
        await orch.stop()
        assert task.status == TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_get_agent_status(self, infra):
        bus, queue, es = infra
        content_agent = ContentAgent(bus, queue, es, config={"output_dir": "/tmp/tc"})
        orch = OrchestratorAgent(bus, queue, es, agents={"content_agent": content_agent})
        await orch.start()
        status = orch.get_agent_status()
        assert "content_agent" in status
        assert status["content_agent"]["running"] is True
        await orch.stop()
