"""
小米 MINO 多Agent协同运营自动化系统
Main entry point.

Usage::

    python main.py                  # run the interactive demo
    python main.py --config /path/to/config.yaml
"""

import argparse
import asyncio
import sys

from src.agents.content_agent import ContentAgent
from src.agents.data_agent import DataAgent
from src.agents.monitor_agent import MonitorAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.scheduler_agent import SchedulerAgent
from src.core.event_system import EventSystem
from src.core.message_bus import MessageBus
from src.core.task_queue import Task, TaskPriority, TaskQueue
from src.utils.config import load_config
from src.utils.logger import get_logger


def build_system(config: dict) -> OrchestratorAgent:
    """Construct and wire up all components of the multi-agent system.

    Args:
        config: Parsed YAML configuration dictionary.

    Returns:
        A fully wired (but not yet started) :class:`OrchestratorAgent`.
    """
    # Shared infrastructure
    message_bus = MessageBus(
        max_queue_size=config.get("message_bus", {}).get("max_queue_size", 1000),
        message_ttl=config.get("message_bus", {}).get("message_ttl", 3600),
    )
    task_queue = TaskQueue()
    event_system = EventSystem()

    agents_cfg = config.get("agents", {})

    # Specialist agents
    content_agent = ContentAgent(
        message_bus=message_bus,
        task_queue=task_queue,
        event_system=event_system,
        config=agents_cfg.get("content_agent", {}),
    )
    data_agent = DataAgent(
        message_bus=message_bus,
        task_queue=task_queue,
        event_system=event_system,
        config=agents_cfg.get("data_agent", {}),
    )
    scheduler_agent = SchedulerAgent(
        message_bus=message_bus,
        task_queue=task_queue,
        event_system=event_system,
        config=agents_cfg.get("scheduler_agent", {}),
    )
    monitor_agent = MonitorAgent(
        message_bus=message_bus,
        task_queue=task_queue,
        event_system=event_system,
        config=agents_cfg.get("monitor_agent", {}),
    )

    # Orchestrator (coordinates all specialist agents)
    orchestrator = OrchestratorAgent(
        message_bus=message_bus,
        task_queue=task_queue,
        event_system=event_system,
        config=agents_cfg.get("orchestrator", {}),
        agents={
            content_agent.agent_type: content_agent,
            data_agent.agent_type: data_agent,
            scheduler_agent.agent_type: scheduler_agent,
            monitor_agent.agent_type: monitor_agent,
        },
    )
    return orchestrator


async def demo(orchestrator: OrchestratorAgent, logger) -> None:
    """Run a brief end-to-end demonstration of the multi-agent system."""
    logger.info("=" * 60)
    logger.info("  小米 MINO 多Agent协同运营自动化系统 — 演示开始")
    logger.info("=" * 60)

    await orchestrator.start()

    # Kick off the dispatch loop as a background task
    dispatch_task = asyncio.create_task(orchestrator.run())

    # ── 1. Data: collect + analyse ──────────────────────────────────
    logger.info("\n[1] 数据Agent: 收集销售数据")
    records = [{"product": f"P{i}", "sales": i * 150, "region": "北京"} for i in range(1, 11)]
    await orchestrator.submit_task(
        name="收集销售数据",
        agent_type="data_agent",
        payload={"operation": "collect", "dataset": "sales_q1", "records": records},
        priority=TaskPriority.HIGH,
    )

    logger.info("[2] 数据Agent: 分析销售字段")
    await orchestrator.submit_task(
        name="分析销售数据",
        agent_type="data_agent",
        payload={"operation": "analyse", "dataset": "sales_q1", "field": "sales"},
        priority=TaskPriority.HIGH,
    )

    logger.info("[3] 数据Agent: 生成报告")
    await orchestrator.submit_task(
        name="生成Q1报告",
        agent_type="data_agent",
        payload={
            "operation": "report",
            "dataset": "sales_q1",
            "title": "2024年Q1销售数据报告",
            "fields": ["sales"],
        },
    )

    # ── 2. Content: generate + publish ─────────────────────────────
    logger.info("[4] 内容Agent: 生成推广文章")
    gen_task = await orchestrator.submit_task(
        name="生成新品推广文章",
        agent_type="content_agent",
        payload={
            "operation": "generate",
            "title": "小米新品发布会亮点汇总",
            "prompt": "本次发布会带来了多款全新智能家居产品，性能出色，价格亲民。",
            "content_type": "article",
            "tags": ["小米", "新品", "智能家居"],
        },
    )

    # ── 3. Scheduler: register a recurring job ─────────────────────
    logger.info("[5] 调度Agent: 注册每日数据分析任务")
    await orchestrator.submit_task(
        name="注册每日报告任务",
        agent_type="scheduler_agent",
        payload={
            "operation": "schedule",
            "job_name": "每日销售报告",
            "target_agent": "data_agent",
            "task_name": "自动生成每日报告",
            "task_payload": {
                "operation": "report",
                "dataset": "sales_q1",
                "title": "每日销售自动报告",
            },
            "interval": 86400,  # 24 h
            "delay": 86400,
        },
    )

    # ── 4. Monitor: health check ────────────────────────────────────
    logger.info("[6] 监控Agent: 系统健康检查")
    await orchestrator.submit_task(
        name="系统健康检查",
        agent_type="monitor_agent",
        payload={"operation": "check"},
        priority=TaskPriority.LOW,
    )

    # Give agents time to drain the queue
    await asyncio.sleep(3)

    # ── Print summary ───────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("  Agent 状态汇总")
    logger.info("=" * 60)
    for agent_type, info in orchestrator.get_agent_status().items():
        stats = info["stats"]
        logger.info(
            "  %-20s  已处理: %d  失败: %d",
            info["name"],
            stats["tasks_processed"],
            stats["tasks_failed"],
        )

    task_summary = orchestrator.get_task_summary()
    logger.info(
        "\n  任务状态: 已完成=%d  失败=%d  等待=%d",
        len(task_summary["completed"]),
        len(task_summary["failed"]),
        len(task_summary["pending"]),
    )

    logger.info("=" * 60)
    logger.info("  演示结束")
    logger.info("=" * 60)

    dispatch_task.cancel()
    await orchestrator.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="小米 MINO 多Agent协同运营自动化系统"
    )
    parser.add_argument("--config", default=None, help="Path to config YAML file")
    args = parser.parse_args()

    log = get_logger("main")

    try:
        config = load_config(args.config)
    except FileNotFoundError as exc:
        log.error("Config error: %s", exc)
        sys.exit(1)

    # Apply global log level from config
    log_level = config.get("system", {}).get("log_level", "INFO")
    log = get_logger("main", log_level=log_level)

    orchestrator = build_system(config)

    try:
        asyncio.run(demo(orchestrator, log))
    except KeyboardInterrupt:
        log.info("Interrupted by user")


if __name__ == "__main__":
    main()
