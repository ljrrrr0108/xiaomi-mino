# 小米 MINO — 多Agent协同运营自动化系统

> **Multi-Agent Collaborative Operations Automation System**

A production-ready Python framework in which multiple specialised AI agents collaborate to automate content operations, data analysis, task scheduling, and system monitoring — all orchestrated by a central coordinator.

---

## 架构概览 / Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    OrchestratorAgent                      │
│              (总调度Agent — central router)               │
└─────────┬──────────┬───────────────┬─────────────────────┘
          │          │               │               │
          ▼          ▼               ▼               ▼
  ContentAgent  DataAgent   SchedulerAgent   MonitorAgent
  (内容运营)   (数据分析)     (任务调度)      (系统监控)

          All agents communicate via:
          ┌──────────────┐  ┌───────────┐  ┌─────────────┐
          │  MessageBus  │  │ TaskQueue │  │ EventSystem │
          └──────────────┘  └───────────┘  └─────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **OrchestratorAgent** | Central coordinator — routes tasks, manages concurrency, collects results |
| **ContentAgent** | Content lifecycle: generate → edit → publish → summarise |
| **DataAgent** | Data pipeline: collect → analyse → report → query |
| **SchedulerAgent** | Recurring & one-shot job scheduling with retry support |
| **MonitorAgent** | System health checks, metric collection, threshold alerting |
| **MessageBus** | Async pub/sub channel for inter-agent messages |
| **TaskQueue** | Priority-based async work queue (CRITICAL → HIGH → NORMAL → LOW) |
| **EventSystem** | Lightweight system-wide event dispatcher |

---

## 快速开始 / Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the demo

```bash
python main.py
```

The demo will:
1. Collect and analyse sales data via the **DataAgent**
2. Generate an operations report
3. Create and publish a promotional article via the **ContentAgent**
4. Register a daily recurring report job via the **SchedulerAgent**
5. Run a system health check via the **MonitorAgent**
6. Print a summary of all agent activity

### 3. Run tests

```bash
pytest
```

---

## 项目结构 / Project Structure

```
xiaomi-mino/
├── main.py                    # Entry point & demo
├── requirements.txt
├── pytest.ini
├── config/
│   └── config.yaml            # System configuration
├── src/
│   ├── agents/
│   │   ├── base_agent.py      # Abstract agent base class
│   │   ├── orchestrator.py    # Central orchestrator
│   │   ├── content_agent.py   # Content operations
│   │   ├── data_agent.py      # Data analysis & reporting
│   │   ├── scheduler_agent.py # Task scheduling
│   │   └── monitor_agent.py   # System monitoring
│   ├── core/
│   │   ├── message_bus.py     # Async pub/sub message broker
│   │   ├── task_queue.py      # Priority task queue
│   │   └── event_system.py    # Event dispatcher
│   └── utils/
│       ├── logger.py          # Rotating file + console logger
│       └── config.py          # YAML config loader
└── tests/
    ├── test_core.py           # Core infrastructure tests
    └── test_agents.py         # Agent unit tests
```

---

## 配置 / Configuration

Edit `config/config.yaml` to tune the system:

```yaml
system:
  log_level: "INFO"          # DEBUG | INFO | WARNING | ERROR

agents:
  orchestrator:
    max_concurrent_tasks: 10

  content_agent:
    output_dir: "outputs/content"

  data_agent:
    report_dir: "outputs/reports"

  monitor_agent:
    check_interval: 30        # seconds
    alert_threshold:
      cpu_percent: 80
      memory_percent: 85
```

---

## 自定义扩展 / Extending the System

### Add a new agent

1. Subclass `BaseAgent` in `src/agents/`.
2. Implement `process_task(self, task: Task) -> Any`.
3. Register the new agent with the `OrchestratorAgent`.

```python
from src.agents.base_agent import BaseAgent
from src.core.task_queue import Task

class MyCustomAgent(BaseAgent):
    def __init__(self, message_bus, task_queue, event_system, config=None):
        super().__init__(
            name="自定义Agent",
            agent_type="custom_agent",
            message_bus=message_bus,
            task_queue=task_queue,
            event_system=event_system,
            config=config,
        )

    async def process_task(self, task: Task):
        # your logic here
        return {"result": "ok"}
```

### Submit tasks programmatically

```python
from main import build_system
from src.utils.config import load_config
import asyncio

config = load_config()
orchestrator = build_system(config)

async def run():
    await orchestrator.start()
    asyncio.create_task(orchestrator.run())

    task = await orchestrator.submit_task(
        name="我的任务",
        agent_type="content_agent",
        payload={"operation": "generate", "title": "Hello", "prompt": "World"},
    )
    await asyncio.sleep(2)
    print(task.status, task.result)
    await orchestrator.stop()

asyncio.run(run())
```

---

## License

MIT
