"""
Data Agent — data collection, analysis, and report generation.

Supported operations (set in ``task.payload["operation"]``):

* ``collect``   — ingest raw data records into the in-memory store
* ``analyse``   — compute descriptive statistics on a named dataset
* ``report``    — generate a structured Markdown report
* ``query``     — filter/aggregate records from a dataset
"""

import json
import os
import statistics
import time
from typing import Any, Dict, List, Optional

from src.agents.base_agent import BaseAgent
from src.core.event_system import EventSystem
from src.core.message_bus import MessageBus
from src.core.task_queue import Task, TaskQueue
from src.utils.logger import get_logger


class DataAgent(BaseAgent):
    """Specialist agent for data analysis and reporting.

    Args:
        message_bus: Shared message bus.
        task_queue: Shared task queue.
        event_system: Shared event system.
        config: Data-agent configuration (``config["agents"]["data_agent"]``).
    """

    def __init__(
        self,
        message_bus: MessageBus,
        task_queue: TaskQueue,
        event_system: EventSystem,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            name="数据分析Agent",
            agent_type="data_agent",
            message_bus=message_bus,
            task_queue=task_queue,
            event_system=event_system,
            config=config or {},
        )
        self._report_dir: str = self.config.get("report_dir", "outputs/reports")
        os.makedirs(self._report_dir, exist_ok=True)
        # dataset_name → list of record dicts
        self._datasets: Dict[str, List[Dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    async def process_task(self, task: Task) -> Any:
        """Dispatch to the correct operation handler.

        Args:
            task: Work item with ``payload["operation"]`` set to one of
                ``collect``, ``analyse``, ``report``, or ``query``.

        Returns:
            Operation-specific result.
        """
        payload: Dict[str, Any] = task.payload or {}
        operation = payload.get("operation", "analyse")

        dispatch = {
            "collect": self._collect,
            "analyse": self._analyse,
            "report": self._report,
            "query": self._query,
        }
        handler = dispatch.get(operation)
        if handler is None:
            raise ValueError(f"Unknown data operation: '{operation}'")

        self.logger.info("Processing data operation '%s' (task=%s)", operation, task.name)
        return await handler(payload)

    # ------------------------------------------------------------------
    # Operation handlers
    # ------------------------------------------------------------------

    async def _collect(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest raw records into a named dataset.

        Payload keys:
            dataset (str): Dataset name.
            records (list[dict]): Records to append.
        """
        dataset = payload.get("dataset", "default")
        records: List[Dict[str, Any]] = payload.get("records", [])
        self._datasets.setdefault(dataset, [])
        for rec in records:
            rec["_ingested_at"] = time.time()
            self._datasets[dataset].append(rec)
        self.logger.info(
            "Collected %d records into dataset '%s' (total=%d)",
            len(records),
            dataset,
            len(self._datasets[dataset]),
        )
        return {"dataset": dataset, "added": len(records), "total": len(self._datasets[dataset])}

    async def _analyse(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Compute descriptive statistics for a numeric field in a dataset.

        Payload keys:
            dataset (str): Dataset name.
            field (str): Numeric field to analyse.
        """
        dataset = payload.get("dataset", "default")
        field = payload.get("field", "value")
        records = self._datasets.get(dataset, [])
        values = [r[field] for r in records if field in r and isinstance(r[field], (int, float))]

        if not values:
            return {"dataset": dataset, "field": field, "count": 0, "error": "no numeric data"}

        result = {
            "dataset": dataset,
            "field": field,
            "count": len(values),
            "sum": sum(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        }
        self.logger.info(
            "Analysed dataset '%s' field '%s': mean=%.2f", dataset, field, result["mean"]
        )
        return result

    async def _report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a Markdown report and save it to the report directory.

        Payload keys:
            dataset (str): Dataset name.
            title (str): Report title.
            fields (list[str]): Fields to include in statistics table.
        """
        dataset = payload.get("dataset", "default")
        title = payload.get("title", f"{dataset} 数据报告")
        fields: List[str] = payload.get("fields", [])
        records = self._datasets.get(dataset, [])

        # Auto-detect numeric fields if none specified
        if not fields and records:
            fields = [k for k, v in records[0].items() if isinstance(v, (int, float))]

        lines = [
            f"# {title}",
            f"\n生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"\n总记录数: {len(records)}",
            "\n---\n",
        ]

        stats_table: Dict[str, Dict[str, Any]] = {}
        for f in fields:
            values = [r[f] for r in records if f in r and isinstance(r[f], (int, float))]
            if values:
                stats_table[f] = {
                    "count": len(values),
                    "mean": round(statistics.mean(values), 2),
                    "min": min(values),
                    "max": max(values),
                }

        if stats_table:
            lines.append("## 统计摘要\n")
            lines.append("| 字段 | 数量 | 均值 | 最小值 | 最大值 |")
            lines.append("|------|------|------|--------|--------|")
            for fname, s in stats_table.items():
                lines.append(
                    f"| {fname} | {s['count']} | {s['mean']} | {s['min']} | {s['max']} |"
                )

        report_text = "\n".join(lines)
        filename = os.path.join(
            self._report_dir, f"report_{dataset}_{int(time.time())}.md"
        )
        with open(filename, "w", encoding="utf-8") as fh:
            fh.write(report_text)

        self.logger.info("Generated report for dataset '%s' → %s", dataset, filename)
        return {"file": filename, "title": title, "record_count": len(records)}

    async def _query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Filter records from a dataset.

        Payload keys:
            dataset (str): Dataset name.
            filters (dict): Field→value equality filters.
            limit (int): Maximum number of results (default 100).
        """
        dataset = payload.get("dataset", "default")
        filters: Dict[str, Any] = payload.get("filters", {})
        limit: int = payload.get("limit", 100)
        records = self._datasets.get(dataset, [])

        results = records
        for key, value in filters.items():
            results = [r for r in results if r.get(key) == value]
        results = results[:limit]

        self.logger.info(
            "Query on '%s' with filters=%s returned %d records", dataset, filters, len(results)
        )
        return {"dataset": dataset, "count": len(results), "records": results}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def list_datasets(self) -> Dict[str, int]:
        """Return dataset names and their record counts."""
        return {name: len(recs) for name, recs in self._datasets.items()}
