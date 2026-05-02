"""
Monitor Agent — collects system metrics and raises alerts when thresholds
are exceeded.

Supported operations (set in ``task.payload["operation"]``):

* ``check``         — run a one-shot health check
* ``get_metrics``   — return the latest collected metrics
* ``set_threshold`` — update an alert threshold at runtime
* ``get_alerts``    — return all triggered alert records
"""

import asyncio
import os
import time
from typing import Any, Dict, List, Optional

from src.agents.base_agent import BaseAgent
from src.core.event_system import Event, EventSystem, EventType
from src.core.message_bus import MessageBus, MessageType
from src.core.task_queue import Task, TaskQueue
from src.utils.logger import get_logger


def _get_cpu_percent() -> float:
    """Return approximate single-core CPU usage (%) without psutil.

    Uses ``/proc/stat`` on Linux; returns 0.0 on unsupported platforms.
    """
    try:
        with open("/proc/stat") as f:
            line = f.readline()
        fields = list(map(float, line.split()[1:]))
        idle = fields[3]
        total = sum(fields)
        return max(0.0, min(100.0, (total - idle) / total * 100.0)) if total else 0.0
    except Exception:
        return 0.0


def _get_memory_percent() -> float:
    """Return memory usage (%) using ``/proc/meminfo`` without psutil."""
    try:
        info: Dict[str, float] = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    info[parts[0].rstrip(":")] = float(parts[1])
        total = info.get("MemTotal", 0)
        available = info.get("MemAvailable", info.get("MemFree", 0))
        if total == 0:
            return 0.0
        return (1.0 - available / total) * 100.0
    except Exception:
        return 0.0


class MonitorAgent(BaseAgent):
    """Specialist agent for system health monitoring and alerting.

    Continuously samples system metrics and raises alerts when configurable
    thresholds are exceeded.

    Args:
        message_bus: Shared message bus.
        task_queue: Shared task queue.
        event_system: Shared event system.
        config: Monitor configuration (``config["agents"]["monitor_agent"]``).
    """

    def __init__(
        self,
        message_bus: MessageBus,
        task_queue: TaskQueue,
        event_system: EventSystem,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            name="系统监控Agent",
            agent_type="monitor_agent",
            message_bus=message_bus,
            task_queue=task_queue,
            event_system=event_system,
            config=config or {},
        )
        self._check_interval: float = float(self.config.get("check_interval", 30))
        default_thresholds = self.config.get("alert_threshold", {})
        self._thresholds: Dict[str, float] = {
            "cpu_percent": float(default_thresholds.get("cpu_percent", 80)),
            "memory_percent": float(default_thresholds.get("memory_percent", 85)),
        }
        self._latest_metrics: Dict[str, Any] = {}
        self._alert_log: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        asyncio.create_task(self._monitor_loop())
        self.logger.info("Monitor loop started (interval=%.0fs)", self._check_interval)

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    async def process_task(self, task: Task) -> Any:
        """Dispatch monitoring operations.

        Args:
            task: Work item with ``payload["operation"]``.
        """
        payload: Dict[str, Any] = task.payload or {}
        operation = payload.get("operation", "check")

        dispatch = {
            "check": self._check,
            "get_metrics": self._get_metrics,
            "set_threshold": self._set_threshold,
            "get_alerts": self._get_alerts,
        }
        handler = dispatch.get(operation)
        if handler is None:
            raise ValueError(f"Unknown monitor operation: '{operation}'")

        self.logger.info("Processing monitor operation '%s' (task=%s)", operation, task.name)
        return await handler(payload)

    # ------------------------------------------------------------------
    # Operation handlers
    # ------------------------------------------------------------------

    async def _check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Run a one-shot health check and return metrics + any alerts."""
        metrics = self._collect_metrics()
        alerts = self._evaluate_thresholds(metrics)
        return {"metrics": metrics, "alerts": alerts, "timestamp": time.time()}

    async def _get_metrics(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Return the most recently collected metrics snapshot."""
        return dict(self._latest_metrics)

    async def _set_threshold(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Update an alert threshold at runtime.

        Payload keys:
            metric (str): Metric name (e.g. ``cpu_percent``).
            value (float): New threshold value.
        """
        metric = payload.get("metric")
        value = payload.get("value")
        if metric is None or value is None:
            raise ValueError("Both 'metric' and 'value' must be provided")
        self._thresholds[metric] = float(value)
        self.logger.info("Updated threshold '%s' → %.2f", metric, value)
        return {"metric": metric, "threshold": float(value)}

    async def _get_alerts(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Return the alert history."""
        limit = payload.get("limit", 50)
        return {"alerts": self._alert_log[-limit:], "total": len(self._alert_log)}

    # ------------------------------------------------------------------
    # Monitor loop
    # ------------------------------------------------------------------

    async def _monitor_loop(self) -> None:
        """Periodically collect metrics and check thresholds."""
        while self._running:
            metrics = self._collect_metrics()
            self._latest_metrics = metrics
            alerts = self._evaluate_thresholds(metrics)
            if alerts:
                for alert in alerts:
                    await self.event_system.emit(
                        Event(
                            event_type=EventType.ALERT_TRIGGERED,
                            source=self.name,
                            data=alert,
                        )
                    )
                    await self.send_message(
                        "broadcast",
                        MessageType.ALERT,
                        payload=alert,
                    )
            await asyncio.sleep(self._check_interval)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _collect_metrics(self) -> Dict[str, Any]:
        """Sample current system metrics."""
        metrics: Dict[str, Any] = {
            "timestamp": time.time(),
            "cpu_percent": _get_cpu_percent(),
            "memory_percent": _get_memory_percent(),
        }
        # Disk usage of the current working directory
        try:
            stat = os.statvfs(".")
            total = stat.f_frsize * stat.f_blocks
            free = stat.f_frsize * stat.f_bfree
            metrics["disk_percent"] = (1.0 - free / total) * 100.0 if total else 0.0
        except Exception:
            metrics["disk_percent"] = 0.0
        return metrics

    def _evaluate_thresholds(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compare *metrics* against configured thresholds.

        Returns a list of alert dicts (empty if no thresholds exceeded).
        """
        alerts = []
        for metric, threshold in self._thresholds.items():
            value = metrics.get(metric)
            if value is not None and value >= threshold:
                alert = {
                    "metric": metric,
                    "value": value,
                    "threshold": threshold,
                    "timestamp": time.time(),
                    "severity": "critical" if value >= threshold * 1.1 else "warning",
                }
                self._alert_log.append(alert)
                self.logger.warning(
                    "ALERT: %s=%.1f%% exceeds threshold %.1f%%", metric, value, threshold
                )
                alerts.append(alert)
        return alerts

    def get_latest_metrics(self) -> Dict[str, Any]:
        """Return the latest metrics snapshot (non-async helper)."""
        return dict(self._latest_metrics)
