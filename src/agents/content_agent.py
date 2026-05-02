"""
Content Agent — handles content creation, editing, and publishing operations.

Supported operations (set in ``task.payload["operation"]``):

* ``generate``  — generate content from a prompt/template
* ``edit``      — apply edits to existing content
* ``publish``   — mark content as published and persist to output directory
* ``summarise`` — produce a short summary of the provided text
"""

import json
import os
import textwrap
import time
from typing import Any, Dict, Optional

from src.agents.base_agent import BaseAgent
from src.core.event_system import EventSystem
from src.core.message_bus import MessageBus
from src.core.task_queue import Task, TaskQueue
from src.utils.logger import get_logger


class ContentAgent(BaseAgent):
    """Specialist agent for content operations.

    Args:
        message_bus: Shared message bus.
        task_queue: Shared task queue.
        event_system: Shared event system.
        config: Content-agent configuration (``config["agents"]["content_agent"]``).
    """

    def __init__(
        self,
        message_bus: MessageBus,
        task_queue: TaskQueue,
        event_system: EventSystem,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            name="内容运营Agent",
            agent_type="content_agent",
            message_bus=message_bus,
            task_queue=task_queue,
            event_system=event_system,
            config=config or {},
        )
        self._output_dir: str = self.config.get("output_dir", "outputs/content")
        os.makedirs(self._output_dir, exist_ok=True)
        self._content_store: Dict[str, Dict[str, Any]] = {}  # content_id → metadata

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    async def process_task(self, task: Task) -> Any:
        """Dispatch to the correct operation handler.

        Expected ``task.payload`` keys:

        * ``operation`` (str): One of ``generate``, ``edit``, ``publish``,
          ``summarise``.
        * Additional keys are operation-specific (see helper methods).

        Args:
            task: Work item carrying operation details in its payload.

        Returns:
            Operation-specific result dictionary.
        """
        payload: Dict[str, Any] = task.payload or {}
        operation = payload.get("operation", "generate")

        dispatch = {
            "generate": self._generate,
            "edit": self._edit,
            "publish": self._publish,
            "summarise": self._summarise,
        }
        handler = dispatch.get(operation)
        if handler is None:
            raise ValueError(f"Unknown content operation: '{operation}'")

        self.logger.info("Processing content operation '%s' (task=%s)", operation, task.name)
        return await handler(payload)

    # ------------------------------------------------------------------
    # Operation handlers
    # ------------------------------------------------------------------

    async def _generate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Generate new content from a prompt/template.

        Payload keys:
            title (str): Content title.
            prompt (str): Brief description or keywords.
            content_type (str): E.g. ``article``, ``post``, ``report`` (default ``article``).
            tags (list[str]): Optional tags.
        """
        title = payload.get("title", "未命名内容")
        prompt = payload.get("prompt", "")
        content_type = payload.get("content_type", "article")
        tags = payload.get("tags", [])

        content_id = f"{content_type}_{int(time.time())}"
        body = self._compose_content(title, prompt, content_type)

        metadata = {
            "content_id": content_id,
            "title": title,
            "content_type": content_type,
            "tags": tags,
            "body": body,
            "status": "draft",
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        self._content_store[content_id] = metadata
        self.logger.info("Generated content '%s' (id=%s)", title, content_id)
        return metadata

    async def _edit(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Apply edits to existing content.

        Payload keys:
            content_id (str): ID of the content to edit.
            updates (dict): Key-value pairs to update (e.g. ``{"title": "…", "body": "…"}``).
        """
        content_id = payload.get("content_id")
        updates = payload.get("updates", {})

        if content_id not in self._content_store:
            raise KeyError(f"Content '{content_id}' not found")

        record = self._content_store[content_id]
        record.update(updates)
        record["updated_at"] = time.time()
        self.logger.info("Edited content id='%s'", content_id)
        return record

    async def _publish(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Mark content as published and write to the output directory.

        Payload keys:
            content_id (str): ID of the content to publish.
        """
        content_id = payload.get("content_id")
        if content_id not in self._content_store:
            raise KeyError(f"Content '{content_id}' not found")

        record = self._content_store[content_id]
        record["status"] = "published"
        record["published_at"] = time.time()

        # Persist to file
        filename = os.path.join(self._output_dir, f"{content_id}.json")
        with open(filename, "w", encoding="utf-8") as fh:
            json.dump(record, fh, ensure_ascii=False, indent=2)

        self.logger.info("Published content '%s' → %s", content_id, filename)
        return {"content_id": content_id, "status": "published", "file": filename}

    async def _summarise(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Produce a short summary of provided text.

        Payload keys:
            text (str): Source text to summarise.
            max_sentences (int): Maximum sentences in summary (default 3).
        """
        text: str = payload.get("text", "")
        max_sentences: int = payload.get("max_sentences", 3)

        sentences = [s.strip() for s in text.replace("。", "。\n").split("\n") if s.strip()]
        summary = " ".join(sentences[:max_sentences])
        summary = textwrap.shorten(summary, width=200, placeholder="…")
        self.logger.info("Summarised text (%d chars → %d chars)", len(text), len(summary))
        return {"summary": summary, "original_length": len(text), "summary_length": len(summary)}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _compose_content(self, title: str, prompt: str, content_type: str) -> str:
        """Build a simple structured content body.

        This is a rule-based stub.  In a production system this would call
        an LLM API.
        """
        templates = {
            "article": (
                f"# {title}\n\n"
                f"## 摘要\n{prompt}\n\n"
                "## 正文\n请根据以上提示撰写详细内容。\n\n"
                "## 结论\n综上所述，以上内容值得关注。\n"
            ),
            "post": (
                f"📢 **{title}**\n\n{prompt}\n\n#小米 #运营 #MINO"
            ),
            "report": (
                f"# {title} — 运营报告\n\n"
                f"**报告摘要**: {prompt}\n\n"
                "---\n\n"
                "| 指标 | 数值 |\n|------|------|\n"
                "| 完成率 | 98% |\n"
                "| 覆盖率 | 95% |\n"
            ),
        }
        return templates.get(content_type, f"# {title}\n\n{prompt}")

    def list_content(self, status: Optional[str] = None) -> list:
        """Return all content records, optionally filtered by *status*."""
        records = list(self._content_store.values())
        if status:
            records = [r for r in records if r.get("status") == status]
        return records
