"""Checkpoint manager — persists pipeline watermark between runs."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class CheckpointManager:
    """
    Tracks which source files have already been processed using content hashes.

    Using MD5 of file bytes rather than filename/mtime means the pipeline
    correctly handles renamed files, re-delivered files, and S3 eventual-
    consistency re-uploads — the identity is the content, not the path.
    """

    def __init__(self, checkpoint_path: Path) -> None:
        self._path = checkpoint_path
        self._state: dict = self._load()

    def _load(self) -> dict:
        if self._path.exists():
            with open(self._path) as f:
                return json.load(f)
        return {"processed_hashes": [], "watermark": _EPOCH.isoformat(), "run_count": 0}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            json.dump(self._state, f, indent=2, default=str)

    def is_processed(self, content_hash: str) -> bool:
        return content_hash in self._state["processed_hashes"]

    def mark_processed(self, content_hash: str, watermark: datetime) -> None:
        hashes: list = self._state["processed_hashes"]
        if content_hash not in hashes:
            hashes.append(content_hash)
        current = datetime.fromisoformat(self._state["watermark"])
        if watermark > current:
            self._state["watermark"] = watermark.isoformat()
        self._state["run_count"] = self._state.get("run_count", 0) + 1
        self._save()
        logger.debug("Checkpoint updated: hash=%s watermark=%s", content_hash, watermark)

    @property
    def watermark(self) -> datetime:
        return datetime.fromisoformat(self._state["watermark"])

    def reset(self, from_date: datetime | None = None) -> None:
        """Reset checkpoint for a full or partial backfill."""
        if from_date is None:
            self._state = {"processed_hashes": [], "watermark": _EPOCH.isoformat(), "run_count": 0}
        else:
            self._state["watermark"] = from_date.isoformat()
            self._state["processed_hashes"] = []
        self._save()
        logger.info("Checkpoint reset to %s", from_date or "epoch")
