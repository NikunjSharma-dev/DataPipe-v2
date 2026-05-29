"""
datapipe/watcher.py — Real-time file watcher.

v2: Uses watchdog (inotify / FSEvents / kqueue) instead of polling.
Mirrors DeltaContext's Indexer.start_watcher() pattern.
Falls back to polling if watchdog is unavailable.
"""

from __future__ import annotations

import logging
import signal
import time
from pathlib import Path
from threading import Event, Thread
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datapipe.engine import Pipeline

logger = logging.getLogger("datapipe.watcher")


class FileWatcher:
    """
    Real-time file watcher that triggers pipeline.run() on any change.

    Uses watchdog (inotify / FSEvents) when available; falls back to
    a 2-second polling loop otherwise.
    """

    def __init__(self, pipeline: "Pipeline", poll_interval: float = 2.0) -> None:
        self.pipeline = pipeline
        self.poll_interval = poll_interval
        self._stop = Event()
        self._thread: Thread | None = None

    # -- public API ----------------------------------------------------------

    def start(self) -> None:
        self._stop.clear()
        try:
            self._thread = Thread(
                target=self._watchdog_loop, daemon=True, name="datapipe-watchdog"
            )
        except ImportError:
            self._thread = Thread(
                target=self._poll_loop, daemon=True, name="datapipe-poller"
            )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def watch_until_signal(self) -> None:
        """Block until SIGINT / SIGTERM, then stop cleanly."""
        self.start()
        done = Event()

        def _handler(sig, frame):
            print("\n[datapipe] Stopping watcher…")
            done.set()

        signal.signal(signal.SIGINT,  _handler)
        signal.signal(signal.SIGTERM, _handler)
        done.wait()
        self.stop()

    # -- watchdog implementation ---------------------------------------------

    def _watchdog_loop(self) -> None:
        from watchdog.events import FileSystemEventHandler, FileSystemEvent
        from watchdog.observers import Observer

        pipe = self.pipeline
        _debounce: dict[str, float] = {}
        _DEBOUNCE_S = 0.5

        class _Handler(FileSystemEventHandler):
            def on_any_event(self, event: FileSystemEvent):
                if event.is_directory:
                    return
                now = time.monotonic()
                key = getattr(event, "src_path", "")
                if now - _debounce.get(key, 0) < _DEBOUNCE_S:
                    return
                _debounce[key] = now
                logger.info("Watchdog: %s %s", event.event_type, key)
                try:
                    stats = pipe.run()
                    logger.info("Re-index:\n%s", stats.summary())
                except Exception as exc:
                    logger.error("Re-index failed: %s", exc)

        observer = Observer()
        for directory, _ in pipe._source_dirs:
            if directory.exists():
                observer.schedule(_Handler(), str(directory), recursive=True)

        observer.start()
        logger.info("Watchdog started (inotify/FSEvents)")
        try:
            while not self._stop.is_set():
                time.sleep(0.5)
        finally:
            observer.stop()
            observer.join()
            logger.info("Watchdog stopped.")

    # -- polling fallback ----------------------------------------------------

    def _poll_loop(self) -> None:
        logger.info("Polling watcher started (%.1fs interval)", self.poll_interval)
        snapshot = self._take_snapshot()
        while not self._stop.is_set():
            time.sleep(self.poll_interval)
            if self._stop.is_set():
                break
            new_snap = self._take_snapshot()
            if new_snap != snapshot:
                logger.info("Change detected — re-indexing…")
                try:
                    stats = self.pipeline.run()
                    logger.info("Re-index:\n%s", stats.summary())
                except Exception as exc:
                    logger.error("Re-index failed: %s", exc)
                snapshot = new_snap
        logger.info("Poll watcher stopped.")

    def _take_snapshot(self) -> dict[str, float]:
        snap: dict[str, float] = {}
        for directory, patterns in self.pipeline._source_dirs:
            if not directory.exists():
                continue
            for pattern in patterns:
                for path in directory.rglob(pattern):
                    if path.is_file():
                        try:
                            snap[str(path)] = path.stat().st_mtime
                        except OSError:
                            pass
        return snap
