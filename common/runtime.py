from __future__ import annotations
import signal
import threading
from typing import Callable, Optional
from common.logging_config import logger

class ServiceRuntime:
    """Simple lifecycle loop with graceful SIGINT/SIGTERM handling."""

    def __init__(
        self,
        on_tick: Optional[Callable[[], None]] = None,
        tick_seconds: float = 0.5,
        log_prefix: str = "[agent]"
    ):
        self._stopping = threading.Event()
        self._on_tick = on_tick
        self._tick = tick_seconds
        self._log_prefix = log_prefix

    def install_signals(self):
        """Install SIGINT/SIGTERM handlers (no-op if not on main thread)."""
        try:
            signal.signal(signal.SIGTERM, self._graceful)
            signal.signal(signal.SIGINT, self._graceful)
            logger.debug(f"[Agent]: {self._log_prefix} signal handlers installed for SIGTERM/SIGINT")
        except ValueError:
            # Raised if called outside the main thread.
            logger.debug(f"[Agent]: {self._log_prefix} signal handlers not installed (not main thread)", )
        except Exception as e:
            logger.warning(f"[Agent]: {self._log_prefix} failed installing signal handlers: {e}")

    def run_forever(self):
        self.install_signals()
        logger.info(f"[Agent]:{self._log_prefix} runtime starting (tick={self._tick})")
        while not self._stopping.is_set():
            if self._on_tick:
                try:
                    self._on_tick()
                except Exception:
                    logger.exception(f"[Agent]: {self._log_prefix} tick error")
            # Use Event.wait to allow immediate wake-up on stop
            self._stopping.wait(self._tick)
        logger.info(f"[Agent]: {self._log_prefix} runtime stopped")

    def stop(self):
        """Programmatic stop."""
        logger.info(f"[Agent]: {self._log_prefix} stop requested")
        self._stopping.set()

    def _graceful(self, *_):
        logger.info(f"[Agent]: {self._log_prefix} received shutdown signal. Exiting...")
        self._stopping.set()
