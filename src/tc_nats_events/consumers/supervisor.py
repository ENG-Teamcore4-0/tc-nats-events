"""
Loop supervision and health (NATS-07)
=====================================

The fetch loop used to exit after 10 consecutive errors and nothing restarted
it: the pod kept running while consuming nothing. The supervisor restarts it
with capped exponential backoff and exposes health for readiness probes.
"""

import time
from dataclasses import dataclass, replace
from typing import Any, Dict, Optional

MAX_CONSECUTIVE_ERRORS = 10
MAX_RESTART_DELAY_SECONDS = 60.0


def restart_delay(streak: int) -> float:
    """Delay before restart number ``streak`` of the current failure episode."""
    return min(2.0 ** max(streak - 1, 0), MAX_RESTART_DELAY_SECONDS)


def error_delay(consecutive_errors: int) -> float:
    return float(min(consecutive_errors, 10))


@dataclass(frozen=True)
class HealthSnapshot:
    """
    Immutable health record. ``restarts`` is the lifetime total (for metrics);
    ``restart_streak`` resets on the first successful fetch so a failure days
    later starts again from a short backoff.
    """

    restarts: int = 0
    restart_streak: int = 0
    last_progress_monotonic: Optional[float] = None
    last_progress_at: Optional[float] = None
    last_error: Optional[str] = None

    def progress(self) -> "HealthSnapshot":
        """A fetch returned, a message was handled or a heartbeat ran."""
        return replace(
            self,
            last_progress_monotonic=time.monotonic(),
            last_progress_at=time.time(),
        )

    def fetch_ok(self) -> "HealthSnapshot":
        return replace(self.progress(), restart_streak=0)

    def failed(self, error: Exception) -> "HealthSnapshot":
        return replace(self, last_error=f"{type(error).__name__}: {error}")

    def restarted(self) -> "HealthSnapshot":
        return replace(
            self, restarts=self.restarts + 1, restart_streak=self.restart_streak + 1
        )

    def as_dict(self, state: str, running: bool, stale_after: float) -> Dict[str, Any]:
        fresh = (
            self.last_progress_monotonic is not None
            and time.monotonic() - self.last_progress_monotonic < stale_after
        )
        return {
            "state": state,
            "healthy": running and state in {"syncing", "live"} and fresh,
            "restarts": self.restarts,
            "last_fetch_ok_at": self.last_progress_at,
            "last_error": self.last_error,
        }
