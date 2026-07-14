"""
Adaptive concurrency controller for the OpenAI ISIC classification runner.

Pure, deterministic decision logic with no I/O and no asyncio dependency, so
it can be unit-tested independently of the async runner. The runner calls
`decide()` once per completed window of requests; the controller never
inspects individual requests, only the aggregated window counts.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# Error rate at or above this always forces a decrease, regardless of
# --decrease-threshold (which only governs the retry_rate trigger).
ERROR_RATE_DECREASE_THRESHOLD = 0.02


@dataclass
class WindowDecision:
    window_number: int
    previous_concurrency: int
    new_concurrency: int
    completed: int
    requests_retried: int
    retry_events: int
    errors: int
    retry_rate: float
    error_rate: float
    reason: str

    @property
    def changed(self) -> bool:
        return self.new_concurrency != self.previous_concurrency


class AdaptiveConcurrencyController:
    """Adjusts concurrency once per completed window based on recent
    retry/error rates.

    Policy (decrease takes priority over increase; at most one adjustment
    per window; result always clamped to [min_concurrency, max_concurrency]):
      - decrease when retry_rate >= decrease_threshold OR error_rate >= 0.02
        -> new = max(min_concurrency, floor(current * decrease_factor))
      - else increase by increase_step when retry_rate <= increase_threshold
        AND error_rate == 0 AND current < max_concurrency
      - otherwise hold.
    """

    def __init__(
        self,
        start_concurrency: int,
        min_concurrency: int,
        max_concurrency: int,
        increase_threshold: float,
        decrease_threshold: float,
        increase_step: int,
        decrease_factor: float,
    ) -> None:
        if min_concurrency < 1:
            raise ValueError("min_concurrency must be >= 1")
        if max_concurrency < min_concurrency:
            raise ValueError("max_concurrency must be >= min_concurrency")
        if increase_step < 1:
            raise ValueError("increase_step must be >= 1")
        if not (0.0 < decrease_factor < 1.0):
            raise ValueError("decrease_factor must be between 0 and 1 (exclusive)")

        self.min_concurrency = min_concurrency
        self.max_concurrency = max_concurrency
        self.increase_threshold = increase_threshold
        self.decrease_threshold = decrease_threshold
        self.increase_step = increase_step
        self.decrease_factor = decrease_factor

        self.concurrency = max(min_concurrency, min(start_concurrency, max_concurrency))
        self._window_number = 0

    def decide(
        self,
        completed: int,
        requests_retried: int,
        retry_events: int,
        errors: int,
    ) -> WindowDecision:
        """Record one completed window's stats and return the adjustment decision.

        `completed` should count only requests that produced a real outcome
        (success or non-fatal terminal error) — exclude fatal/auth failures
        and requests skipped due to an in-progress abort, since those must
        not influence adaptation.
        """
        self._window_number += 1
        previous = self.concurrency

        retry_rate = (requests_retried / completed) if completed else 0.0
        error_rate = (errors / completed) if completed else 0.0

        new_concurrency = previous
        reason = "hold"

        if completed > 0 and (
            retry_rate >= self.decrease_threshold or error_rate >= ERROR_RATE_DECREASE_THRESHOLD
        ):
            candidate = max(self.min_concurrency, math.floor(previous * self.decrease_factor))
            if candidate < previous:
                new_concurrency = candidate
                reason = "decrease"
            else:
                reason = "decrease-condition-met-but-already-at-min"
        elif (
            completed > 0
            and retry_rate <= self.increase_threshold
            and error_rate == 0.0
            and previous < self.max_concurrency
        ):
            new_concurrency = min(self.max_concurrency, previous + self.increase_step)
            reason = "increase"

        self.concurrency = new_concurrency

        return WindowDecision(
            window_number=self._window_number,
            previous_concurrency=previous,
            new_concurrency=new_concurrency,
            completed=completed,
            requests_retried=requests_retried,
            retry_events=retry_events,
            errors=errors,
            retry_rate=retry_rate,
            error_rate=error_rate,
            reason=reason,
        )
