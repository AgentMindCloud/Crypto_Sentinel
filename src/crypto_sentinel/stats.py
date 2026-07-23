from __future__ import annotations

import math
import statistics
from collections.abc import Sequence


def robust_zscore(value: float, sample: Sequence[float]) -> float | None:
    """Return a median/MAD z-score, with a standard-deviation fallback.

    MAD-based scores are less distorted by the heavy tails common in crypto data.
    """
    clean = [float(x) for x in sample if math.isfinite(float(x))]
    if len(clean) < 5 or not math.isfinite(value):
        return None
    median = statistics.median(clean)
    deviations = [abs(x - median) for x in clean]
    mad = statistics.median(deviations)
    if mad > 1e-12:
        return 0.6744897501960817 * (value - median) / mad
    if len(clean) >= 2:
        stdev = statistics.stdev(clean)
        if stdev > 1e-12:
            return (value - statistics.mean(clean)) / stdev
    return 0.0 if math.isclose(value, median, rel_tol=1e-12, abs_tol=1e-12) else None


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
