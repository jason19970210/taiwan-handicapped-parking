"""
Per-city row-count guard for the collect step.

Compares the row counts of a freshly collected dataset with the currently
committed CSV and reports cities whose count fell (or exploded) more than a
threshold, so a truncated or unfiltered API run can never be written over good
data.
"""

from typing import Dict, List, Optional

import pandas as pd
from pathlib import Path

DEFAULT_MAX_DROP_PCT = 5.0
DEFAULT_MAX_GROWTH_PCT = 50.0


def city_counts(df: pd.DataFrame) -> Dict[str, int]:
    """Row count per value of the `city` column."""
    return {str(k): int(v) for k, v in df["city"].value_counts().items()}


def read_committed_counts(csv_path: str) -> Dict[str, int]:
    """Per-city counts of the CSV currently on disk ({} if there is none)."""
    path = Path(csv_path)
    if not path.exists():
        return {}
    return city_counts(pd.read_csv(path, usecols=["city"], encoding="utf-8-sig"))


def check_city_counts(
    old: Dict[str, int],
    new: Dict[str, int],
    max_drop_pct: float = DEFAULT_MAX_DROP_PCT,
    max_growth_pct: Optional[float] = DEFAULT_MAX_GROWTH_PCT,
) -> List[str]:
    """
    Return one message per city whose count moved too far.

    A city that vanished counts as a 100% drop. Cities with no baseline (or a
    baseline of 0) are never flagged. A drop of exactly `max_drop_pct` passes.
    `max_growth_pct=None` disables the growth check.
    """
    problems = []
    for city, old_n in sorted(old.items()):
        if old_n <= 0:
            continue
        new_n = new.get(city, 0)
        change_pct = (new_n - old_n) / old_n * 100.0
        if change_pct < -max_drop_pct:
            problems.append(
                f"{city}: {old_n} -> {new_n} rows ({change_pct:+.1f}%, "
                f"limit -{max_drop_pct:g}%)"
            )
        elif max_growth_pct is not None and change_pct > max_growth_pct:
            problems.append(
                f"{city}: {old_n} -> {new_n} rows ({change_pct:+.1f}%, "
                f"limit +{max_growth_pct:g}%)"
            )
    return problems
