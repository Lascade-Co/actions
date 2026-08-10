"""Money primitives and the two-state source value.

Every figure in this pipeline is a Decimal. A source either produced an amount
or it did not; there is no third state, and an ``Unavailable`` is never turned
into ``0`` — a plausible zero is indistinguishable from a genuinely quiet month,
which is the failure this whole pipeline is built to avoid.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Union


@dataclass(frozen=True)
class Amount:
    usd: Decimal


@dataclass(frozen=True)
class Unavailable:
    reason: str


SourceValue = Union[Amount, Unavailable]

_WHOLE = Decimal("1")


def to_decimal(raw) -> Decimal:
    """Decimal from anything, via str so a float's binary error never lands."""
    return Decimal(str(raw))


def round_usd(value: Decimal) -> Decimal:
    return value.quantize(_WHOLE, rounding=ROUND_HALF_UP)


def format_usd(value: Decimal) -> str:
    """Render whole dollars. A value that rounds to zero renders ``$0``.

    Deciding the sign from the unrounded value is what produces ``-$0`` for a
    small negative; the rounded value is the one the reader sees, so it is the
    one the sign must agree with.
    """
    rounded = round_usd(value)
    if rounded == 0:
        return "$0"
    if rounded < 0:
        return f"-${-rounded:,}"
    return f"${rounded:,}"


def combine(values: Iterable[SourceValue], reason: str) -> SourceValue:
    """Sum the available values; ``Unavailable(reason)`` when none are."""
    amounts = [v.usd for v in values if isinstance(v, Amount)]
    if not amounts:
        return Unavailable(reason)
    return Amount(sum(amounts, Decimal("0")))
