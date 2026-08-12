import unittest
from decimal import Decimal

from pnl_money import (
    Amount,
    Unavailable,
    combine,
    format_delta_usd,
    format_usd,
    round_usd,
    to_decimal,
)


class ToDecimalTest(unittest.TestCase):
    def test_parses_string_without_float(self):
        self.assertEqual(to_decimal("350.5000"), Decimal("350.5000"))

    def test_parses_int(self):
        self.assertEqual(to_decimal(7), Decimal("7"))

    def test_float_does_not_leak_binary_error(self):
        # 0.1 as a float is 0.1000000000000000055511151231257827
        self.assertEqual(to_decimal(0.1), Decimal("0.1"))


class RoundUsdTest(unittest.TestCase):
    def test_rounds_half_up(self):
        self.assertEqual(round_usd(Decimal("0.5")), Decimal("1"))

    def test_rounds_to_whole_dollars(self):
        self.assertEqual(round_usd(Decimal("12429.62")), Decimal("12430"))


class FormatUsdTest(unittest.TestCase):
    def test_thousands_separator(self):
        self.assertEqual(format_usd(Decimal("12430")), "$12,430")

    def test_negative(self):
        self.assertEqual(format_usd(Decimal("-1200")), "-$1,200")

    def test_small_negative_never_renders_negative_zero(self):
        # The trap: -0.4 formats as "-0" under naive formatting.
        self.assertEqual(format_usd(Decimal("-0.4")), "$0")

    def test_small_positive_is_plain_zero(self):
        self.assertEqual(format_usd(Decimal("0.4")), "$0")


class FormatDeltaUsdTest(unittest.TestCase):
    def test_positive_has_explicit_plus(self):
        self.assertEqual(format_delta_usd(Decimal("1200.4")), "+$1,200")

    def test_negative_keeps_minus_before_dollar(self):
        self.assertEqual(format_delta_usd(Decimal("-1200.4")), "-$1,200")

    def test_zero_has_no_sign(self):
        self.assertEqual(format_delta_usd(Decimal("0.4")), "$0")


class CombineTest(unittest.TestCase):
    def test_sums_available(self):
        result = combine([Amount(Decimal("10")), Amount(Decimal("5"))], "none")
        self.assertEqual(result, Amount(Decimal("15")))

    def test_ignores_unavailable_but_keeps_the_rest(self):
        result = combine([Amount(Decimal("10")), Unavailable("meta down")], "none")
        self.assertEqual(result, Amount(Decimal("10")))

    def test_all_unavailable_yields_unavailable_not_zero(self):
        result = combine([Unavailable("a"), Unavailable("b")], "nothing readable")
        self.assertEqual(result, Unavailable("nothing readable"))

    def test_empty_yields_unavailable_not_zero(self):
        self.assertEqual(combine([], "nothing readable"), Unavailable("nothing readable"))
