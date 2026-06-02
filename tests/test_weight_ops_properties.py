import unittest

from hypothesis import given
from hypothesis import strategies as st

from services.weight_ops import (
    apply_group_caps_cash_first,
    apply_single_caps_cash_first,
    normalize_cash_first,
    tighten_buy_delta,
)


weight_st = st.floats(min_value=0.0, max_value=0.6, allow_nan=False, allow_infinity=False)
ticker_weights_st = st.fixed_dictionaries(
    {
        "SPY": weight_st,
        "QQQ": weight_st,
        "XLK": weight_st,
        "SOXX": weight_st,
        "DRAM": weight_st,
    }
)


class WeightOpsPropertyTest(unittest.TestCase):
    @given(weights=ticker_weights_st)
    def test_normalize_cash_first_sum_is_exactly_one(self, weights):
        result, _ = normalize_cash_first(weights)
        total = sum(result.values())
        self.assertLess(abs(total - 1.0), 1e-9, f"sum={total}")

    @given(weights=ticker_weights_st)
    def test_normalize_cash_first_non_cash_never_inflated(self, weights):
        result, _ = normalize_cash_first(weights)
        for ticker, weight in weights.items():
            if ticker != "CASH":
                self.assertLessEqual(result.get(ticker, 0.0), weight + 1e-9)

    @given(
        weights=ticker_weights_st,
        cash_extra=st.floats(min_value=0.0, max_value=0.5, allow_nan=False, allow_infinity=False),
    )
    def test_normalize_cash_first_no_total_exceeds_one(self, weights, cash_extra):
        with_cash = dict(weights)
        with_cash["CASH"] = cash_extra
        result, _ = normalize_cash_first(with_cash)
        total = sum(result.values())
        self.assertLessEqual(total, 1.0 + 1e-9, f"total={total} with cash_extra={cash_extra}")

    @given(weights=ticker_weights_st)
    def test_apply_single_caps_cash_accounting(self, weights):
        caps = {"SPY": 0.15, "QQQ": 0.20, "XLK": 0.10}
        cash_before = weights.get("CASH", 0.0)
        result, diag = apply_single_caps_cash_first(weights, caps)
        cash_after = result.get("CASH", 0.0)
        self.assertLess(abs((cash_after - cash_before) - diag["total_released"]), 1e-9)

    @given(weights=ticker_weights_st)
    def test_apply_group_caps_cash_accounting(self, weights):
        role_map = {
            "SPY": "core",
            "QQQ": "core",
            "XLK": "sector",
            "SOXX": "thematic",
            "DRAM": "satellite",
        }
        group_caps = {"core": 0.25, "sector": 0.15, "thematic": 0.10}
        cash_before = weights.get("CASH", 0.0)
        result, diag = apply_group_caps_cash_first(weights, group_caps, role_map)
        cash_after = result.get("CASH", 0.0)
        self.assertLess(abs((cash_after - cash_before) - diag["total_released"]), 1e-9)

    @given(weights=ticker_weights_st, current_weights=ticker_weights_st)
    def test_tighten_buy_delta_never_exceeds_target(self, weights, current_weights):
        result, _ = tighten_buy_delta(weights, current_weights, max_buy_delta=0.05)
        for ticker, target_weight in weights.items():
            if ticker != "CASH":
                self.assertLessEqual(result.get(ticker, 0.0), target_weight + 1e-9)

    @given(weights=ticker_weights_st, current_weights=ticker_weights_st)
    def test_tighten_buy_delta_cash_accounting(self, weights, current_weights):
        cash_before = weights.get("CASH", 0.0)
        result, diag = tighten_buy_delta(weights, current_weights, max_buy_delta=0.05)
        cash_after = result.get("CASH", 0.0)
        self.assertLess(abs((cash_after - cash_before) - diag["total_released"]), 1e-9)


if __name__ == "__main__":
    unittest.main()
