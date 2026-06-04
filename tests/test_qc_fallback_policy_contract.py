import ast
import tempfile
import unittest
from pathlib import Path

from services.execution_policy import ROLE_POLICIES, TICKER_ROLES, TickerRole
from tools.generate_qc_fallback_policy import (
    compare_qc_fallback_policy,
    expected_qc_fallback_policy,
    generate_policy_snippet,
    load_qc_fallback_policy,
)


QC_FILE = Path(__file__).resolve().parents[2] / "quantconnect_files" / "test1.py"


class QCFallbackPolicyContractTest(unittest.TestCase):
    def test_generator_emits_deterministic_fastapi_policy_snippet(self):
        snippet = generate_policy_snippet()
        expected = expected_qc_fallback_policy()

        self.assertIn(f'POLICY_VERSION = "{expected["POLICY_VERSION"]}"', snippet)
        self.assertIn('"PSI":"thematic"', snippet)
        self.assertIn('"hedge":{"max_single":0.03,"max_total_group":0.08}', snippet)

    def test_qc_checker_passes_current_quantconnect_file(self):
        result = compare_qc_fallback_policy(QC_FILE)

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["expected_counts"]["tickers"], len(TICKER_ROLES))

    def test_qc_checker_detects_policy_drift(self):
        expected = expected_qc_fallback_policy()
        roles = dict(expected["TICKER_ROLES"])
        roles["PSI"] = "sector"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "bad_qc.py"
            path.write_text(
                "\n".join([
                    "class QCAgenticV1:",
                    f'    POLICY_VERSION = "{expected["POLICY_VERSION"]}"',
                    f"    TICKER_ROLES = {roles!r}",
                    f"    ROLE_CAPS = {expected['ROLE_CAPS']!r}",
                ]),
                encoding="utf-8",
            )

            result = compare_qc_fallback_policy(path)

        self.assertFalse(result["ok"])
        self.assertEqual(result["differences"][0]["field"], "TICKER_ROLES")
        self.assertIn("PSI", result["differences"][0]["changed"])

    def test_qc_fallback_roles_match_fastapi_policy(self):
        qc_policy = _load_qc_policy_constants()
        expected = {ticker: role.value for ticker, role in TICKER_ROLES.items()}

        self.assertEqual(qc_policy["TICKER_ROLES"], expected)
        self.assertEqual(qc_policy["TICKER_ROLES"]["PSI"], "thematic")
        for ticker in ["TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VIXY"]:
            self.assertEqual(qc_policy["TICKER_ROLES"][ticker], "hedge")

    def test_qc_fallback_caps_match_fastapi_policy(self):
        qc_policy = _load_qc_policy_constants()
        expected = {
            role.value: {
                "max_single": policy.max_single_weight,
                "max_total_group": policy.max_total_group_weight,
            }
            for role, policy in ROLE_POLICIES.items()
        }

        self.assertEqual(qc_policy["ROLE_CAPS"], expected)
        self.assertEqual(qc_policy["ROLE_CAPS"]["thematic"]["max_single"], 0.075)
        self.assertEqual(qc_policy["ROLE_CAPS"]["hedge"]["max_single"], 0.03)
        self.assertEqual(qc_policy["ROLE_CAPS"]["hedge"]["max_total_group"], 0.08)

    def test_qc_fallback_policy_sync_and_version_are_deployed(self):
        text = QC_FILE.read_text()
        qc_policy = load_qc_fallback_policy(QC_FILE)

        self.assertEqual(qc_policy["POLICY_VERSION"], "sprint8a")
        self.assertIn('self._policy_source = "compiled_fallback"', text)
        self.assertIn('target == "PolicySync"', text)
        self.assertIn("def _apply_inline_policy", text)
        self.assertIn("_policy_payload_from_command(data)", text)
        self.assertIn('self._get_field(data, "payload_json", None)', text)
        self.assertIn('self._field_as_policy_dict(data, "roles")', text)
        self.assertIn('"policy_source={self._policy_source}', text)
        self.assertIn('"policy_version={self._policy_version', text)
        self.assertIn("[POLICY] Synced from FastAPI version=", text)
        self.assertIn("[POLICY] Inline SetWeights policy applied version=", text)
        self.assertIn("[POLICY] Inline SetWeights policy ignored: missing roles/caps", text)
        self.assertIn("[POLICY] Inline SetWeights policy ignored: version mismatch", text)
        self.assertIn("[POLICY] Inline SetWeights policy skipped", text)
        self.assertIn("[POLICY] source=", text)

    def test_qc_setweights_inline_policy_is_optional_when_version_aligned(self):
        text = QC_FILE.read_text()

        self.assertNotIn("invalid inline execution policy", text)
        self.assertIn("[POLICY] Inline SetWeights policy skipped", text)
        self.assertIn("policy_mismatch = policy_version != current_policy_version", text)

    def test_qc_command_hardening_contract_is_deployed(self):
        text = QC_FILE.read_text()

        self.assertIn("duplicate_command_id", text)
        self.assertIn("missing_policy_version", text)
        self.assertIn("policy_version_mismatch_with_buy", text)
        self.assertIn("Version mismatch but reduce-only", text)
        self.assertIn("_is_reduce_only_command", text)
        self.assertIn("_current_portfolio_weight", text)
        self.assertIn('"policy_mismatch": bool(policy_mismatch)', text)
        self.assertIn('"actual_target_weights": self._dict_from_qc_object(actual_target_weights or {})', text)
        self.assertIn('"order_summary": self._dict_from_qc_object(order_summary or {})', text)
        self.assertIn('"execution_state": self._execution_state_from_order_summary(order_summary or {}, status)', text)
        self.assertIn('"active_command_id": self._active_command_id', text)
        self.assertIn("def _ticket_summaries", text)
        self.assertIn('"actual_order_count": actual_count', text)
        self.assertIn('"submitted_order_count": actual_count', text)
        self.assertIn('"execution_state": "noop_reconciled" if is_noop else None', text)
        self.assertIn('"is_noop": is_noop', text)
        self.assertIn('"noop_reason": "target_matches_current" if is_noop else None', text)
        self.assertIn('"open_order_count_after": open_after', text)
        self.assertIn("unknown tickers rejected", text)

    def test_qc_command_payload_helper_supports_dotnet_dictionary(self):
        text = QC_FILE.read_text()

        self.assertIn("def _get_field(data, key: str, default=None):", text)
        self.assertIn('getattr(data, "ContainsKey", None)', text)
        self.assertIn("callable(contains) and contains(key)", text)
        self.assertIn("return data[key]", text)
        self.assertIn("def _is_mapping_like(raw) -> bool:", text)

    def test_qc_policy_sync_ack_path_is_no_throw_for_none_values(self):
        text = QC_FILE.read_text()

        self.assertIn("payload = self._policy_payload_from_command(data)", text)
        self.assertIn('self._get_field(data, "payload_json", None)', text)
        self.assertIn('if raw is None:', text)
        self.assertIn("orders = self.transactions.get_open_orders()", text)
        self.assertIn("return len(list(orders or []))", text)
        self.assertIn("target_weights = self._dict_from_qc_object(getattr(self, \"_target_weights\", {}) or {})", text)
        self.assertIn("actual_target_weights\": self._dict_from_qc_object(actual_target_weights or {})", text)
        self.assertIn("def _is_market_open_safe", text)
        self.assertIn('"IsMarketOpen"', text)
        self.assertIn("def _mapping_get", text)
        self.assertIn("def _policy_version", text)
        self.assertIn("def _policy_roles", text)
        self.assertIn("def _policy_caps", text)
        self.assertNotIn("_execution_policy.get", text)
        self.assertNotIn("payload.get", text)
        self.assertNotIn("policy.get", text)
        self.assertIn("def _set_holdings_safe", text)
        self.assertIn("def _liquidate_safe", text)
        self.assertIn('"SetHoldings"', text)
        self.assertIn('"Liquidate"', text)
        self.assertIn("self._processed_data_command_ids = set()", text)
        self.assertIn("self._active_command_id = None", text)
        self.assertIn("def _update_active_execution_state", text)
        self.assertIn("def _execution_state_from_order_summary", text)
        self.assertIn('explicit = str(order_summary.get("execution_state") or "").lower().strip()', text)
        self.assertIn('submitted = int(order_summary.get("actual_order_count", order_summary.get("submitted_order_count") or 0) or 0)', text)
        self.assertIn("def _classify_new_command_vs_active", text)
        self.assertIn("def _is_same_target", text)
        self.assertIn("def _active_execution_order_summary", text)
        self.assertIn("def _supersede_active_execution", text)
        self.assertIn("def _cancel_open_orders_safe", text)
        self.assertIn("def _cancel_order_by_id", text)
        self.assertIn('target == "CancelOrders"', text)
        self.assertIn('"target_command_id"', text)
        self.assertIn('"action": "cancel"', text)
        self.assertIn("active_command_in_progress", text)
        self.assertIn("already_in_progress", text)
        self.assertIn("reduce_only_override_candidate", text)
        self.assertIn("reduce_only_override_cancel_failed", text)
        self.assertIn("emergency_override", text)
        self.assertIn('"superseded_command_id": superseded_command_id', text)
        self.assertIn('"canceled_order_count": canceled_order_count', text)
        self.assertIn('"active_execution_status": self._active_execution_status', text)
        self.assertIn('"processed_command_count": len(processed_command_ids)', text)
        self.assertIn("[ACK] account snapshot degraded:", text)
        self.assertNotIn('self.is_market_open("SPY")', text)
        self.assertNotIn("self.set_holdings(", text)
        self.assertNotIn("self.liquidate(", text)

    def test_qc_thematic_fallback_cap_is_not_legacy_five_percent(self):
        qc_policy = _load_qc_policy_constants()

        self.assertEqual(qc_policy["TICKER_ROLES"]["FTXL"], "thematic")
        self.assertEqual(qc_policy["TICKER_ROLES"]["PSI"], "thematic")
        self.assertEqual(qc_policy["ROLE_CAPS"]["thematic"]["max_single"], 0.075)
        self.assertGreater(qc_policy["ROLE_CAPS"]["thematic"]["max_single"], 0.051)

    def test_qc_universe_matches_fastapi_execution_policy(self):
        qc_policy = _load_qc_policy_constants()
        qc_universe = _load_qc_universe_constants()

        self.assertEqual(qc_universe, set(qc_policy["TICKER_ROLES"]))
        self.assertEqual(qc_universe, set(TICKER_ROLES))


def _load_qc_policy_constants() -> dict:
    return {
        "TICKER_ROLES": load_qc_fallback_policy(QC_FILE)["TICKER_ROLES"],
        "ROLE_CAPS": load_qc_fallback_policy(QC_FILE)["ROLE_CAPS"],
    }


def _load_qc_universe_constants() -> set[str]:
    tree = ast.parse(QC_FILE.read_text())
    class_node = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "QCAgenticV1"
    )
    env: dict[str, object] = {}
    for node in class_node.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id in {"CORE_UNIVERSE", "SATELLITE_UNIVERSE", "HEDGE_UNIVERSE"}:
            env[target.id] = _eval_policy_node(node.value, env)
    if {"CORE_UNIVERSE", "SATELLITE_UNIVERSE", "HEDGE_UNIVERSE"}.issubset(env):
        return set(env["CORE_UNIVERSE"]) | set(env["SATELLITE_UNIVERSE"]) | set(env["HEDGE_UNIVERSE"])
    return set(load_qc_fallback_policy(QC_FILE)["TICKER_ROLES"])


def _eval_policy_node(node: ast.AST, env: dict[str, object]):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return env[node.id]
    if isinstance(node, ast.Dict):
        return {
            _eval_policy_node(key, env): _eval_policy_node(value, env)
            for key, value in zip(node.keys, node.values)
        }
    if isinstance(node, ast.List):
        return [_eval_policy_node(item, env) for item in node.elts]
    raise AssertionError(f"Unsupported QC policy node: {ast.dump(node)}")


if __name__ == "__main__":
    unittest.main()
