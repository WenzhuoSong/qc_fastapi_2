import ast
import unittest
from pathlib import Path

from services.execution_policy import ROLE_POLICIES, TICKER_ROLES, TickerRole


QC_FILE = Path(__file__).resolve().parents[2] / "quantconnect_files" / "test1.py"


class QCFallbackPolicyContractTest(unittest.TestCase):
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

        self.assertIn('"version": "sprint8a_fallback"', text)
        self.assertIn('target == "PolicySync"', text)
        self.assertIn("_apply_inline_policy(data)", text)
        self.assertIn('"policy_source={self._policy_source}', text)
        self.assertIn('"policy_version={self._execution_policy.get', text)
        self.assertIn("[POLICY] Synced from FastAPI version=", text)
        self.assertIn("[POLICY] Inline SetWeights policy applied version=", text)
        self.assertIn("[POLICY] source=", text)

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
        self.assertIn("def _ticket_summaries", text)
        self.assertIn('"open_order_count_after": open_after', text)
        self.assertIn("unknown tickers rejected", text)

    def test_qc_command_payload_helper_supports_dotnet_dictionary(self):
        text = QC_FILE.read_text()

        self.assertIn("def _get_field(data, key: str, default=None):", text)
        self.assertIn('hasattr(data, "ContainsKey")', text)
        self.assertIn("data.ContainsKey(key)", text)
        self.assertIn("return data[key]", text)
        self.assertIn("def _is_mapping_like(raw) -> bool:", text)

    def test_qc_policy_sync_ack_path_is_no_throw_for_none_values(self):
        text = QC_FILE.read_text()

        self.assertIn("payload = self._dict_from_qc_object(self._get_field(data, \"payload\", {}) or {})", text)
        self.assertIn('if raw is None:', text)
        self.assertIn("orders = self.transactions.get_open_orders()", text)
        self.assertIn("return len(list(orders or []))", text)
        self.assertIn("target_weights = self._dict_from_qc_object(getattr(self, \"_target_weights\", {}) or {})", text)
        self.assertIn("actual_target_weights\": self._dict_from_qc_object(actual_target_weights or {})", text)

    def test_qc_thematic_fallback_cap_is_not_legacy_five_percent(self):
        qc_policy = _load_qc_policy_constants()

        self.assertEqual(qc_policy["TICKER_ROLES"]["FTXL"], "thematic")
        self.assertEqual(qc_policy["TICKER_ROLES"]["PSI"], "thematic")
        self.assertEqual(qc_policy["ROLE_CAPS"]["thematic"]["max_single"], 0.075)
        self.assertGreater(qc_policy["ROLE_CAPS"]["thematic"]["max_single"], 0.051)


def _load_qc_policy_constants() -> dict:
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
        if target.id.startswith("MAX_") or target.id in {"TICKER_ROLES", "ROLE_CAPS"}:
            env[target.id] = _eval_policy_node(node.value, env)
    return {
        "TICKER_ROLES": env["TICKER_ROLES"],
        "ROLE_CAPS": env["ROLE_CAPS"],
    }


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
    raise AssertionError(f"Unsupported QC policy node: {ast.dump(node)}")


if __name__ == "__main__":
    unittest.main()
