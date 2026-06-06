import unittest

from services.openai_chat_compat import build_chat_completion_kwargs, is_gpt5_family


class OpenAIChatCompatTest(unittest.TestCase):
    def test_gpt5_family_uses_max_completion_tokens_and_default_temperature(self):
        kwargs = build_chat_completion_kwargs(
            model="gpt-5.4-mini",
            messages=[{"role": "user", "content": "json"}],
            temperature=0.2,
            max_tokens=1200,
            response_format={"type": "json_object"},
        )

        self.assertTrue(is_gpt5_family("gpt-5.4-mini-2026-03-17"))
        self.assertEqual(kwargs["max_completion_tokens"], 1200)
        self.assertNotIn("max_tokens", kwargs)
        self.assertNotIn("temperature", kwargs)
        self.assertEqual(kwargs["response_format"], {"type": "json_object"})

    def test_legacy_chat_model_keeps_existing_parameters(self):
        kwargs = build_chat_completion_kwargs(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "json"}],
            temperature=0.1,
            max_tokens=400,
        )

        self.assertFalse(is_gpt5_family("gpt-4o-mini"))
        self.assertEqual(kwargs["max_tokens"], 400)
        self.assertNotIn("max_completion_tokens", kwargs)
        self.assertEqual(kwargs["temperature"], 0.1)


if __name__ == "__main__":
    unittest.main()
