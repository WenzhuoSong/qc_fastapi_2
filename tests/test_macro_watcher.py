import unittest

from services.macro_watcher import _is_fomc_meeting_event


class MacroWatcherTests(unittest.TestCase):
    def test_fomc_minutes_are_not_treated_as_next_meeting(self):
        self.assertFalse(_is_fomc_meeting_event({"event": "FOMC Minutes"}))
        self.assertFalse(
            _is_fomc_meeting_event(
                {"event": "FOMC Minutes - Meeting of Apr. 28-29"}
            )
        )

    def test_fomc_meeting_is_next_meeting_candidate(self):
        self.assertTrue(_is_fomc_meeting_event({"event": "FOMC Meeting"}))
        self.assertTrue(
            _is_fomc_meeting_event(
                {"event": "Federal Open Market Committee Meeting"}
            )
        )
        self.assertTrue(
            _is_fomc_meeting_event({"event": "Fed Interest Rate Decision"})
        )


if __name__ == "__main__":
    unittest.main()
