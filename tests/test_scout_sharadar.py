import io
import tempfile
import unittest
import zipfile
from pathlib import Path

import httpx

from scout.data import sharadar


class SharadarTests(unittest.TestCase):
    def test_bulk_follows_the_redirect_without_the_key(self):
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append((request.url.host, request.headers.get("x-api-key")))
            if request.url.host == "api.sharadar.com":
                return httpx.Response(302, headers={"location": "https://files.example.com/t.zip?sig=abc"})
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as z:
                z.writestr("SHARADAR_T.csv", "ticker,date,close\nAAA,2020-01-02,1.5\nBBB,2020-01-02,\n")
            return httpx.Response(200, content=buf.getvalue())

        with tempfile.TemporaryDirectory() as d, httpx.Client(transport=httpx.MockTransport(handler)) as c:
            root = Path(d)
            entry = sharadar.download("secret-key", "t", root=root, client=c)
            self.assertEqual(seen, [("api.sharadar.com", "secret-key"), ("files.example.com", None)])
            self.assertNotIn("secret-key", str(entry))
            p = sharadar.to_parquet("t", root=root)
            self.assertEqual(p["rows"], 2)
            import pandas as pd

            df = pd.read_parquet(root / "parquet" / "t.parquet")
            self.assertEqual(df["close"].tolist()[0], "1.5")
            self.assertTrue(pd.isna(df["close"].tolist()[1]))
            sharadar.purge(root)
            self.assertFalse(root.exists())


    def test_an_interrupted_download_resumes(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("SHARADAR_T.csv", "ticker,date,close\n" + "AAA,2020-01-02,1.5\n" * 2000)
        body = buf.getvalue()
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.sharadar.com":
                return httpx.Response(302, headers={"location": "https://files.example.com/t.zip"})
            calls["n"] += 1
            if calls["n"] == 1:
                raise httpx.ReadError("cut")
            rng = request.headers.get("range")
            if rng:
                start = int(rng.split("=")[1].rstrip("-"))
                return httpx.Response(206, content=body[start:], headers={"content-range": f"bytes {start}-{len(body) - 1}/{len(body)}"})
            if calls["n"] == 2:                     # first full answer cut halfway
                return httpx.Response(200, content=body[: len(body) // 2], headers={"content-length": str(len(body))})
            return httpx.Response(200, content=body)

        with tempfile.TemporaryDirectory() as d, httpx.Client(transport=httpx.MockTransport(handler)) as c:
            from unittest import mock

            with mock.patch("time.sleep"):
                entry = sharadar.download("k", "t", root=Path(d), client=c)
            self.assertEqual(entry["bytes"], len(body))


if __name__ == "__main__":
    unittest.main()
