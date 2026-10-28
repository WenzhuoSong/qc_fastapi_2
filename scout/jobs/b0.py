"""B0 interface: `python -m scout.jobs.b0 export [--out DIR]` | `import <file.yaml>` (see `scout.b0`)."""
import sys
from pathlib import Path

from scout import b0
from scout.jobs._run import run


async def main() -> dict:
    if len(sys.argv) < 2 or sys.argv[1] not in ("export", "import"):
        raise SystemExit("usage: python -m scout.jobs.b0 export [--out DIR] | import <file.yaml>")
    if sys.argv[1] == "export":
        out = Path(sys.argv[sys.argv.index("--out") + 1]) if "--out" in sys.argv else None
        return await b0.export(out)
    path = Path(sys.argv[2])
    return {"imported": await b0.import_entries(b0.load_import(path), f"从 B0 导入（{path.name}）")}


if __name__ == "__main__":
    run("b0", main, alert=False)
