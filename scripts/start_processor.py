"""Standalone processor runner."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if __name__ == "__main__":
    import asyncio
    from processor.main import main
    asyncio.run(main())
