from __future__ import annotations

import asyncio

from app.bootstrap import run


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
