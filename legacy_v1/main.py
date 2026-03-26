import asyncio

from app.bootstrap import run
from app.logging import configure_logging

configure_logging()


if __name__ == "__main__":
    asyncio.run(run())
