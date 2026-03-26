from __future__ import annotations

import ssl
from dataclasses import dataclass

from aiohttp import ClientSession, TCPConnector


@dataclass(slots=True)
class PanelClient:
    base_url: str
    username: str
    password: str
    verify_ssl: bool = True

    def _connector(self) -> TCPConnector:
        if self.verify_ssl:
            return TCPConnector()
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return TCPConnector(ssl=ssl_context)

    async def login(self) -> bool:
        if not self.base_url:
            return False
        async with ClientSession(connector=self._connector()) as session:
            async with session.post(
                f"{self.base_url}/login",
                data={"username": self.username, "password": self.password},
            ) as response:
                return response.status < 400
