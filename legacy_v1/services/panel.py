import asyncio
import uuid
import logging
import secrets
from typing import Any, Dict, List, Optional, Tuple
import aiohttp
import random
import time
import json

from config import Config

logger = logging.getLogger(__name__)


class PanelAPI:
    def __init__(self) -> None:
        self.apibase = Config.PANEL_BASE.rstrip("/")
        self.username = Config.PANEL_LOGIN
        self.password = Config.PANEL_PASSWORD
        self.verifyssl = Config.VERIFY_SSL
        self.session: Optional[aiohttp.ClientSession] = None
        self.token: Optional[str] = None
        self.logged_in: bool = False
        self.lock = asyncio.Lock()
        self.request_retries: int = 3
        self.retry_backoff: float = 0.75

    @staticmethod
    def _target_inbound_ids() -> List[int]:
        raw = str(getattr(Config, "PANEL_TARGET_INBOUND_IDS", "") or "").strip()
        if not raw:
            return []
        result: List[int] = []
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                result.append(int(item))
            except ValueError:
                logger.warning("Panel config: invalid inbound id skipped: %s", item)
        limit = max(0, int(getattr(Config, "PANEL_TARGET_INBOUND_COUNT", 0) or 0))
        if limit > 0:
            return result[:limit]
        return result

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(ssl=self.verifyssl)
        timeout = aiohttp.ClientTimeout(total=15)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, cookie_jar=aiohttp.CookieJar(unsafe=True)
        )
        await self.login()

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def _request_json(self, method: str, url: str, **kwargs):
        if not self.session:
            return 0, {}, ""
        last_error = None
        for attempt in range(1, self.request_retries + 1):
            try:
                async with self.session.request(method, url, **kwargs) as resp:
                    text = await resp.text()
                    data = {}
                    if text:
                        try:
                            data = json.loads(text)
                        except json.JSONDecodeError:
                            logger.debug("Panel API non-JSON response from %s: %s", url, text[:500])
                            data = {}
                    if resp.status >= 500 and attempt < self.request_retries:
                        logger.warning("Panel API transient HTTP %s for %s attempt=%s/%s", resp.status, url, attempt, self.request_retries)
                        await asyncio.sleep(self.retry_backoff * attempt)
                        continue
                    return resp.status, data, text
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < self.request_retries:
                    logger.warning("Panel API retry %s/%s for %s after error: %s", attempt, self.request_retries, url, e)
                    await asyncio.sleep(self.retry_backoff * attempt)
                    continue
                logger.error("Panel API request failed: %s | error=%s", url, e)
                return 0, {}, ""
        if last_error:
            logger.error("Panel API request failed: url=%s error=%s", url, last_error)
        return 0, {}, ""

    @staticmethod
    def _needs_reauth(status: int, data: Dict[str, Any]) -> bool:
        if status in (401, 403, 404):
            return True
        if status == 200 and isinstance(data, dict) and data.get("success") is False:
            return True
        return False

    async def _request_json_with_reauth(self, method: str, url: str, **kwargs):
        status, data, text = await self._request_json(method, url, **kwargs)
        if self._needs_reauth(status, data):
            await self.login()
            status, data, text = await self._request_json(method, url, **kwargs)
        return status, data, text

    async def login(self) -> None:
        async with self.lock:
            if not self.session:
                return
            try:
                url = f"{self.apibase}/login"
                status, data, _ = await self._request_json(
                    "POST",
                    url,
                    json={"username": self.username, "password": self.password},
                )
                if status == 200 and data.get("success"):
                    # 3x-ui использует cookie-сессию, токен не нужен
                    self.logged_in = True
                    logger.info("Panel login ok | Вход в 3x-ui успешен")
                else:
                    self.logged_in = False
                    logger.error("Panel login failed: status=%s msg=%s", status, data.get("msg"))
            except Exception as e:
                self.logged_in = False
                logger.error("Panel auth exception: %s", e)

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    async def ensure_auth(self) -> None:
        if not self.logged_in:
            await self.login()

    async def get_inbounds(self) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        url = f"{self.apibase}/panel/api/inbounds/list"
        status, data, _ = await self._request_json_with_reauth(
            "GET", url, headers=self._headers()
        )
        if status == 200 and data.get("success"):
            obj = data.get("obj") or []
            logger.info("Fetched %s inbounds from panel", len(obj))
            return data

        logger.error("Panel getInbounds failed: url=%s status=%s msg=%s", url, status, data.get("msg"))
        return None

    def _parse_inbound_clients(self, inbound: Dict[str, Any]) -> List[Dict[str, Any]]:
        clients: List[Dict[str, Any]] = []
        settings = inbound.get("settings")

        if isinstance(settings, str):
            try:
                settings_obj = json.loads(settings)
                s_clients = settings_obj.get("clients") or []
                if isinstance(s_clients, list):
                    clients.extend(s_clients)
            except json.JSONDecodeError as exc:
                logger.debug("Panel inbound settings parse failed for inbound=%s: %s", inbound.get("id"), exc)
        elif isinstance(settings, dict):
            s_clients = settings.get("clients") or []
            if isinstance(s_clients, list):
                clients.extend(s_clients)

        protocol = inbound.get("protocol", "")
        for client in clients:
            client["protocol"] = protocol

        return clients

    @staticmethod
    def _normalize_inbound_settings(inbound: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        settings_raw = inbound.get("settings")
        if isinstance(settings_raw, str):
            try:
                settings = json.loads(settings_raw)
            except json.JSONDecodeError:
                settings = {}
        elif isinstance(settings_raw, dict):
            settings = dict(settings_raw)
        else:
            settings = {}

        clients = settings.get("clients")
        if not isinstance(clients, list):
            clients = []
        normalized_clients = [dict(item) for item in clients if isinstance(item, dict)]
        settings["clients"] = normalized_clients
        return settings, normalized_clients

    @staticmethod
    def _serialize_inbound_settings(inbound: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(inbound)
        payload["settings"] = json.dumps(settings, ensure_ascii=False, indent=2)
        return payload

    @staticmethod
    def _is_base_email(email: str, base_email: str) -> bool:
        if not email or not base_email:
            return False
        return email.endswith(base_email)

    @staticmethod
    def _base_email_aliases(base_email: str) -> List[str]:
        normalized = (base_email or "").strip()
        if not normalized or "@" not in normalized:
            return [normalized] if normalized else []
        local_part, domain = normalized.split("@", 1)
        aliases = [normalized]
        if local_part.startswith("user_"):
            aliases.append(f"{local_part[5:]}@{domain}")
        else:
            aliases.append(f"user_{local_part}@{domain}")
        result: List[str] = []
        for item in aliases:
            if item and item not in result:
                result.append(item)
        return result

    @classmethod
    def _email_for_inbound(cls, base_email: str, inbound_id: int) -> str:
        normalized = (base_email or "").strip()
        if not normalized or "@" not in normalized or inbound_id <= 0:
            return normalized
        local_part, domain = normalized.split("@", 1)
        return f"i{int(inbound_id)}_{local_part}@{domain}"

    @classmethod
    def _base_email_aliases_for_inbound(cls, base_email: str, inbound_id: int) -> List[str]:
        aliases: List[str] = []
        for item in cls._base_email_aliases(base_email):
            if item and item not in aliases:
                aliases.append(item)
            scoped = cls._email_for_inbound(item, inbound_id)
            if scoped and scoped not in aliases:
                aliases.append(scoped)
        return aliases

    @classmethod
    def _matches_any_base_email(cls, email: str, base_email: str) -> bool:
        return any(cls._is_base_email(email, alias) for alias in cls._base_email_aliases(base_email))

    @classmethod
    def _matches_base_email_for_inbound(cls, email: str, base_email: str, inbound_id: int) -> bool:
        return any(
            cls._is_base_email(email, alias)
            for alias in cls._base_email_aliases_for_inbound(base_email, inbound_id)
        )

    async def _update_inbound(self, inbound: Dict[str, Any], settings: Dict[str, Any]) -> bool:
        inbound_id = int(inbound.get("id") or 0)
        if inbound_id <= 0:
            return False
        payload = self._serialize_inbound_settings(inbound, settings)
        url = f"{self.apibase}/panel/api/inbounds/update/{inbound_id}"
        status, data, text = await self._request_json_with_reauth(
            "POST",
            url,
            headers=self._headers(),
            json=payload,
        )
        if status in (200, 201) and data.get("success"):
            return True
        logger.error(
            "Ошибка update inbound %s: status=%s msg=%s",
            inbound_id,
            status,
            data.get("msg"),
        )
        if text:
            logger.error(text)
        return False

    @staticmethod
    def _make_client_record(
        protocol: str,
        email: str,
        limit_ip: int,
        total_bytes: int,
        expiry_ms: int,
        sub_id: str,
        *,
        shared_client_id: str,
        shared_password: str,
        enable: bool = True,
        source: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        client: Dict[str, Any] = dict(source or {})
        client.update(
            {
                "email": email,
                "enable": enable,
                "flow": str(client.get("flow") or ""),
                "limitIp": limit_ip,
                "totalGB": total_bytes,
                "expiryTime": expiry_ms,
                "subId": sub_id,
            }
        )
        if protocol == "trojan":
            client["password"] = shared_password
        else:
            client["id"] = shared_client_id
        return client

    def _find_matching_client_index(
        self,
        clients: List[Dict[str, Any]],
        *,
        email: str,
        shared_client_id: str,
        shared_password: str,
    ) -> Optional[int]:
        normalized_email = email.strip().lower()
        aliases = {item.lower() for item in self._base_email_aliases(email)}
        for index, item in enumerate(clients):
            item_email = str(item.get("email") or "").strip().lower()
            item_uuid = str(item.get("id") or item.get("clientId") or "").strip()
            item_password = str(item.get("password") or "").strip()
            if item_email and (item_email == normalized_email or item_email in aliases):
                return index
            if shared_client_id and item_uuid == shared_client_id:
                return index
            if shared_password and item_password == shared_password:
                return index
        return None

    async def _get_target_inbounds(self) -> List[Dict[str, Any]]:
        inbounds = await self.get_inbounds()
        if not inbounds or not inbounds.get("success"):
            return []
        target_inbound_ids = set(self._target_inbound_ids())
        if target_inbound_ids:
            return [
                i for i in inbounds.get("obj", [])
                if int(i.get("id") or 0) in target_inbound_ids
            ]
        return [i for i in inbounds.get("obj", []) if i.get("enable", False)]

    async def find_clients_by_base_email(self, base_email: str) -> List[Dict[str, Any]]:
        inbounds = await self.get_inbounds()
        if not inbounds or not inbounds.get("success"):
            return []

        result = []
        for inbound in inbounds.get("obj", []):
            inbound_id = int(inbound.get("id") or 0)
            for stat in inbound.get("clientStats", []) or []:
                email = stat.get("email", "")
                if self._matches_base_email_for_inbound(email, base_email, inbound_id):
                    stat["inboundId"] = inbound_id
                    result.append(stat)
        return result

    async def find_clients_full_by_email(self, base_email: str) -> List[Dict[str, Any]]:
        inbounds = await self.get_inbounds()
        if not inbounds or not inbounds.get("success"):
            return []

        result: List[Dict[str, Any]] = []
        for inbound in inbounds.get("obj", []):
            inbound_id = int(inbound.get("id") or 0)
            protocol = inbound.get("protocol", "").lower()
            client_stats = inbound.get("clientStats", []) or []
            clients = self._parse_inbound_clients(inbound)

            for stat in client_stats:
                email = stat.get("email", "") or ""
                if not self._matches_base_email_for_inbound(email, base_email, inbound_id):
                    continue

                client_id = None
                password = None
                sub_id = None
                client_obj = None

                for c in clients:
                    c_email = c.get("email", "") or ""
                    if c_email == email:
                        client_id = c.get("id") or c.get("clientId")
                        password = c.get("password")
                        sub_id = c.get("subId")
                        client_obj = c
                        break

                item = dict(stat)
                item["inboundId"] = inbound_id
                item["clientId"] = client_id
                item["password"] = password
                item["subId"] = sub_id
                item["protocol"] = protocol
                item["clientObj"] = client_obj
                result.append(item)

        logger.info(f"Найдено {len(result)} клиентов по base_email='{base_email}'")
        return result

    async def _rollback_created_clients(self, base_email: str) -> None:
        try:
            deleted = await self.delete_client(base_email)
            if deleted:
                logger.warning("PanelAPI rollback removed partially created clients for %s", base_email)
            else:
                logger.warning("PanelAPI rollback found nothing to remove for %s", base_email)
        except Exception as exc:
            logger.error("PanelAPI rollback failed for %s: %s", base_email, exc)

    async def create_client(
        self,
        email: str,
        limit_ip: int,
        total_gb: int,
        days: int = 30,
        sub_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        enabled_inbounds = await self._get_target_inbounds()
        if not enabled_inbounds:
            logger.error("Нет включённых inbound для создания клиента")
            return None

        expiry_ms = int((time.time() + days * 86400) * 1000)
        total_bytes = int(total_gb * 1073741824)
        shared_sub_id = str((sub_id or "").strip() or f"user{random.randint(100000, 999999)}")
        shared_client_id = str(uuid.uuid4())
        shared_password = secrets.token_urlsafe(12)
        created_inbounds = []
        last_client = None

        for inbound in enabled_inbounds:
            inbound_id = int(inbound.get("id") or 0)
            inbound_email = self._email_for_inbound(email, inbound_id)
            protocol = str(inbound.get("protocol") or "").lower()
            settings, clients = self._normalize_inbound_settings(inbound)
            client = self._make_client_record(
                protocol,
                inbound_email,
                limit_ip,
                total_bytes,
                expiry_ms,
                shared_sub_id,
                shared_client_id=shared_client_id,
                shared_password=shared_password,
            )

            existing_index = self._find_matching_client_index(
                clients,
                email=inbound_email,
                shared_client_id=shared_client_id,
                shared_password=shared_password,
            )
            if existing_index is None:
                clients.append(client)
            else:
                clients[existing_index] = client
            settings["clients"] = clients
            if await self._update_inbound(inbound, settings):
                logger.info(
                    f"Клиент {email} успешно создан в inbound {inbound_id} ({protocol})"
                )
                created_inbounds.append(inbound_id)
                client["protocol"] = protocol
                client["created_inbounds"] = list(created_inbounds)
                last_client = client
            else:
                logger.error("Ошибка сохранения клиента %s в inbound %s", email, inbound_id)

        if created_inbounds and last_client:
            if len(created_inbounds) != len(enabled_inbounds):
                logger.warning(
                    "PanelAPI created client only on %s/%s inbounds for %s; rolling back partial state",
                    len(created_inbounds),
                    len(enabled_inbounds),
                    email,
                )
                await self._rollback_created_clients(email)
                return None
            return last_client
        return None

    async def upsert_client(
        self,
        *,
        email: str,
        limit_ip: int,
        total_gb: int,
        days: int = 30,
        sub_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        await self.ensure_auth()
        target_inbounds = await self._get_target_inbounds()
        if not target_inbounds:
            logger.error("Нет включённых inbound для upsert клиента")
            return None

        existing_clients = await self.find_clients_full_by_email(email)
        existing_by_inbound = {int(item.get("inboundId") or 0): item for item in existing_clients if int(item.get("inboundId") or 0) > 0}

        expiry_ms = int((time.time() + days * 86400) * 1000)
        total_bytes = int(total_gb * 1073741824)
        representative = existing_clients[0] if existing_clients else {}
        shared_client_id = str(representative.get("clientId") or uuid.uuid4())
        shared_password = str(representative.get("password") or secrets.token_urlsafe(12))
        shared_sub_id = str((sub_id or "").strip() or representative.get("subId") or f"user{random.randint(100000, 999999)}")
        updated_inbounds: List[int] = []
        last_client: Optional[Dict[str, Any]] = None

        for inbound in target_inbounds:
            inbound_id = int(inbound.get("id") or 0)
            inbound_email = self._email_for_inbound(email, inbound_id)
            protocol = str(inbound.get("protocol") or "").lower()
            existing = existing_by_inbound.get(inbound_id)
            settings, clients = self._normalize_inbound_settings(inbound)
            source = existing.get("clientObj") if existing and isinstance(existing.get("clientObj"), dict) else None
            client = self._make_client_record(
                protocol,
                inbound_email,
                limit_ip,
                total_bytes,
                expiry_ms,
                shared_sub_id,
                shared_client_id=shared_client_id,
                shared_password=shared_password,
                source=source,
            )
            existing_index = self._find_matching_client_index(
                clients,
                email=inbound_email,
                shared_client_id=shared_client_id,
                shared_password=shared_password,
            )
            if existing_index is None:
                clients.append(client)
            else:
                clients[existing_index] = client
            settings["clients"] = clients
            if await self._update_inbound(inbound, settings):
                updated_inbounds.append(inbound_id)
                client["protocol"] = protocol
                client["created_inbounds"] = list(updated_inbounds)
                last_client = client
                continue
            logger.error("Ошибка upsert клиента %s в inbound %s", email, inbound_id)
            return None

        return last_client

    async def delete_client(self, base_email: str) -> bool:
        await self.ensure_auth()
        clients = await self.find_clients_full_by_email(base_email)

        if not clients:
            logger.info(
                f"Клиенты с частью email '{base_email}' не найдены, ничего не удаляем"
            )
            return True

        success_count = 0

        for c in clients:
            inbound_id = c.get("inboundId")
            client_id = c.get("clientId")
            password = c.get("password")
            protocol = c.get("protocol", "").lower()
            email = c.get("email", "")

            if not inbound_id:
                logger.error(f"Пропускаем клиента email={email}: нет inboundId")
                continue

            if protocol == "trojan":
                delete_id = password
            else:
                delete_id = client_id

            if not delete_id:
                logger.error(f"Пропускаем клиента email={email}: нет delete_id")
                continue

            delete_url = (
                f"{self.apibase}/panel/api/inbounds/{inbound_id}/delClient/{delete_id}"
            )
            status, data, text = await self._request_json_with_reauth(
                "POST", delete_url, headers=self._headers()
            )

            if status == 200 and data.get("success"):
                logger.info(
                    f"Клиент email={email} (inboundId={inbound_id}, protocol={protocol}) успешно удалён"
                )
                success_count += 1
            else:
                logger.error(
                    f"Ошибка удаления клиента email={email} inbound={inbound_id}: status={status} msg={data.get('msg')}"
                )
                if text:
                    logger.error(text)

        return success_count > 0

    async def extend_client_expiry(self, base_email: str, add_days: int) -> bool:
        await self.ensure_auth()
        clients = await self.find_clients_full_by_email(base_email)
        if not clients:
            return False

        success = False
        target_inbounds = await self.get_inbounds()
        if not target_inbounds or not target_inbounds.get("success"):
            return False
        inbound_map = {
            int(item.get("id") or 0): item
            for item in target_inbounds.get("obj", [])
            if int(item.get("id") or 0) > 0
        }
        for c in clients:
            inbound_id = c.get("inboundId")
            client_obj = c.get("clientObj")
            if not inbound_id or not isinstance(client_obj, dict):
                continue
            inbound = inbound_map.get(int(inbound_id))
            if not inbound:
                continue

            current_expiry = c.get("expiryTime", 0) or 0
            if current_expiry and current_expiry > 0:
                new_expiry = int(current_expiry + add_days * 86400 * 1000)
            else:
                new_expiry = int((time.time() + add_days * 86400) * 1000)

            settings, inbound_clients = self._normalize_inbound_settings(inbound)
            shared_client_id = str(client_obj.get("id") or c.get("clientId") or "")
            shared_password = str(client_obj.get("password") or c.get("password") or "")
            existing_index = self._find_matching_client_index(
                inbound_clients,
                email=str(client_obj.get("email") or ""),
                shared_client_id=shared_client_id,
                shared_password=shared_password,
            )
            if existing_index is None:
                continue
            updated_client = dict(inbound_clients[existing_index])
            updated_client["expiryTime"] = new_expiry
            inbound_clients[existing_index] = updated_client
            settings["clients"] = inbound_clients

            if await self._update_inbound(inbound, settings):
                success = True
            else:
                logger.error("Ошибка продления клиента %s в inbound %s", base_email, inbound_id)

        return success

    async def get_client_stats(self, base_email: str) -> List[Dict[str, Any]]:
        return await self.find_clients_by_base_email(base_email)
