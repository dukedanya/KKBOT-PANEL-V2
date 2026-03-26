import sys
import types
import tempfile
import unittest
import importlib
import sqlite3


fake_aiosqlite = types.ModuleType("aiosqlite")

class CursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor
        self.rowcount = cursor.rowcount
        self.lastrowid = cursor.lastrowid

    async def fetchone(self):
        return self._cursor.fetchone()

    async def fetchall(self):
        return self._cursor.fetchall()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class ConnectionWrapper:
    def __init__(self, path):
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row

    @property
    def row_factory(self):
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._conn.row_factory = value

    async def execute(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        return CursorWrapper(cur)

    async def commit(self):
        self._conn.commit()

    async def close(self):
        self._conn.close()


async def _connect(path):
    return ConnectionWrapper(path)


fake_aiosqlite.Connection = ConnectionWrapper
fake_aiosqlite.Row = sqlite3.Row
fake_aiosqlite.connect = _connect
sys.modules["aiosqlite"] = fake_aiosqlite

import db.database as db_module
importlib.reload(db_module)
Database = db_module.Database


class WithdrawRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_pending_request_returns_same_request_id(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            db = Database(tmp.name)
            await db.connect()
            await db.add_user(1)

            first = await db.create_withdraw_request(1, 500.0)
            second = await db.create_withdraw_request(1, 500.0)
            history = await db.get_withdraw_requests_by_user(1, limit=10)

            self.assertEqual(first, second)
            self.assertEqual(len(history), 1)

            await db.close()


if __name__ == "__main__":
    unittest.main()
