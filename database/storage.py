from datetime import datetime, timedelta
import io
from sqlite3 import Connection, connect
from typing import Dict

from tabulate import tabulate

from tasks.config import tz
from tasks.loader import sender


class Database:
    db: Connection
    table_data = {}
    last_query = None

    def __init__(self, mem_db, name):
        """Create a new Database object."""
        self.db = mem_db
        self.name = name

        self.tables = [
            table[0] for table in self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall()
        ]

        self.update_time = datetime.now(tz=tz)

    def unload(self):
        self.db.close()

    def get_query(self, query):
        self.update_time = datetime.now(tz=tz)
        execution = self.db.execute(query)
        description = [row[0] for row in execution.description]

        self.last_query = [
            list(row) for row in execution.fetchall()
        ], description

        return self.last_query

    def execute_query(self, query):
        self.update_time = datetime.now(tz=tz)
        self.db.execute(query)
        self.db.commit()

    def get_table(self, table):
        table_data = self.table_data.get(table)
        if not table_data:
            table_data = self.get_query(f"SELECT * FROM {table}")
            self.table_data[table] = table_data

        return self.tabulate_result(*table_data)

    def tabulate_result(self, tab_list, headers):
        width, height = len(tab_list), len(headers)
        for row in range(width):
            for cell in range(height):
                if tab_list[row][cell] is None:
                    tab_list[row][cell] = ""

        return tabulate(
            tab_list,
            headers=headers,
            tablefmt="grid",
            maxcolwidths=14,
        )

    def get_buttons(self):
        buttons = [
            [table, f"table_{i}"] for i, table in enumerate(self.tables)
        ]
        buttons += [
            [sender.text("get_sqlite"), "get_sqlite"],
            [sender.text("get_excel"), "get_excel"],
            [sender.text("get_csv"), "get_csv"],
            [sender.text("all_tables"), "back"],
        ]

        return buttons

    def get_sqlite(self):
        """Return a binary copy of the SQLite database."""
        buffer = io.BytesIO()
        with connect(":memory:") as mem_db:
            self.db.backup(mem_db)

            if hasattr(mem_db, "serialize"):
                buffer.write(mem_db.serialize())
            else:
                mem_db.backup(
                    connect("file:memdb1?mode=memory&cache=shared", uri=True),
                )
                for line in mem_db.iterdump():
                    buffer.write(f"{line}\n".encode())

        buffer.seek(0)

        return buffer.getvalue()

    def __str__(self):
        return sender.text("db_data", self.name, ", ".join(self.tables))


def clear_databases(now):
    for user_id, db in databases.items():
        if now - db.update_time > timedelta(minutes=30):
            db.unload()
            del databases[user_id]


databases: Dict[int, Database] = {}
