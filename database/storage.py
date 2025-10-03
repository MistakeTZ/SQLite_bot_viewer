from sqlite3 import Connection
from tasks.loader import sender
from datetime import datetime, timedelta
from tasks.config import tz
from typing import Dict
from tabulate import tabulate


class Database:
    db: Connection
    table_data = {}

    def __init__(self, mem_db, name):
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

        return [list(row) for row in execution.fetchall()], description
    
    def execute_query(self, query):
        self.db.execute(query)
        self.db.commit()

    def get_table(self, table):
        table_data = self.table_data.get(table)
        if not table_data:
            table_data = self.get_query(f"SELECT * FROM {table}")
            self.table_data[table] = table_data

        return self.tabulate_result(*table_data)

    def tabulate_result(self, tab_list, headers):
        x, y = len(tab_list), len(headers)
        for row in range(x):
            for cell in range(y):
                if tab_list[row][cell] is None:
                    tab_list[row][cell] = ""

        return tabulate(tab_list, headers=headers, tablefmt="grid", maxcolwidths=14)

    def get_buttons(self):
        return [[table, f"table_{i}"] for i, table in enumerate(self.tables)]

    def __str__(self):
        return sender.text("db_data", self.name, ", ".join(self.tables))


def clear_databases(now):
    for user_id, db in databases.items():
        if now - db.update_time > timedelta(minutes=30):
            db.unload()
            del databases[user_id]


databases: Dict[int, Database] = {}
