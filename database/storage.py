from sqlite3 import Connection
from tasks.loader import sender
from datetime import datetime, timedelta
from tasks.config import tz
from typing import Dict


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
        final_list = [description] + execution.fetchall()

        return [list(item) for item in final_list]

    def get_table(self, table):
        table_data = self.table_data.get(table)
        if not table_data:
            table_data = self.get_query(f"SELECT * FROM {table}")
            self.table_data[table] = table_data

        x, y = len(table_data), len(table_data[0])
        for row in range(x):
            for cell in range(y):
                if table_data[row][cell] is None:
                    table_data[row][cell] = ""
                else:
                    table_data[row][cell] = str(table_data[row][cell])
        
        columns = [0] * y
        rows = [0] * x
        max_size = 60 // y

        for i in range(x):
            for j in range(y):
                columns[j] = min(max_size, max(columns[j], len(str(table_data[i][j]))))

        for i in range(x):
            for j in range(y):
                rows[i] = max(rows[i], (
                    len(str(table_data[i][j])) + (columns[j] - 1)
                ) // columns[j])

        table = []
        line_sep = "+"

        for i in range(y):
            line_sep += "-" * columns[i] + "+"
        line_sep += "\n"
        table_length = len(line_sep)

        for i in range(x):
            table.append(line_sep)

            for j in range(rows[i]):
                table.append("|")

                for k in range(y):
                    string = table_data[i][k][j * columns[k]:((j + 1) * columns[k])]
                    table[-1] += f"{string:<{columns[k]}}|\n"

                    if len(line_sep) + len(table[-1]) + table_length > 4000:
                        return "".join(table[:-1])
            
            table_length += len(table[-1]) + len(line_sep)

        table.append(line_sep)

        return "".join(table)

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
