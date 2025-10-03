from sqlite3 import Connection
from tasks.loader import sender


databases = {}


class Database:
    db: Connection

    def __init__(self, mem_db, name):
        self.db = mem_db
        self.name = name

        self.tables = [
            table[0] for table in self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall()
        ]
    
    def __str__(self):
        return sender.text("db_data", self.name, ", ".join(self.tables))
