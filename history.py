import os
import sqlite3
from multiprocessing import Lock

from loguru import logger


class ChatHistory:
    def __init__(self, db_path: str = "data/chat_history.db"):
        self.__db_path = db_path
        # create parent directory if it doesn't exist
        os.makedirs(os.path.dirname(self.__db_path), exist_ok=True)

        self.__lock = Lock()  # lock for thread safety
        self.__conn = sqlite3.connect(self.__db_path)
        self.__cursor = self.__conn.cursor()

        with self.__lock:
            self.__init_table()
            self.__create_index()

    def __init_table(self):
        logger.debug("Creating chat_history table")
        self.__cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                created TEXT NOT NULL,
                role TEXT NOT NULL,
                message TEXT NOT NULL
            )
        """
        )
        self.__conn.commit()

    def __create_index(self):
        logger.debug("Creating chat_id index")
        self.__cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS user_index ON chat_history (chat_id)
        """
        )
        self.__conn.commit()

    def add_message(self, chat_id: str, role: str, message: str, created: str):
        with self.__lock:
            self.__cursor.execute(
                """
                INSERT INTO chat_history (chat_id, role, message, created)
                VALUES (?, ?, ?, ?)
            """,
                (chat_id, role, message, created),
            )
            self.__conn.commit()

    def get_messages(self, chat_id: str) -> list[tuple[str, str, str]]:
        """
        Get all messages for a given chat_id, in order of creation (increasing).
        """
        with self.__lock:
            self.__cursor.execute(
                """
                SELECT role, message, created
                FROM chat_history
                WHERE chat_id = ?
                ORDER BY created
            """,
                (chat_id,),
            )
            return self.__cursor.fetchall()

    def close(self):
        self.__conn.close()


if __name__ == "__main__":
    ch = ChatHistory()
    ch.add_message(1, "user", "Hello, world!", "2022-01-01 12:00:00")
    ch.add_message(1, "bot", "Hi there!", "2022-01-01 12:00:01")
    ch.add_message(1, "user", "How are you?", "2022-01-01 12:00:02")
    ch.add_message(1, "bot", "I'm good, thanks!", "2022-01-01 12:00:03")

    messages = ch.get_messages(1)
    for role, message, created in messages:
        print(f"{role!r}: {message!r} ({created!r})")

    ch.close()
