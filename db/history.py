import os
import sqlite3

from loguru import logger
from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str = Field(..., description="The role of the sender of the message.")
    content: str = Field(..., description="The message content.")
    created: str = Field(..., description="The timestamp of the message creation.")


class ChatHistory:
    def __init__(self, db_path: str = "data/chat_history.db"):
        self.__db_path = db_path
        # create parent directory if it doesn't exist
        os.makedirs(os.path.dirname(self.__db_path), exist_ok=True)

        self.__conn = sqlite3.connect(self.__db_path)
        self.__cursor = self.__conn.cursor()

        self.__init_table()
        self.__create_index()

    def __init_table(self):
        logger.debug("Initializing chat_history table")
        self.__cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_history (
                chat_id INTEGER NOT NULL ,
                created TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL
            )
        """
        )
        self.__conn.commit()

    def __create_index(self):
        logger.debug("Initializing chat_id index")
        self.__cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS user_index ON chat_history (chat_id)
        """
        )
        self.__conn.commit()

    def add_message(self, chat_id: str, message: ChatMessage):
        self.__cursor.execute(
            """
            INSERT INTO chat_history (chat_id, role, content, created)
            VALUES (?, ?, ?, ?)
        """,
            (chat_id, message.role, message.content, message.created),
        )
        self.__conn.commit()

    def get_messages(self, chat_id: str) -> list[ChatMessage]:
        """
        Get all messages for a given chat_id, in order of creation (increasing).
        """
        self.__cursor.execute(
            """
            SELECT role, content, created
            FROM chat_history
            WHERE chat_id = ?
            ORDER BY created
        """,
            (chat_id,),
        )
        return [
            ChatMessage(role=role, content=content, created=created)
            for role, content, created in self.__cursor.fetchall()
        ]

    def close(self):
        self.__conn.close()


if __name__ == "__main__":
    ch = ChatHistory()
    ch.add_message(
        1, ChatMessage(role="user", content="Hello!", created="2021-09-01 12:00:00")
    )
    ch.add_message(
        1, ChatMessage(role="bot", content="Hi!", created="2021-09-01 12:00:01")
    )

    messages = ch.get_messages(1)
    for message in messages:
        logger.debug(f"Message: {message}")

    ch.close()
