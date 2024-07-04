import multiprocessing as mp
from queue import Empty

from loguru import logger

from .history import ChatHistory, ChatMessage


def get_chat_messages(chat_id: str, lock: "mp.Lock") -> list[ChatMessage]:
    """
    Get all chat messages for a given chat_id.
    In a separate function to be able to run it in a separate thread.
    """
    logger.debug(f"Getting chat messages for chat_id: {chat_id}")
    with lock:
        chat_history = ChatHistory()
        return chat_history.get_messages(chat_id)


def insert_chat_message(chat_id: str, message: ChatMessage, lock: "mp.Lock"):
    """
    Insert a chat message into the chat history.
    In a separate function to be able to run it in a separate thread.
    """
    logger.debug(f"Inserting chat message: {message} for chat_id: {chat_id}")
    with lock:
        chat_history = ChatHistory()
        chat_history.add_message(chat_id, message)


def insertion_worker(queue: "mp.Queue", lock: "mp.Lock"):
    """
    Worker function to concatenate chunks and insert messages into the chat history.
    """
    tmp_dict = dict()

    while True:
        try:
            while x := queue.get(1):
                chat_id, chunk = x
                completion_id = chunk.id

                if completion_id not in tmp_dict:
                    tmp_dict[completion_id] = {
                        "role": "",
                        "content": "",
                        "created": "",
                        "chat_id": chat_id,
                    }

                # concatenate the chunk into the message

                if chunk.choices[0].delta.content:
                    tmp_dict[completion_id]["content"] += chunk.choices[0].delta.content
                if chunk.choices[0].delta.role:
                    tmp_dict[completion_id]["role"] = (
                        chunk.choices[0].delta.role or tmp_dict[completion_id]["role"]
                    )
                # use the latest created timestamp
                tmp_dict[completion_id]["created"] = str(chunk.created)

                if chunk.choices[0].finish_reason:
                    insert_chat_message(
                        tmp_dict[completion_id].pop("chat_id"),
                        ChatMessage.model_validate(tmp_dict.pop(completion_id)),
                        lock,
                    )
        except Empty:
            continue
