"""Main project file"""

import asyncio
import datetime as dt
import json
import random
import sys
from multiprocessing import Lock, Process, Queue
from uuid import uuid4

import anyio
from fastapi import FastAPI
from loguru import logger
from sse_starlette.sse import EventSourceResponse
from starlette.responses import JSONResponse

from db.history import ChatMessage
from db.ops import get_chat_messages, insert_chat_message, insertion_worker
from llm import prompt_llm_async
from settings import Settings

# Set up logging
logger.remove()
logger.add(
    sys.stderr,
    level=Settings().LOG_LEVEL,
    colorize=True,
    enqueue=True,
)
# create a lock and a queue for chat history operations
chat_history_lock = Lock()
chat_message_queue = Queue(maxsize=1000)

# start the insertion worker
insertion_worker = Process(
    target=insertion_worker,
    kwargs={"queue": chat_message_queue, "lock": chat_history_lock},
)
try:
    insertion_worker.start()
    logger.debug("Started message insertion worker")
except:
    logger.exception("Failed to start insertion worker")


# create a FastAPI app instance
app = FastAPI()


@app.get("/stream-example")
async def stream_example():
    """
    Example route for streaming text to the client, one word/token at a time.
    """

    async def stream_tokens():
        """
        Placeholder implementation for token streaming. Try running this route as-is to better understand how to
        stream data using Server-Sent Events (SSEs) in FastAPI.
        See this tutorial for more information: https://devdojo.com/bobbyiliev/how-to-use-server-sent-events-sse-with-fastapi
        """
        for token in ["hello", ", ", "this ", "is ", "a ", "streamed ", "response."]:
            # fake delay:
            await asyncio.sleep(random.randint(0, 3))

            logger.debug(f"Yielding token: {token}")
            yield token

    return EventSourceResponse(stream_tokens())


# Your code/routes here (you may also keep code in separate files and import/it them here):


@app.post("/chat", response_class=JSONResponse)
async def chat(message: str, chat_id: str | None = None):

    async def stream_tokens(
        user_message_content: str, existing_messages: list, chat_id: str
    ):
        user_message_insert_flag = False
        stream = await prompt_llm_async(user_message_content, existing_messages)
        async for chunk in stream:
            if not user_message_insert_flag:
                # insert the user message into the chat history
                await anyio.to_thread.run_sync(
                    insert_chat_message,
                    chat_id,
                    ChatMessage(
                        content=message,
                        role="user",
                        created=str(
                            int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
                        ),
                    ),
                    chat_history_lock,
                )
                user_message_insert_flag = True

            logger.trace(f"Yielding token: {chunk}")
            chat_message_queue.put((chat_id, chunk))
            try:
                chunk_dict = chunk.model_dump()
                chunk_dict["chat_id"] = chat_id
                yield json.dumps(chunk_dict)
            except:
                logger.exception("Failed to encode chunk")

    # retrieve chat history
    hist_messages = []
    if chat_id:
        # load chat history
        hist_messages = await anyio.to_thread.run_sync(
            get_chat_messages, chat_id, chat_history_lock
        )
    else:
        # generate a new chat_id
        chat_id = str(uuid4())

    # insert the new message into the chat history
    return EventSourceResponse(stream_tokens(message, hist_messages, chat_id))


@app.post("/chat-history")
async def get_chat_history(chat_id: str) -> list[ChatMessage]:
    """
    Get all chat messages for a given chat_id.
    """

    try:
        messages = await anyio.to_thread.run_sync(
            get_chat_messages, chat_id, chat_history_lock
        )
    except:
        logger.exception("Failed to get chat history")

    return messages
