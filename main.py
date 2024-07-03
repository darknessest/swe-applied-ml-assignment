"""Main project file"""

import asyncio
import random

from fastapi import FastAPI

from sse_starlette.sse import EventSourceResponse

# see llm.py to better understand the interaction with OpenAI's Chat Completion service.
from llm import prompt_llm_async

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
        for token in ['hello', ', ', 'this ', 'is ', 'a ', 'streamed ', 'response.']:
            # fake delay:
            await asyncio.sleep(random.randint(0, 3))

            print(f"Yielding token: {token}")
            yield token

    return EventSourceResponse(stream_tokens())

# Your code/routes here (you may also keep code in separate files and import/it them here):

