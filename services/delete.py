import httpx
from urllib.parse import urljoin, urlencode
import polars as pl
import asyncio
import aiofiles
from io import StringIO
import json
import jmespath
import copy

# Configuration
from config import BASE_URL  # Ensure this is defined

json_cache={}

async def get_json(type_):
    if type_ not is json_cache:
        async with aiofiles.open(f'/root/main-kafka-main/Temps/{type_}.json', 'r') as file_json:
            json_cache[type_] = json.loads(await file_json.read())
    return json_cache[type_]

