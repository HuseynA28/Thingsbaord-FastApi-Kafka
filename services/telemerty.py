import httpx
from urllib.parse import urljoin ,urlencode
from fastapi import FastAPI, HTTPException, Depends, Query
from typing import Dict
import datetime
import warnings
import polars as pl
from config import BASE_URL, DATAFRAME_OUTPUT_PATH
import asyncio
from aiokafka import AIOKafkaProducer

device_id="1cbe6d30-f04b-11ef-b931-9dad38df1d1f"
entityId=2206
topic="my_topic"
async def fetch_telemetry_from_device(file_name:str, useStrictDataTypes:bool, token: str):  
    headers = {"Authorization": f"Bearer {token}"}
    source_key_endpoint = f'/api/plugins/telemetry/DEVICE/{device_id}/keys/timeseries'
    async with httpx.AsyncClient() as client:
        try:
            url_keys = urljoin(BASE_URL, source_key_endpoint)
            response = await client.get(url_keys, headers=headers)
            response.raise_for_status()
            key_names = response.json()
            # for entityId in key_names:
              
            telemetry_path = f'/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries'
            
            query_params =  {
                'telemetry_keys': entityId,
                "useStrictDataTypes":useStrictDataTypes 
            }
            url_telemetry = urljoin(BASE_URL, telemetry_path)
            url_telemetry = f"{url_telemetry}?{urlencode(query_params)}"
            response = await client.get(url_telemetry, headers=headers)
            response.raise_for_status()
            telemetry_data = response.json()
            producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
            await producer.start()
            try:
                # await producer.send_and_wait("my_topic", b"Hello Kafka!")
                # await producer.send_and_wait("my_topic", telemetry_data.encode("utf-8"))
                await producer.send_and_wait('my_topic', bytes(str(telemetry_data), 'utf-8'))


            finally:
                await producer.stop()  # Ensure the producer is closed properly
        except:
            print("Hello") 
        return telemetry_data
  

