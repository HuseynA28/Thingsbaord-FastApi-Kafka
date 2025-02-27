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
import os
from dotenv import load_dotenv
import json
import polars as pl
import csv
# device_ids=pl.read_csv("/root/main-kafka-main/Datasets/temp.csv", has_header=False)
load_dotenv()
topic = os.getenv("TOPIC", "my_topic")
device_id_="1cbe6d30-f04b-11ef-b931-9dad38df1d1f"
entityId=2206
topic="my_topic"


async def fetch_telemetry_from_device(file_name:str, useStrictDataTypes:bool, token: str):  
    headers = {"Authorization": f"Bearer {token}"}
    with open(f'/root/main-kafka-main/Datasets/{file_name}', 'r') as file:
        device_ids = csv.reader(file)
        device_list=[]
        for device_id in device_ids:
            source_key_endpoint = f'/api/plugins/telemetry/DEVICE/{device_id[0]}/keys/timeseries'
            async with httpx.AsyncClient() as client:            
                url_keys = urljoin(BASE_URL, source_key_endpoint)
                response = await client.get(url_keys, headers=headers)
                response.raise_for_status()
                entityId = response.json()
                telemetry_path = f'/api/plugins/telemetry/DEVICE/{device_id[0]}/values/timeseries'
                query_params =  {
                    'telemetry_keys': entityId,
                    "useStrictDataTypes":useStrictDataTypes 
                }
                url_telemetry = urljoin(BASE_URL, telemetry_path)
                url_telemetry = f"{url_telemetry}?{urlencode(query_params)}"               
                response = await client.get(url_telemetry, headers=headers)
                response.raise_for_status()
                telemetry_data = response.json()
                device_list.append(device_id[0])
                producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
                await producer.start()
                try:
                    await producer.send_and_wait('my_topic', bytes(str(telemetry_data), 'utf-8'))

                finally:
                    await producer.stop() 
              
            
        return f"Telemerty from total {len(device_list)} devices were sent {telemetry_data}"
    del(device_ids) 



