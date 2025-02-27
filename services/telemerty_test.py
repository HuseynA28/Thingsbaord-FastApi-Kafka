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

import aiofiles
from io import StringIO

import jmespath
# device_ids=pl.read_csv("/root/main-kafka-main/Datasets/temp.csv", has_header=False)
load_dotenv()
topic = os.getenv("TOPIC", "my_topic")
device_id_="1cbe6d30-f04b-11ef-b931-9dad38df1d1f"
entityId=2206
topic="my_topic"
file_files=[]
file_files_dict={}

async def fetch_telemetry_from_device_test(file_name:str, useStrictDataTypes:bool, token: str):  
    headers = {"Authorization": f"Bearer {token}"}
    async with aiofiles.open(f'/root/main-kafka-main/Datasets/{file_name}', 'r') as file:
        content=await file.read()
        info_df = pl.read_csv(StringIO(content))
        for name, type , device_id in info_df.select(["name", "type", 'id']).iter_rows():
            async with aiofiles.open(f'/root/main-kafka-main/Temps/{type}.json', 'r') as file_json:
                file_json = await file_json.read()
                temp_file = json.loads(file_json)
               
                temp_file["polling"]["deviceid"] = name.split('-')[0]
                temp_file["polling"]["device"][0]["slaveadd"]= name.split('-')[1]
                device_keys= jmespath.search("polling.device[*].modbusmap[*]",temp_file)[0]
                for i in device_keys:
                    if i["type"]==0:  
                        key=i["addr"].split("x")[1]+"_"+"H"
                    elif i["type"]==1:
                        key=i["addr"].split("x")[1]+"_"+"I"
                    else:
                        return f"There is a type {i['type']} that we cannot find."

                    async with httpx.AsyncClient() as client: 
                        telemetry_path = f'/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries'
                        query_params =  {
                            'keys': key,
                            "useStrictDataTypes":useStrictDataTypes 
                        }
                        url_telemetry = urljoin(BASE_URL, telemetry_path)
                        url_telemetry = f"{url_telemetry}?{urlencode(query_params)}"               
                        response = await client.get(url_telemetry, headers=headers)
                        response.raise_for_status()
                        telemetry_data = response.json()
                        for keys , values in telemetry_data.items():
                            i["val"]=values[0]["value"]
                           
                    # async with aiofiles.open("data.json", mode="w") as file:
                    #     await file.write(json.dumps(temp_file, indent=4))
                            
                async with aiofiles.open(f"LatestData/temp_file{device_id}.json", mode="w") as file:
                        await file.write(json.dumps(temp_file, indent=4))
              
    return file_files_dict



