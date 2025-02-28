import httpx
from urllib.parse import urljoin, urlencode
import polars as pl
import asyncio
import aiofiles
from io import StringIO
import json
import jmespath
import copy


from config import BASE_URL  

json_cache = {}

async def get_json(type_):
    """You have to edit also the code  that it look at  the device type_ nname 
    such as SEN5X  if exist do not add otherwise add it """
    if type_ not in json_cache:
        async with aiofiles.open(f'/root/main-kafka-main/Temps/{type_}.json', 'r') as file_json:
            json_cache[type_] = json.loads(await file_json.read())
            
    return json_cache[type_]

async def process_device(name, type_, device_id, token, useStrictDataTypes, client):
    """Process a single device's telemetry data and write it to a JSON file.
    If  you have different  type_ you add here the if condition we have just now we device type  SEN5X .
    we do not have  any problem there beacuse  in the dataframe we have  just one type device SEN5X , If you would have different it will 
    create a error beacuse the stucture is different . Pay attention  to Datasets/temp.csv
    """
    try:
        temp_file = copy.deepcopy(await get_json(type_))
        temp_file["polling"]["deviceid"] = name.split('-')[0]
        temp_file["polling"]["device"][0]["slaveadd"] = name.split('-')[1]
        device_keys = jmespath.search("polling.device[*].modbusmap[*]", temp_file)[0]
        keys_to_fetch = []
        for i in device_keys:
            if i["type"] == 0:
                key = i["addr"].split("x")[1] + "_" + "H"
            elif i["type"] == 1:
                key = i["addr"].split("x")[1] + "_" + "I"
            else:
                raise ValueError(f"Unknown type {i['type']}")
            keys_to_fetch.append(key)     
        telemetry_path = f'/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries'
        query_params = {'keys': ','.join(keys_to_fetch), "useStrictDataTypes": useStrictDataTypes}
        url_telemetry = f"{urljoin(BASE_URL, telemetry_path)}?{urlencode(query_params)}"
        response = await client.get(url_telemetry, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()
        telemetry_data = response.json() 
        for i, key in zip(device_keys, keys_to_fetch):
            i["val"] = telemetry_data.get(key, [{"value": None}])[0]["value"]
        async with aiofiles.open(f"LatestData/temp_file{device_id}.json", mode="w") as file:
            await file.write(json.dumps(temp_file, indent=4))
    except Exception as e:
        print(f"Error processing device {device_id}: {e}")

async def fetch_telemetry_from_device_test(file_name: str, useStrictDataTypes: bool, token: str):
    """Fetch and process telemetry data for all devices in the input CSV file."""
  
    async with aiofiles.open(f'/root/main-kafka-main/Datasets/{file_name}', 'r') as file:
        info_df = pl.read_csv(StringIO(await file.read()))
        
        

    async with httpx.AsyncClient() as client:
        tasks = [
            process_device(name, type_, device_id, token, useStrictDataTypes, client)
            for name, type_, device_id in info_df.select(["name", "type", "id"]).iter_rows()
        ]
        await asyncio.gather(*tasks)
    
    return {f"The data is saved  {json_cache}"}  



### if  the Thingsbaord is overwhelmed  the use  the code below 

# sem = asyncio.Semaphore(50)  
# async def limited_process(*args):
#     async with sem:
#         await process_device(*args)
# tasks = [limited_process(name, type_, device_id, token, useStrictDataTypes, client) for ...]