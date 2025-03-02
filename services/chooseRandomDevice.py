import polars as pl
import httpx
from urllib.parse import urljoin
import asyncio
import polars as pl
from fastapi import HTTPException
from config import BASE_URL, DATAFRAME_ALL_DEVICES, DATAFRAME_OUTPUT_PATH, DATAFRAME_RANDOM_DEVICES


async def get_keys(client: httpx.AsyncClient, header: dict, entityID:str):
    all_keys = urljoin(BASE_URL, f"/api/plugins/telemetry/DEVICE/{entityID}/keys/timeseries")
    response = await client.get(all_keys, headers=header)
    response.raise_for_status()
    data = response.json()
    one_device_keys = pl.DataFrame(data, schema=["keys"])
    return one_device_keys





async def RandomDevice(FromDataFrame: str, token:str, deviceNumber:int, SaveAsDataFrame, getAll:bool):
    if not getAll:
        file_path = f"{DATAFRAME_ALL_DEVICES}{FromDataFrame}.csv"
        df_devices = pl.read_csv(file_path, columns=["id"]).sample(n=deviceNumber, seed=42)
        header = {"Authorization": f"Bearer {token}"}
        random_path=f"{DATAFRAME_RANDOM_DEVICES}{SaveAsDataFrame}.csv"
        async with httpx.AsyncClient() as client:
            tasks = [
                    get_keys(client=client, header=header, entityID=entityID)  for entityID in df_devices["id"] 
                ]
            allKeys = await asyncio.gather(*tasks)
            devices_keys_df = pl.concat(allKeys)
            devices_keys_df.write_csv(random_path)
            return f'{devices_keys_df.shape[0]} number of keys are saved'

    else:
        file_path = f"{DATAFRAME_ALL_DEVICES}{FromDataFrame}.csv"
        df_devices = pl.read_csv(file_path, columns=["id"])
        header = {"Authorization": f"Bearer {token}"}
        random_path=f"{DATAFRAME_RANDOM_DEVICES}{SaveAsDataFrame}.csv"
        async with httpx.AsyncClient() as client:
            tasks = [
                    get_keys(client=client, header=header, entityID=entityID)  for entityID in df_devices["id"] 
                ]
            allKeys = await asyncio.gather(*tasks)
            devices_keys_df = pl.concat(allKeys)
            devices_keys_df.write_csv(random_path)
            return f' {devices_keys_df.shape[0]} number of keys are saved. You saved all of the keys from all of  the devices'
    
  

