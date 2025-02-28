import httpx
from urllib.parse import urljoin
from fastapi import HTTPException
import datetime
import warnings
import polars as pl
from config import BASE_URL, DATAFRAME_ALL_DEVICES
devices_df=pl.DataFrame(schema=["name", "id", "type", "label", "customerId"])
totalPages=0
async def getAllDevices(
    page_size: int ,
    token:str, 
    page:int, 
    dataFrameName:str):
    header = {"Authorization":f"Bearer {token}"}
    file_path = f"{DATAFRAME_ALL_DEVICES}{dataFrameName}.csv" 
    devices_df.write_csv(file_path)  
    async with httpx.AsyncClient() as client:
        try:
            url_all_devices= urljoin(BASE_URL, f"/api/tenant/devices")
            params={
                "pageSize":page_size, 
                "page":page,
                
            }
            response=await client.get(url_all_devices, headers=header, params=params)
            response.raise_for_status()
            data=response.json()
            totalPages=data["totalPages"]
            return data["totalPages"]
        except:
            "hello"
            
    
    
    