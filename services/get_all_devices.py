   
    
import httpx
from urllib.parse import urljoin
import asyncio
import polars as pl
from fastapi import HTTPException
from config import BASE_URL, DATAFRAME_ALL_DEVICES

async def fetch_page(page: int, client: httpx.AsyncClient, url: str, header: dict, page_size: int):
    params = {"pageSize": page_size, "page": page}
    response = await client.get(url, headers=header, params=params)
    response.raise_for_status()
    data = response.json()
    devices_data = pl.DataFrame(data["data"])
    devices_data = devices_data.with_columns(
        pl.col("id").struct.field("id").alias("id"),
        pl.col("customerId").struct.field("id").alias("customerId")
    ).select(["name", "id", "type", "label", "customerId"])
    return devices_data

async def getAllDevices(page_size: int, token: str, dataFrameName: str):
    # Set up headers and file path
    header = {"Authorization": f"Bearer {token}"}
    file_path = f"{DATAFRAME_ALL_DEVICES}{dataFrameName}.csv"
    url_all_devices = urljoin(BASE_URL, "/api/tenant/devices")
    
    async with httpx.AsyncClient() as client:
        params = {"pageSize": page_size, "page": 0}
        response = await client.get(url_all_devices, headers=header, params=params)
        response.raise_for_status()
        data = response.json()
        total_pages = data["totalPages"]  
        total_elements = data["totalElements"]  
    
        first_page_data = pl.DataFrame(data["data"]).with_columns(
            pl.col("id").struct.field("id").alias("id"),
            pl.col("customerId").struct.field("id").alias("customerId")
        ).select(["name", "id", "type", "label", "customerId"])
        
        
        if total_pages > 1:
            tasks = [
                fetch_page(page, client, url_all_devices, header, page_size)
                for page in range(1, total_pages)
            ]
            remaining_pages_data = await asyncio.gather(*tasks)
            all_devices_dfs = [first_page_data] + remaining_pages_data
        else:
            all_devices_dfs = [first_page_data]
        devices_df = pl.concat(all_devices_dfs)
        devices_df.write_csv(file_path)

        return f"I saved total {total_elements} devices you can find it in {file_path}"
