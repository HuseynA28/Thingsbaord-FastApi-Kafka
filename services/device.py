import httpx
from urllib.parse import urljoin
from fastapi import HTTPException
import datetime
import warnings
import polars as pl
from config import BASE_URL, DATAFRAME_OUTPUT_PATH

async def get_device_details(
    customer_id: str,
    token: str,
    page_size: int,
    page: int,
    active: bool,
    include_customers: bool,
    as_child: bool,
    save_dataframe: bool
):
    """
    Fetch device details. If `as_child` is False, fetch devices for the given customer.
    If True, retrieve the parent customer and then fetch the devices.
    Optionally save the result as a CSV file.
    """
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient() as client:
        try:
            if not as_child:
                url = urljoin(BASE_URL, f"/api/customer/{customer_id}/deviceInfos")
                params = {
                    "pageSize": page_size,
                    "page": page,
                    "includeCustomers": include_customers,
                    "active": active
                }
                print("Hello from here")
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
              
                if not data["data"]:
                    raise HTTPException(status_code=404, detail="Device not found.Change the parms")
                
                try:
                    devices_df = pl.DataFrame(data["data"])
                    devices_df = devices_df.with_columns(
                        pl.col("id").struct.field("id").alias("id"),
                        pl.col("customerId").struct.field("id").alias("customerId")
                    ).select(["name", "id", "type", "label", "customerId"])
                    
                    if save_dataframe:
                        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        file_path = f"{DATAFRAME_OUTPUT_PATH}{customer_id}-{now}.csv"
                        devices_df.write_csv(file_path)
                        return f"Data saved to {file_path}"
                    
                    return devices_df.to_dicts()
                except Exception as exc:
                    warnings.warn("Failed to process device data. Check parameters and data format.")
                    raise HTTPException(status_code=500, detail="Error processing device data") from exc
            
            else:
                
                # Fetch parent customer info
                url = urljoin(BASE_URL, f"/api/customer/info/{customer_id}/")
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                name = data.get("name", "Unknown Customer")
                parent_customer = data.get("parentCustomerId")
                
                if parent_customer and "id" in parent_customer:
                    parent_customer_id = parent_customer["id"]
                    
                    # Fetch devices for the parent customer
                    url = urljoin(BASE_URL, f"/api/customer/{parent_customer_id}/deviceInfos")
                    params = {
                        "pageSize": page_size,
                        "page": page,
                        "includeCustomers": include_customers,
                        "active": active
                    }
                    response = await client.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    if not data:
                        raise HTTPException(status_code=404, detail="Device not found")
                    
                    try:
                        devices_df = pl.DataFrame(data["data"])
                        devices_df = devices_df.with_columns(
                            pl.col("id").struct.field("id").alias("id"),
                            pl.col("customerId").struct.field("id").alias("customerId")
                        ).select(["name", "id", "type", "label", "customerId"])
                        
                        if save_dataframe:
                            now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                            file_path = f"{DATAFRAME_OUTPUT_PATH}{customer_id}-{now}.csv"
                            devices_df.write_csv(file_path)
                            return f"Data saved to {file_path}"
                        
                        return devices_df.to_dicts()
                    except Exception as exc:
                        warnings.warn("Failed to process device data for parent customer.")
                        raise HTTPException(status_code=500, detail="Error processing device data for parent") from exc
                else:
                    return f'The customer "{name}" does not have a parent customer.'
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code, 
                detail="HTTP error occurred while fetching device details"
            ) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
