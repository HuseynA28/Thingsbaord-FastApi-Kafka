import httpx
from urllib.parse import urljoin
from fastapi import HTTPException
import datetime
import warnings
import polars as pl
from config import BASE_URL, DATAFRAME_OUTPUT_PATH
customerName=[]
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
                    for customer_id in devices_df["customerId"]:
                        url_customer = urljoin(BASE_URL, f"/api/customer/{customer_id}/shortInfo")
                        response_customer_info = await client.get(url_customer, headers=headers, params=params)
                        response_customer_info.raise_for_status()
                        data_customer_info = response_customer_info.json()
                        customerName.append(data_customer_info["title"])
                        
                    devices_df = devices_df.with_columns(pl.Series("CustomerName", customerName))
                    customerName.clear()

                    
                    if save_dataframe:
                        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        file_path = f"{DATAFRAME_OUTPUT_PATH}{customer_id}-{now}.csv"
                        devices_df.write_csv(file_path)
                        return f"Data saved to {file_path} and there are total {devices_df.shape} information"
                    
                    return devices_df.to_dicts()
                except Exception as exc:
                    warnings.warn(f"The data part gives teh error {exc}")
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
                        for customer_id in devices_df["customerId"]:
                            url_customer = urljoin(BASE_URL, f"/api/customer/{customer_id}/shortInfo")
                            response_customer_info = await client.get(url_customer, headers=headers, params=params)
                            response_customer_info.raise_for_status()
                            data_customer_info = response_customer_info.json()
                            customerName.append(data_customer_info["title"])
                        devices_df = devices_df.with_columns(pl.Series("CustomerName", customerName))
                        customerName.clear()
                        
                        if save_dataframe:
                            now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                            file_path = f"{DATAFRAME_OUTPUT_PATH}{customer_id}-{now}.csv"
                            devices_df.write_csv(file_path)
                            return f"Data saved to {file_path} and there are total {devices_df.shape} information"

             
                        
                        return devices_df.to_dicts()
                    except Exception as exc:
                        warnings.warn(f"Failed to process device data for the parent customer. There is no data available. Check the raw data: {data['data']}. The reason could be that includeCustomers is set to False, preventing the parent from accessing devices from its children. If you choose asChild = True, ensure that includeCustomers is also set to True.")
                        raise HTTPException(status_code=500, detail=f"Failed to process device data for the parent customer. There is no data available. Check the raw data: {data['data']}. The reason could be that includeCustomers is set to False, preventing the parent from accessing devices from its children. If you choose asChild = True, ensure that includeCustomers is also set to True.") from exc
                else:
                    return f'The customer "{name}" does not have a parent customer.'
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code, 
                detail="HTTP error occurred while fetching device details"
            ) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
customerName.clear()
                        