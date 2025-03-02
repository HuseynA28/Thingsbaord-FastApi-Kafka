from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
import httpx
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import asyncio

from auth import oauth2_scheme
from services.customer import get_customer_info
from services.device import get_device_details
from services.telemerty import fetch_telemetry_from_device
from services.telemerty_test_copy import fetch_telemetry_from_device_test

from services.chooseRandomDevice import   RandomDevice
from config import BASE_URL
from typing import Dict
from services.get_all_devices import getAllDevices
app = FastAPI()
page_size=10000000,
page=0

class TokenResponse(BaseModel):
    access_token: str
    token_type: str

async def get_token(form_data: OAuth2PasswordRequestForm) -> TokenResponse:
    """
    Authenticate user and retrieve an access token.
    """
    login_url = f"{BASE_URL}/api/auth/login"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                login_url,
                json={"username": form_data.username, "password": form_data.password}
            )
            response.raise_for_status()
            data = response.json()
            token = data.get("token")
            if not token:
                raise HTTPException(status_code=400, detail="Token not found in response")
            return TokenResponse(access_token=token, token_type="bearer")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code, detail="Login failed"
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.post("/token", response_model=TokenResponse, include_in_schema=False)
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    return await get_token(form_data)

@app.get("/checkConnection")
async def check_connection(token: Annotated[str, Depends(oauth2_scheme)]):
    return {"message": "You have accessed a protected route!", "token": token}

@app.get("/getCustomerInfo/")
async def get_customer_info_endpoint(
    token: Annotated[str, Depends(oauth2_scheme)],
    customer_id: str = Query(..., description="Customer id (e.g., 784f394c-42b6-435a-983c-b7beff2784f9)")
):
    return await get_customer_info(customer_id=customer_id, token=token)

@app.get("/getCustomerDetails/")
async def get_customer_details_endpoint(
    token: Annotated[str, Depends(oauth2_scheme)],
    customer_id: str = Query(..., description="Customer id (e.g., 784f394c-42b6-435a-983c-b7beff2784f9)"),
    page_size: int = Query(1000000, description="Maximum number of entities on one page (default: 1000000)"),
    page: int = Query(0, description="Page number starting from 0"),
    include_customers: bool = Query(True, description="Include customers means that it will act as a parent and retrieve all child devices as well."),
    active: bool = Query(False, description="Filter active devices"),
    as_child: bool = Query(False, description="Search as child means that it will return all child devices of the parent, but NOT the grandparent's devices."),
    save_dataframe: bool = Query(False, description="Save result as CSV")
):
    return await get_device_details(
        customer_id=customer_id,
        token=token,
        page_size=page_size,
        page=page,
        active=active,
        include_customers=include_customers,
        as_child=as_child,
        save_dataframe=save_dataframe
    )
    
@app.get("/GetAllDeviceInfo/")
async def get_devices(
    token: Annotated[str, Depends(oauth2_scheme)],
    pageSize: int = Query(100, description="..."),
    saveAsDataFrame: str = Query(..., description="...")
):
    return await getAllDevices(
        page_size=pageSize,
        token=token,
        dataFrameName=saveAsDataFrame
    )
    
@app.get("/ChooseRandomDevice/")
async def get_randomDevice(
    token: Annotated[str, Depends(oauth2_scheme)],
    FromDataFrame:str = Query(..., description="..."),
    deviceNumber: int = Query(100, description="..."),
    SaveAsDataFrame: str = Query(..., description="Give name for the dataframe that it will be saved in Random Devices folder "),
    getAll :bool = Query(False, description="If you have a lot of  the data it can take some time . We trust in God of paralization")
):
    return await RandomDevice(
        deviceNumber=deviceNumber,
        FromDataFrame=FromDataFrame,
        token=token,
        SaveAsDataFrame=SaveAsDataFrame,
        getAll=getAll
    )
    

@app.get("/Metamorphosis/")

async def send_message(
    token: Annotated[str, Depends(oauth2_scheme)],
    file_name: str = Query(..., description="Write the name of file  that  you saved. This  file is in Datasets folder and name of the file is temp.csv if you have not save one with different name "),
    useStrictDataTypes: bool = Query(False, description="Enables/disables conversion of telemetry values to strings. Set parameter to 'true' in order to disable the conversion."),
    ):
    return await fetch_telemetry_from_device_test(
    token=token,
    file_name=file_name,
    useStrictDataTypes=useStrictDataTypes

) 
    
# @app.get("/Metamorphosis/test")

# async def send_message(topic: str, message: str):
#     producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
#     await producer.start()
#     try:
#         await producer.send_and_wait(topic, message.encode('utf-8'))
#     finally:
#         await producer.stop()  # Ensure the producer is closed properly