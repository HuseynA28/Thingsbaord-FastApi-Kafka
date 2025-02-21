import httpx
from urllib.parse import urljoin
from fastapi import HTTPException
from config import BASE_URL

async def get_customer_info(customer_id: str, token: str) -> str:
    """
    Retrieve customer information and determine if the customer has a parent.
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = urljoin(BASE_URL, f"/api/customer/info/{customer_id}/")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            name = data.get("name", "Unknown Customer")
            parent_customer = data.get("parentCustomerId")
            
            if parent_customer and "id" in parent_customer:
                return f'The customer "{name}" is a child of the customer with ID {parent_customer["id"]}.'
            else:
                return f'The customer "{name}" does not have a parent customer.'
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code, 
                detail="Failed to fetch customer info"
            ) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
