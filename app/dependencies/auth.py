import httpx
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.core.config import settings

security = HTTPBearer()

async def get_current_business_id(
    credentials: HTTPAuthorizationCredentials = Security(security)
) -> int:
    token = credentials.credentials
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                settings.SAAS_AUTH_VALIDATION_URL,
                json={"token": token},
                timeout=5.0
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("valid"):
                raise HTTPException(status_code=401, detail="Invalid token")
            return int(data["business_id"])
        except httpx.HTTPError:
            raise HTTPException(status_code=401, detail="Auth service unavailable")
