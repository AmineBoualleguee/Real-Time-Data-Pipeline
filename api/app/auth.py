import os

from fastapi import Header, HTTPException, status

API_KEY = os.getenv("API_KEY", "")


async def require_api_key(x_api_key: str = Header(default="")):
    """Gate a route behind X-API-Key. No-op if API_KEY is unset (local dev)."""
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
