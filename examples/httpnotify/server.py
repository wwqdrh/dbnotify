import asyncio

from fastapi import FastAPI, Body
import uvicorn
import httpx
from pydantic import BaseModel


app = FastAPI()


class Item(BaseModel):
    table: str
    payload: dict


@app.post("/cb")
async def cb(item: Item):
    print(item)


@app.on_event("startup")
async def startup():
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(
            "http://localhost:8000/callback",
            json={
                "table": "notes",
                "url": "http://localhost:9000/cb",
            },
        )


async def main():
    uvicorn.run(app="server:app", host="0.0.0.0", port=9000, reload=True)


if __name__ == "__main__":
    asyncio.run(main())
