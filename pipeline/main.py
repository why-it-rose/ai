from fastapi import FastAPI

from api.v1 import news
import uvicorn

app = FastAPI(title="why-it-rose crawler api")

app.include_router(news.router, prefix="/api/v1")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
