from fastapi import FastAPI

from routes.access_stats_ref import router as access_stats_ref_router

app = FastAPI()

app.include_router(access_stats_ref_router)