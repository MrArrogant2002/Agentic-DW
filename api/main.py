from fastapi import FastAPI

from api.routes import router

app = FastAPI(
    title="Autonomous SQL Agent API",
    version="0.1.0",
    description="Phase 4 baseline: planner + SQL generator + safe executor + evaluator",
)
app.include_router(router)

