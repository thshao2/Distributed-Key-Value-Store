from fastapi import FastAPI
from packages.gossip import Gossip
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Runs the gossip protocol in the background."""
    async with Gossip.gossip():
        yield

app = FastAPI(lifespan=lifespan)

# Import and register routers (equivalent to Flask's blueprints)

from routers.ping import ping_router # /ping endpoint
app.include_router(ping_router)

from routers.view import view_router # /view endpoints
app.include_router(view_router)

from routers.get_data import get_data_router # GET endpoints for /data and /data/<key>
app.include_router(get_data_router)

from routers.put_data import put_data_router # PUT endpoints for /data/<key>
app.include_router(put_data_router)

from routers.update import update_data_router # Internal endpoints for relaying PUT
app.include_router(update_data_router)

# Entry point for the app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)
