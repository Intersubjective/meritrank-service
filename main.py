import uvicorn

from meritrank_service.asgi import create_meritrank_app

uvicorn.run(create_meritrank_app(), host="0.0.0.0", port=8000)
