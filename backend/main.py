import os

import core.config as core_config
from runtime.app import app, create_web_app
from services.lead_service import save_lead

__all__ = ["app", "create_web_app", "core_config", "save_lead"]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "runtime.app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", os.getenv("BACKEND_PORT", "8001"))),
        reload=False,
        log_level="info",
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
