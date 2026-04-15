from __future__ import annotations

import os
from typing import Dict


class SocialIntegration:
    def is_ready(self) -> bool:
        return bool(os.getenv("HERMES_SOCIAL_PUBLISH_ENABLED", "").lower() in {"1", "true", "yes"})

    def prepare_post(self, *, body: str, channel: str) -> Dict[str, str | bool]:
        return {
            "body": body,
            "channel": channel,
            "ready": self.is_ready(),
            "mode": "approval_gated",
        }


social_integration = SocialIntegration()
