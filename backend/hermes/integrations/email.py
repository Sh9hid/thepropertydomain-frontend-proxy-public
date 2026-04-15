from __future__ import annotations

import os
from typing import Dict


class EmailIntegration:
    def is_ready(self) -> bool:
        return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USERNAME") and os.getenv("SMTP_PASSWORD"))

    def prepare_draft(self, *, subject: str, message: str) -> Dict[str, str | bool]:
        return {
            "subject": subject,
            "message": message,
            "ready": self.is_ready(),
            "mode": "draft_only",
        }


email_integration = EmailIntegration()
