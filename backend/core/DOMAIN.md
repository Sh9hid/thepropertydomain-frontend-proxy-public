# Domain: Core
## Purpose:
The "Base Layer" for the entire system.
## Rules:
- **State-Agnostic:** Core logic should handle low-level infrastructure (DB, Auth, Global Config).
- **Zero-Coupling:** Do not import from `services` or `api`. 
- **The Source of Truth:** `backend/core/database.py` is the **only** place where SQLite connections are managed.
- **Utils:** Only place general-purpose helpers (date formatting, phone normalization) here.
