# Domain: API
## Purpose: 
The entry point for all HTTP requests. 
## Rules:
- **No Business Logic:** API routes must only handle request parsing, response formatting, and error handling.
- **Service-First:** Every route must call a function in `backend/services/`.
- **Validation:** Use Pydantic models from `backend/models/` for all request/response validation.
- **Async Only:** All routes must be `async`.
