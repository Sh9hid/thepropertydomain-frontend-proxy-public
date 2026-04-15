# Domain: Models
## Purpose:
The "Type Layer" and "Source of Truth" for data structures.
## Rules:
- **Unified Schemas:** Every table in `leads.db` MUST have a corresponding Pydantic and SQLAlchemy/SQL model here.
- **Zero-Logic:** Models should define data structures, not behaviors. 
- **Validation-First:** Use Pydantic's `Field` and `@validator` to enforce data integrity before it hits the DB.
- **Enums:** Define all Lead States (e.g., `INITIAL_TOUCH`, `CALLBACK`) as Enums here.
