"""Pipeline guard — status transition validation."""


def assert_status_transition_allowed(current_status, new_status: str = "", **kwargs) -> None:
    """Validate that a lead status transition is allowed.

    Currently permissive — allows all transitions. Add rules as business
    logic solidifies.  Accepts extra kwargs (source, appointment_at) for
    future rule enforcement without breaking callers.
    """
    pass
