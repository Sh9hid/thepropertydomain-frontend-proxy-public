from typing import get_args, get_origin


def test_route_dependency_aliases_exist_and_are_annotated():
    from api.routes._deps import APIKeyDep, SessionDep

    api_key_args = get_args(APIKeyDep)
    session_args = get_args(SessionDep)

    assert get_origin(APIKeyDep) is not None
    assert get_origin(SessionDep) is not None
    assert api_key_args[0] is str
    assert session_args[0].__name__ == "AsyncSession"
