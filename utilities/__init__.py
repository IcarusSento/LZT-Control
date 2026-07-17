"""Local utility modules exposed by the LZT Control panel.

FastAPI is loaded lazily so data handlers remain independent from the web
application stack.
"""


def create_utilities_router(*args, **kwargs):
    from .router import create_utilities_router as factory

    return factory(*args, **kwargs)


__all__ = ["create_utilities_router"]
