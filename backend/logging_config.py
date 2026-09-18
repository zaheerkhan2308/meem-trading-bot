"""Shared, rotating logging for every backend launch mode."""
import functools
import inspect
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


_SENSITIVE_NAMES = {
    "password", "token", "secret", "api_key", "api_secret", "authorization",
}


def _safe_value(value: Any, name: str = "") -> str:
    """Keep call logging useful without exposing credentials or huge payloads."""
    if any(part in name.lower() for part in _SENSITIVE_NAMES):
        return "<redacted>"
    if isinstance(value, dict):
        rendered = "{" + ", ".join(
            f"{key!r}: {_safe_value(item, str(key))}"
            for key, item in value.items()
        ) + "}"
        return rendered if len(rendered) <= 2000 else f"{rendered[:1997]}..."
    if isinstance(value, (list, tuple, set)):
        value = type(value)(_safe_value(item) for item in value)
    rendered = repr(value)
    return rendered if len(rendered) <= 2000 else f"{rendered[:1997]}..."


def log_call(func):
    """Log bound arguments, return values, and raised exceptions for one call."""
    if getattr(func, "_meem_call_logged", False):
        return func
    call_logger = logging.getLogger(func.__module__)
    signature = inspect.signature(func)

    def _arguments(args: tuple, kwargs: dict) -> str:
        bound = signature.bind_partial(*args, **kwargs)
        return ", ".join(
            f"{name}={_safe_value(value, name)}"
            for name, value in bound.arguments.items()
        ) or "none"

    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            arguments = _arguments(args, kwargs)
            call_logger.info("CALL %s(%s)", func.__qualname__, arguments)
            try:
                response = await func(*args, **kwargs)
            except Exception:
                call_logger.error(
                    "ERROR %s(%s)", func.__qualname__, arguments, exc_info=True
                )
                raise
            call_logger.info(
                "RETURN %s -> %s", func.__qualname__, _safe_value(response)
            )
            return response
        wrapper = async_wrapper
    else:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            arguments = _arguments(args, kwargs)
            call_logger.info("CALL %s(%s)", func.__qualname__, arguments)
            try:
                response = func(*args, **kwargs)
            except Exception:
                call_logger.error(
                    "ERROR %s(%s)", func.__qualname__, arguments, exc_info=True
                )
                raise
            call_logger.info(
                "RETURN %s -> %s", func.__qualname__, _safe_value(response)
            )
            return response

    wrapper._meem_call_logged = True
    return wrapper


def instrument_module(namespace: dict[str, Any]) -> None:
    """Instrument functions and methods defined by one backend module."""
    module_name = namespace.get("__name__")
    for name, value in list(namespace.items()):
        if inspect.isfunction(value) and value.__module__ == module_name:
            namespace[name] = log_call(value)
        elif inspect.isclass(value) and value.__module__ == module_name:
            for method_name, method in list(vars(value).items()):
                if inspect.isfunction(method):
                    setattr(value, method_name, log_call(method))


def configure_logging() -> None:
    """Send all backend logs to logs/backend.log and the active console."""
    root = logging.getLogger()
    if getattr(root, "_meem_logging_configured", False):
        return
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()

    log_dir = Path(__file__).resolve().parents[1] / "logs"
    log_dir.mkdir(exist_ok=True)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = RotatingFileHandler(
        log_dir / "backend.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(console_handler)
    root._meem_logging_configured = True
