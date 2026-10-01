"""Centralized redaction for reports, logs, console output, and exceptions."""

from __future__ import annotations

import re
import logging
from dataclasses import asdict, is_dataclass
from typing import Any, Iterable
from urllib.parse import quote, quote_plus

MASK = "***MASKED***"

SAFE_VISIBLE_FIELDS = {
    "source_server_name",
    "target_server_name",
    "source_database_name",
    "target_database_name",
    "server",
    "database",
}
_NORMALIZED_SAFE_VISIBLE_FIELDS = {
    re.sub(r"[^a-z0-9]", "", field_name.lower())
    for field_name in SAFE_VISIBLE_FIELDS
}

_SENSITIVE_KEY_PARTS = {
    "accesskey", "accountkey", "apikey", "clientid", "clientsecret",
    "connectionstring", "database", "endpoint", "host", "jdbcurl",
    "lakehouse", "password", "pwd", "refreshtoken", "sas", "secret",
    "server", "sqlendpoint", "token", "uid", "user", "username",
    "odbcconnect",
    "warehouse", "workspace",
}
_CONNECTION_PAIR = re.compile(
    r"(?i)\b(server|host|endpoint|database|initial catalog|uid|user(?:name)?|"
    r"pwd|password|token|secret|client[_ -]?id|client[_ -]?secret|api[_ -]?key)"
    r"\s*=\s*([^;\s]+)"
)
_URL_AUTHORITY = re.compile(
    r"(?i)\b((?:jdbc:)?(?:https?|sqlserver):"
    r"(?:/{0,2}))([^/;\s]+)"
)
_ODBC_CONNECT_QUERY = re.compile(
    r"(?i)(\bodbc_connect(?:=|%3d))[^&\s'\"<>()\[\]{}]+"
)
_PUBLIC_ERROR_HOSTS = frozenset({"sqlalche.me"})


def _mask_url_authority(match: re.Match) -> str:
    if match.group(2).lower() in _PUBLIC_ERROR_HOSTS:
        return match.group(0)
    return f"{match.group(1)}{MASK}"


def is_sensitive_key(key: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    if normalized in _NORMALIZED_SAFE_VISIBLE_FIELDS:
        return False
    if normalized.endswith("status"):
        return False
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def mask_text(value: Any, secrets: Iterable[Any] = ()) -> str:
    text = str(value)
    text = _ODBC_CONNECT_QUERY.sub(lambda m: f"{m.group(1)}{MASK}", text)
    text = _CONNECTION_PAIR.sub(lambda m: f"{m.group(1)}={MASK}", text)
    text = _URL_AUTHORITY.sub(_mask_url_authority, text)
    for secret in secrets:
        if secret is not None and len(str(secret)) >= 3:
            raw_secret = str(secret)
            encoded_secrets = {
                raw_secret,
                quote(raw_secret, safe=""),
                quote_plus(raw_secret, safe=""),
            }
            for encoded_secret in encoded_secrets:
                text = text.replace(encoded_secret, MASK)
    return text


def mask_data(value: Any, secrets: Iterable[Any] = ()) -> Any:
    """Recursively mask sensitive keys and secret values in arbitrary structures."""
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        return {
            key: MASK if is_sensitive_key(key) and item not in (None, "")
            else mask_data(item, secrets)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [mask_data(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(mask_data(item, secrets) for item in value)
    if isinstance(value, set):
        return {mask_data(item, secrets) for item in value}
    if isinstance(value, str):
        return mask_text(value, secrets)
    return value


def collect_sensitive_values(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if is_sensitive_key(key) and item not in (None, ""):
                found.add(str(item))
            found.update(collect_sensitive_values(item))
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            found.update(collect_sensitive_values(item))
    return found


class MaskingFilter(logging.Filter):
    """Redact log messages centrally before any handler emits them."""
    def __init__(self, secrets: Iterable[Any] = ()):
        super().__init__()
        self.secrets = tuple(secrets)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            # Never let logging's formatting diagnostics expose raw arguments.
            message = "[Log message formatting failed]"
        record.msg = mask_text(message, self.secrets)
        record.args = ()
        return True
