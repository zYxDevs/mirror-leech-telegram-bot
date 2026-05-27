"""AllDebrid filehost resolver.

Used by mirror/leech tasks when the ``-ad`` flag is supplied. Calls the
AllDebrid v4.1 API to unlock filehost links (1fichier, rapidgator,
mega, etc.) and either returns a single direct URL or a multi-file
``dict`` payload that the existing ``add_direct_download`` flow can
consume.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from httpx import AsyncClient, HTTPError

from bot import LOGGER
from bot.core.config_manager import Config
from bot.helper.ext_utils.exceptions import DirectDownloadLinkException


_API_BASE = "https://api.alldebrid.com/v4.1"
_API_BASE_V4 = "https://api.alldebrid.com/v4"
_AGENT = "mltb"
_TIMEOUT = 30.0
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Map a subset of AllDebrid error codes to user-friendly messages.
_FRIENDLY_ERRORS = {
    "AUTH_BAD_APIKEY": "ALLDEBRID_API_KEY is invalid",
    "AUTH_BLOCKED": "AllDebrid account is blocked",
    "AUTH_USER_BANNED": "AllDebrid account is banned",
    "LINK_HOST_NOT_SUPPORTED": "host is not supported by AllDebrid",
    "LINK_HOST_LIMIT_REACHED": "AllDebrid daily limit reached for this host",
    "LINK_HOST_UNAVAILABLE": "host is temporarily unavailable on AllDebrid",
    "LINK_DOWN": "the file is no longer available",
    "LINK_PASS_PROTECTED": "password-protected links are not supported",
    "LINK_TEMPORARY_UNAVAILABLE": "the link is temporarily unavailable",
    "LINK_NOT_SUPPORTED": "this link is not supported by AllDebrid",
}


def _api_error_message(error: dict[str, Any], link: str) -> str:
    code = (error.get("code") or "UNKNOWN").strip()
    message = error.get("message") or "Unknown AllDebrid error"
    friendly = _FRIENDLY_ERRORS.get(code, message)
    return f"AllDebrid: {friendly} ({code}) for {link}"


async def _call_api(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Make a single AllDebrid API call and return the inner ``data`` dict.

    Raises ``DirectDownloadLinkException`` on HTTP/JSON/business errors so
    the existing ``mirror_leech`` flow can surface a friendly message.
    """
    headers = {"User-Agent": _USER_AGENT}
    try:
        async with AsyncClient(timeout=_TIMEOUT, headers=headers) as client:
            request_kwargs: dict[str, Any] = {"params": params or {}}
            if data is not None:
                request_kwargs["data"] = data
            response = await client.request(method, url, **request_kwargs)
            response.raise_for_status()
            payload = response.json()
    except HTTPError as exc:
        raise DirectDownloadLinkException(
            f"ERROR: AllDebrid network error: {exc}"
        ) from exc
    except ValueError as exc:
        raise DirectDownloadLinkException(
            f"ERROR: AllDebrid returned malformed JSON: {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise DirectDownloadLinkException(
            "ERROR: AllDebrid returned an unexpected payload shape"
        )

    if payload.get("status") != "success":
        error = payload.get("error") or {}
        raise DirectDownloadLinkException(
            f"ERROR: {_api_error_message(error, params.get('link', '') if params else '')}"
        )

    inner = payload.get("data")
    if not isinstance(inner, dict):
        raise DirectDownloadLinkException(
            "ERROR: AllDebrid response missing 'data' object"
        )
    return inner


def _ensure_api_key() -> str:
    api_key = (Config.ALLDEBRID_API_KEY or "").strip()
    if not api_key:
        raise DirectDownloadLinkException(
            "ERROR: ALLDEBRID_API_KEY is not configured"
        )
    return api_key


def _basename_from_url(link: str) -> str:
    parsed = urlparse(link)
    name = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    return name or "file"


async def alldebrid_resolve(link: str) -> str | dict[str, Any]:
    """Resolve a filehost link via AllDebrid.

    Returns either:
        - ``str``: a single unrestricted CDN URL.
        - ``dict``: a multi-file payload compatible with the existing
          ``add_direct_download`` flow (``{"contents": [...],
          "title": str, "total_size": int}``).

    Raises ``DirectDownloadLinkException`` when the API rejects the link
    or the configuration is incomplete.
    """
    api_key = _ensure_api_key()
    params = {"agent": _AGENT, "apikey": api_key, "link": link}

    data = await _call_api("GET", f"{_API_BASE_V4}/link/unlock", params=params)

    direct = data.get("link")
    filename = data.get("filename") or _basename_from_url(link)
    filesize = int(data.get("filesize") or 0)
    streams = data.get("streams") or []

    if isinstance(direct, str) and direct:
        LOGGER.info(f"AllDebrid unlocked {link[:80]} -> {direct[:80]}...")
        return direct

    # Some hosts (notably mega folders) return a list of children
    # rather than a single ``link``. Fall back to ``infos`` to enumerate.
    contents: list[dict[str, Any]] = []
    if isinstance(streams, list) and streams:
        for entry in streams:
            stream_url = entry.get("link") or entry.get("url")
            if not stream_url:
                continue
            contents.append(
                {
                    "filename": entry.get("filename") or filename,
                    "path": entry.get("filename") or filename,
                    "url": stream_url,
                    "size": int(entry.get("filesize") or 0),
                    "headers": {},
                }
            )
        if contents:
            return {
                "contents": contents,
                "title": filename,
                "total_size": filesize or sum(c["size"] for c in contents),
            }

    raise DirectDownloadLinkException(
        f"ERROR: AllDebrid did not return a usable download link for {link}"
    )


async def alldebrid_check_supported(link: str) -> bool:
    """Best-effort host support probe.

    Used by callers that want to decide whether to try AllDebrid before
    falling back to the generic direct-link generator. A failure here is
    not fatal; we return ``False`` and let the caller try the fallback
    chain.
    """
    try:
        api_key = _ensure_api_key()
    except DirectDownloadLinkException:
        return False

    try:
        data = await _call_api(
            "GET",
            f"{_API_BASE}/user/hosts",
            params={"agent": _AGENT, "apikey": api_key},
        )
    except DirectDownloadLinkException:
        return False

    domain = (urlparse(link).netloc or "").lower()
    if domain.startswith("www."):
        domain = domain[4:]
    if not domain:
        return False

    hosts = data.get("hosts") or {}
    if not isinstance(hosts, dict):
        return False

    for host_info in hosts.values():
        if not isinstance(host_info, dict) or not host_info.get("status"):
            continue
        for host_domain in host_info.get("domains") or []:
            host_domain = str(host_domain).lower()
            if domain == host_domain or domain.endswith("." + host_domain):
                return True
    return False
