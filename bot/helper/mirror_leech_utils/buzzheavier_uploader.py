"""BuzzHeavier upload helper.

Used by mirror tasks when the ``-bh`` flag is supplied. Walks the
download directory, streams each file to BuzzHeavier with progress
callbacks that the existing status renderer can read, and finishes by
calling ``listener.on_upload_complete``.
"""

from __future__ import annotations

from logging import getLogger
from os import walk, path as ospath
from time import time
from typing import AsyncIterator

from aiofiles import open as aiopen
from httpx import AsyncClient, HTTPError, Limits, Timeout

from ...core.config_manager import Config


LOGGER = getLogger(__name__)

_UPLOAD_BASE = "https://w.buzzheavier.com"
_UPLOAD_CHUNK = 16 * 1024 * 1024  # 16 MiB read window
_HTTP_TIMEOUT = Timeout(connect=30.0, read=600.0, write=600.0, pool=30.0)


def _auth_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    account_id = (Config.BUZZHEAVIER_ACCOUNT_ID or "").strip()
    if account_id:
        headers["Authorization"] = f"Bearer {account_id}"
    return headers


class BuzzHeavierUploader:
    """Stream files in ``self._path`` to BuzzHeavier sequentially."""

    def __init__(self, listener, path: str):
        self._listener = listener
        self._path = path
        self._processed_bytes = 0
        self._start_time = time()
        self._last_speed_bytes = 0
        self._last_speed_at = self._start_time
        self._speed = 0.0
        self._files_dict: dict[str, str] = {}
        self._total_files = 0
        self._error: str = ""

    # ── status interface ─────────────────────────────────────────────

    @property
    def processed_bytes(self) -> int:
        return self._processed_bytes

    @property
    def speed(self) -> float:
        now = time()
        elapsed = now - self._last_speed_at
        if elapsed >= 1.0:
            delta = self._processed_bytes - self._last_speed_bytes
            self._speed = delta / elapsed if elapsed > 0 else 0.0
            self._last_speed_at = now
            self._last_speed_bytes = self._processed_bytes
        return self._speed

    # ── upload ────────────────────────────────────────────────────────

    async def _stream_file(self, file_path: str, file_size: int) -> AsyncIterator[bytes]:
        async with aiopen(file_path, "rb") as fh:
            while True:
                if self._listener.is_cancelled:
                    return
                chunk = await fh.read(_UPLOAD_CHUNK)
                if not chunk:
                    return
                self._processed_bytes += len(chunk)
                yield chunk

    async def _upload_one(self, client: AsyncClient, file_path: str) -> str:
        file_name = ospath.basename(file_path)
        file_size = ospath.getsize(file_path)
        url = f"{_UPLOAD_BASE}/{file_name}"
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(file_size),
            **_auth_headers(),
        }

        LOGGER.info(f"Uploading to BuzzHeavier: {file_name} ({file_size} bytes)")
        response = await client.put(
            url,
            content=self._stream_file(file_path, file_size),
            headers=headers,
        )
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"BuzzHeavier upload failed [{response.status_code}]: "
                f"{response.text[:200]}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"BuzzHeavier returned non-JSON response: {response.text[:200]}"
            ) from exc

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            raise RuntimeError("BuzzHeavier response missing 'data' object")
        file_id = (data.get("id") or "").strip()
        if not file_id:
            raise RuntimeError("BuzzHeavier response missing file id")
        return f"https://buzzheavier.com/{file_id}"

    async def upload(self) -> None:
        files: list[str] = []
        if ospath.isfile(self._path):
            files.append(self._path)
        else:
            for root, _, names in walk(self._path):
                for name in sorted(names):
                    candidate = ospath.join(root, name)
                    if ospath.isfile(candidate):
                        files.append(candidate)

        if not files:
            await self._listener.on_upload_error(
                "BuzzHeavier: no files were found to upload"
            )
            return

        self._total_files = len(files)
        first_link = ""

        try:
            async with AsyncClient(
                timeout=_HTTP_TIMEOUT,
                limits=Limits(max_connections=4, max_keepalive_connections=2),
            ) as client:
                for file_path in files:
                    if self._listener.is_cancelled:
                        await self._listener.on_upload_error(
                            "BuzzHeavier upload cancelled by user"
                        )
                        return
                    try:
                        link = await self._upload_one(client, file_path)
                    except (HTTPError, RuntimeError) as exc:
                        LOGGER.error(
                            f"BuzzHeavier upload error for "
                            f"{ospath.basename(file_path)}: {exc}"
                        )
                        self._error = str(exc)
                        await self._listener.on_upload_error(
                            f"BuzzHeavier: {exc}"
                        )
                        return
                    self._files_dict[link] = ospath.basename(file_path)
                    if not first_link:
                        first_link = link
        except Exception as exc:  # pragma: no cover - safety net
            LOGGER.error(f"BuzzHeavier session error: {exc}")
            await self._listener.on_upload_error(f"BuzzHeavier: {exc}")
            return

        if self._listener.is_cancelled:
            await self._listener.on_upload_error(
                "BuzzHeavier upload cancelled by user"
            )
            return

        LOGGER.info(
            f"BuzzHeavier upload completed: {self._total_files} file(s)"
        )
        # Mirror behaviour: ``link`` is the primary URL, ``files`` is a
        # link → name dict (for multi-file), ``folders``/``mime_type``
        # carry the totals expected by ``on_upload_complete``.
        await self._listener.on_upload_complete(
            first_link,
            self._files_dict,
            self._total_files,
            "BuzzHeavier",
        )
