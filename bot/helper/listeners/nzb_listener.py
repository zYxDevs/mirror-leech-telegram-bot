from asyncio import sleep, gather

from bot import (
    intervals,
    sabnzbd_client,
    nzb_jobs,
    nzb_listener_lock,
    task_dict_lock,
    LOGGER,
)
from ..ext_utils.bot_utils import new_task
from ..ext_utils.status_utils import get_task_by_gid
from ..ext_utils.task_manager import stop_duplicate_check


async def _remove_job(nzo_id, mid):
    res1, _ = await gather(
        sabnzbd_client.delete_history(nzo_id, delete_files=True),
        sabnzbd_client.delete_category(f"{mid}"),
    )
    if not res1:
        await sabnzbd_client.delete_job(nzo_id, True)
    async with nzb_listener_lock:
        if nzo_id in nzb_jobs:
            del nzb_jobs[nzo_id]


@new_task
async def _on_download_error(err, nzo_id, button=None):
    task = await get_task_by_gid(nzo_id)
    LOGGER.info(f"Cancelling Download: {task.name()}")
    await gather(
        task.listener.on_download_error(err, button),
        _remove_job(nzo_id, task.listener.mid),
    )


@new_task
async def _change_status(nzo_id, status):
    task = await get_task_by_gid(nzo_id)
    async with task_dict_lock:
        task.cstatus = status


@new_task
async def _stop_duplicate(nzo_id):
    task = await get_task_by_gid(nzo_id)
    await task.update()
    task.listener.name = task.name()
    msg, button = await stop_duplicate_check(task.listener)
    if msg:
        _on_download_error(msg, nzo_id, button)


@new_task
async def _on_download_complete(nzo_id):
    task = await get_task_by_gid(nzo_id)
    await task.listener.on_download_complete()
    if intervals["stopAll"]:
        return
    await _remove_job(nzo_id, task.listener.mid)


@new_task
async def _nzb_listener():
    while not intervals["stopAll"]:
        async with nzb_listener_lock:
            try:
                jobs = (await sabnzbd_client.get_history())["history"]["slots"]
                downloads = (await sabnzbd_client.get_downloads())["queue"]["slots"]
                if len(nzb_jobs) == 0:
                    intervals["nzb"] = ""
                    break
                for job in jobs:
                    nzo_id = job["nzo_id"]
                    if nzo_id not in nzb_jobs:
                        continue
                    if job["status"] == "Completed":
                        if not nzb_jobs[nzo_id]["uploaded"]:
                            nzb_jobs[nzo_id]["uploaded"] = True
                            _on_download_complete(nzo_id)
                            nzb_jobs[nzo_id]["status"] = "Completed"
                    elif job["status"] == "Failed":
                        _on_download_error(job["fail_message"], nzo_id)
                    elif job["status"] in [
                        "QuickCheck",
                        "Verifying",
                        "Repairing",
                        "Fetching",
                        "Moving",
                        "Extracting",
                    ]:
                        if job["status"] != nzb_jobs[nzo_id]["status"]:
                            _change_status(nzo_id, job["status"])
                for dl in downloads:
                    nzo_id = dl["nzo_id"]
                    if nzo_id not in nzb_jobs:
                        continue
                    if (
                        dl["status"] == "Downloading"
                        and not nzb_jobs[nzo_id]["stop_dup_check"]
                        and not dl["filename"].startswith("Trying")
                    ):
                        nzb_jobs[nzo_id]["stop_dup_check"] = True
                        _stop_duplicate(nzo_id)
            except Exception as e:
                LOGGER.error(str(e))
        await sleep(3)


async def on_download_start(nzo_id):
    async with nzb_listener_lock:
        nzb_jobs[nzo_id] = {
            "uploaded": False,
            "stop_dup_check": False,
            "status": "Downloading",
        }
        if not intervals["nzb"]:
            intervals["nzb"] = _nzb_listener()
