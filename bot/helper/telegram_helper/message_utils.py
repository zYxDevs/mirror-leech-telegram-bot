from asyncio import sleep
from pyrogram.errors import FloodWait, FloodPremiumWait
from pyrogram.types import LinkPreviewOptions
from re import match as re_match
from time import time

from bot import config_dict, LOGGER, status_dict, task_dict_lock, intervals, bot, user
from ..ext_utils.bot_utils import SetInterval
from ..ext_utils.exceptions import TgLinkException
from ..ext_utils.status_utils import get_readable_message


async def send_message(message, text, buttons=None, block=True):
    try:
        return await message.reply(
            text=text,
            quote=True,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            disable_notification=True,
            reply_markup=buttons,
        )
    except FloodWait as f:
        LOGGER.warning(str(f))
        if block:
            await sleep(f.value * 1.2)
            return await send_message(message, text, buttons)
        return str(f)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def edit_message(message, text, buttons=None, block=True):
    try:
        await message.edit(
            text=text,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            reply_markup=buttons,
        )
    except FloodWait as f:
        LOGGER.warning(str(f))
        if block:
            await sleep(f.value * 1.2)
            return await edit_message(message, text, buttons)
        return str(f)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def send_file(message, file, caption=""):
    try:
        return await message.reply_document(
            document=file, quote=True, caption=caption, disable_notification=True
        )
    except FloodWait as f:
        LOGGER.warning(str(f))
        await sleep(f.value * 1.2)
        return await send_file(message, file, caption)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def send_rss(text):
    try:
        app = user or bot
        return await app.send_message(
            chat_id=config_dict["RSS_CHAT"],
            text=text,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            disable_notification=True,
        )
    except (FloodWait, FloodPremiumWait) as f:
        LOGGER.warning(str(f))
        await sleep(f.value * 1.2)
        return await send_rss(text)
    except Exception as e:
        LOGGER.error(str(e))
        return str(e)


async def delete_message(message):
    try:
        await message.delete()
    except Exception as e:
        LOGGER.error(str(e))


async def auto_delete_message(cmd_message=None, bot_message=None):
    await sleep(60)
    if cmd_message is not None:
        await delete_message(cmd_message)
    if bot_message is not None:
        await delete_message(bot_message)


async def delete_status():
    async with task_dict_lock:
        for key, data in list(status_dict.items()):
            try:
                await delete_message(data["message"])
                del status_dict[key]
            except Exception as e:
                LOGGER.error(str(e))


async def get_tg_link_message(link):
    message = None
    links = []
    if link.startswith("https://t.me/"):
        private = False
        msg = re_match(
            r"https:\/\/t\.me\/(?:c\/)?([^\/]+)(?:\/[^\/]+)?\/([0-9-]+)", link
        )
    else:
        private = True
        msg = re_match(
            r"tg:\/\/openmessage\?user_id=([0-9]+)&message_id=([0-9-]+)", link
        )
        if not user:
            raise TgLinkException("USER_SESSION_STRING required for this private link!")

    chat = msg[1]
    msg_id = msg[2]
    if "-" in msg_id:
        start_id, end_id = msg_id.split("-")
        msg_id = start_id = int(start_id)
        end_id = int(end_id)
        btw = end_id - start_id
        if private:
            link = link.split("&message_id=")[0]
            links.append(f"{link}&message_id={start_id}")
            for _ in range(btw):
                start_id += 1
                links.append(f"{link}&message_id={start_id}")
        else:
            link = link.rsplit("/", 1)[0]
            links.append(f"{link}/{start_id}")
            for _ in range(btw):
                start_id += 1
                links.append(f"{link}/{start_id}")
    else:
        msg_id = int(msg_id)

    if chat.isdigit():
        chat = int(chat) if private else int(f"-100{chat}")

    if not private:
        try:
            message = await bot.get_messages(chat_id=chat, message_ids=msg_id)
            if message.empty:
                private = True
        except Exception as e:
            private = True
            if not user:
                raise e

    if not private:
        return (links, "bot") if links else (message, "bot")
    elif user:
        try:
            user_message = await user.get_messages(chat_id=chat, message_ids=msg_id)
        except Exception as e:
            raise TgLinkException(
                f"You don't have access to this chat!. ERROR: {e}"
            ) from e
        if not user_message.empty:
            return (links, "user") if links else (user_message, "user")
    else:
        raise TgLinkException("Private: Please report!")


async def update_status_message(sid, force=False):
    if intervals["stopAll"]:
        return
    async with task_dict_lock:
        if not status_dict.get(sid):
            if obj := intervals["status"].get(sid):
                obj.cancel()
                del intervals["status"][sid]
            return
        if not force and time() - status_dict[sid]["time"] < 3:
            return
        status_dict[sid]["time"] = time()
        page_no = status_dict[sid]["page_no"]
        status = status_dict[sid]["status"]
        is_user = status_dict[sid]["is_user"]
        page_step = status_dict[sid]["page_step"]
        text, buttons = await get_readable_message(
            sid, is_user, page_no, status, page_step
        )
        if text is None:
            del status_dict[sid]
            if obj := intervals["status"].get(sid):
                obj.cancel()
                del intervals["status"][sid]
            return
        if text != status_dict[sid]["message"].text:
            message = await edit_message(
                status_dict[sid]["message"], text, buttons, block=False
            )
            if isinstance(message, str):
                if message.startswith("Telegram says: [400"):
                    del status_dict[sid]
                    if obj := intervals["status"].get(sid):
                        obj.cancel()
                        del intervals["status"][sid]
                else:
                    LOGGER.error(
                        f"Status with id: {sid} haven't been updated. Error: {message}"
                    )
                return
            status_dict[sid]["message"].text = text
            status_dict[sid]["time"] = time()


async def send_status_message(msg, user_id=0):
    if intervals["stopAll"]:
        return
    async with task_dict_lock:
        sid = user_id or msg.chat.id
        is_user = bool(user_id)
        if sid in list(status_dict.keys()):
            page_no = status_dict[sid]["page_no"]
            status = status_dict[sid]["status"]
            page_step = status_dict[sid]["page_step"]
            text, buttons = await get_readable_message(
                sid, is_user, page_no, status, page_step
            )
            if text is None:
                del status_dict[sid]
                if obj := intervals["status"].get(sid):
                    obj.cancel()
                    del intervals["status"][sid]
                return
            message = status_dict[sid]["message"]
            await delete_message(message)
            message = await send_message(msg, text, buttons, block=False)
            if isinstance(message, str):
                LOGGER.error(
                    f"Status with id: {sid} haven't been sent. Error: {message}"
                )
                return
            message.text = text
            status_dict[sid].update({"message": message, "time": time()})
        else:
            text, buttons = await get_readable_message(sid, is_user)
            if text is None:
                return
            message = await send_message(msg, text, buttons, block=False)
            if isinstance(message, str):
                LOGGER.error(
                    f"Status with id: {sid} haven't been sent. Error: {message}"
                )
                return
            message.text = text
            status_dict[sid] = {
                "message": message,
                "time": time(),
                "page_no": 1,
                "page_step": 1,
                "status": "All",
                "is_user": is_user,
            }
    if not intervals["status"].get(sid) and not is_user:
        intervals["status"][sid] = SetInterval(
            config_dict["STATUS_UPDATE_INTERVAL"], update_status_message, sid
        )
