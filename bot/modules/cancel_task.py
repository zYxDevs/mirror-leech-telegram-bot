from asyncio import sleep
from pyrogram.filters import command, regex
from pyrogram.handlers import MessageHandler, CallbackQueryHandler

from bot import task_dict, bot, task_dict_lock, OWNER_ID, user_data, multi_tags
from ..helper.ext_utils.bot_utils import handler_new_task
from ..helper.ext_utils.status_utils import (
    get_task_by_gid,
    get_all_tasks,
    MirrorStatus,
)
from ..helper.telegram_helper import button_build
from ..helper.telegram_helper.bot_commands import BotCommands
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.telegram_helper.message_utils import (
    send_message,
    auto_delete_message,
    delete_message,
    edit_message,
)


@handler_new_task
async def cancel_task(_, message):
    user_id = message.from_user.id if message.from_user else message.sender_chat.id
    msg = message.text.split()
    if len(msg) > 1:
        gid = msg[1]
        if len(gid) == 4:
            multi_tags.discard(gid)
            return
        else:
            task = await get_task_by_gid(gid)
            if task is None:
                await send_message(message, f"GID: <code>{gid}</code> Not Found.")
                return
    elif reply_to_id := message.reply_to_message_id:
        async with task_dict_lock:
            task = task_dict.get(reply_to_id)
        if task is None:
            await send_message(message, "This is not an active task!")
            return
    elif len(msg) == 1:
        msg = (
            "Reply to an active Command message which was used to start the download"
            f" or send <code>/{BotCommands.CancelTaskCommand[0]} GID</code> to cancel it!"
        )
        await send_message(message, msg)
        return
    if (
        OWNER_ID != user_id
        and task.listener.user_id != user_id
        and (user_id not in user_data or not user_data[user_id].get("is_sudo"))
    ):
        await send_message(message, "This task is not for you!")
        return
    obj = task.task()
    await obj.cancel_task()


@handler_new_task
async def cancel_multi(_, query):
    data = query.data.split()
    user_id = query.from_user.id
    if user_id != int(data[1]) and not await CustomFilters.sudo("", query):
        await query.answer("Not Yours!", show_alert=True)
        return
    tag = int(data[2])
    if tag in multi_tags:
        multi_tags.discard(int(data[2]))
        msg = "Stopped!"
    else:
        msg = "Already Stopped/Finished!"
    await query.answer(msg, show_alert=True)
    await delete_message(query.message)


async def cancel_all(status, userId):
    matches = await get_all_tasks(status.strip(), userId)
    if not matches:
        return False
    for task in matches:
        obj = task.task()
        await obj.cancel_task()
        await sleep(2)
    return True


def create_cancel_buttons(isSudo, userId=""):
    buttons = button_build.ButtonMaker()
    buttons.data_button(
        "Downloading", f"canall ms {MirrorStatus.STATUS_DOWNLOADING} {userId}"
    )
    buttons.data_button(
        "Uploading", f"canall ms {MirrorStatus.STATUS_UPLOADING} {userId}"
    )
    buttons.data_button("Seeding", f"canall ms {MirrorStatus.STATUS_SEEDING} {userId}")
    buttons.data_button(
        "Spltting", f"canall ms {MirrorStatus.STATUS_SPLITTING} {userId}"
    )
    buttons.data_button("Cloning", f"canall ms {MirrorStatus.STATUS_CLONING} {userId}")
    buttons.data_button(
        "Extracting", f"canall ms {MirrorStatus.STATUS_EXTRACTING} {userId}"
    )
    buttons.data_button(
        "Archiving", f"canall ms {MirrorStatus.STATUS_ARCHIVING} {userId}"
    )
    buttons.data_button("QueuedDl", f"canall ms {MirrorStatus.STATUS_QUEUEDL} {userId}")
    buttons.data_button("QueuedUp", f"canall ms {MirrorStatus.STATUS_QUEUEUP} {userId}")
    buttons.data_button(
        "SampleVideo", f"canall ms {MirrorStatus.STATUS_SAMVID} {userId}"
    )
    buttons.data_button(
        "ConvertMedia", f"canall ms {MirrorStatus.STATUS_CONVERTING} {userId}"
    )
    buttons.data_button("Paused", f"canall ms {MirrorStatus.STATUS_PAUSED} {userId}")
    buttons.data_button("All", f"canall ms All {userId}")
    if isSudo:
        if userId:
            buttons.data_button("All Added Tasks", f"canall bot ms {userId}")
        else:
            buttons.data_button("My Tasks", f"canall user ms {userId}")
    buttons.data_button("Close", f"canall close ms {userId}")
    return buttons.build_menu(2)


@handler_new_task
async def cancell_all_buttons(_, message):
    async with task_dict_lock:
        count = len(task_dict)
    if count == 0:
        await send_message(message, "No active tasks!")
        return
    isSudo = await CustomFilters.sudo("", message)
    button = create_cancel_buttons(isSudo, message.from_user.id)
    can_msg = await send_message(message, "Choose tasks to cancel!", button)
    await auto_delete_message(message, can_msg)


@handler_new_task
async def cancel_all_update(_, query):
    data = query.data.split()
    message = query.message
    reply_to = message.reply_to_message
    userId = int(data[3]) if len(data) > 3 else ""
    isSudo = await CustomFilters.sudo("", query)
    if not isSudo and userId and userId != query.from_user.id:
        await query.answer("Not Yours!", show_alert=True)
    else:
        await query.answer()
    if data[1] == "close":
        await delete_message(reply_to)
        await delete_message(message)
    elif data[1] == "back":
        button = create_cancel_buttons(isSudo, userId)
        await edit_message(message, "Choose tasks to cancel!", button)
    elif data[1] == "bot":
        button = create_cancel_buttons(isSudo, "")
        await edit_message(message, "Choose tasks to cancel!", button)
    elif data[1] == "user":
        button = create_cancel_buttons(isSudo, query.from_user.id)
        await edit_message(message, "Choose tasks to cancel!", button)
    elif data[1] == "ms":
        buttons = button_build.ButtonMaker()
        buttons.data_button("Yes!", f"canall {data[2]} confirm {userId}")
        buttons.data_button("Back", f"canall back confirm {userId}")
        buttons.data_button("Close", f"canall close confirm {userId}")
        button = buttons.build_menu(2)
        await edit_message(
            message, f"Are you sure you want to cancel all {data[2]} tasks", button
        )
    else:
        button = create_cancel_buttons(isSudo, userId)
        await edit_message(message, "Choose tasks to cancel.", button)
        res = await cancel_all(data[1], userId)
        if not res:
            await send_message(reply_to, f"No matching tasks for {data[1]}!")


bot.add_handler(
    MessageHandler(
        cancel_task,
        filters=command(BotCommands.CancelTaskCommand, case_sensitive=True)
        & CustomFilters.authorized,
    )
)
bot.add_handler(
    MessageHandler(
        cancell_all_buttons,
        filters=command(BotCommands.CancelAllCommand, case_sensitive=True)
        & CustomFilters.authorized,
    )
)
bot.add_handler(CallbackQueryHandler(cancel_all_update, filters=regex("^canall")))
bot.add_handler(CallbackQueryHandler(cancel_multi, filters=regex("^stopm")))
