import logging
from datetime import datetime
from aiogram import types
from database import db
from config import PACKET_INTERVAL
from services.packet_service import PacketService
from handlers.user_handlers import user_last_message_time

async def handle_forwarded(message: types.Message, packet_service: PacketService):
    user_id = message.from_user.id
    current_time = datetime.now()
    db_user_id = db.get_user(user_id, message.from_user.username)

    is_own_message = False
    forwarded_id = 0
    forwarded_name = "Unknown"

    if message.forward_from:
        forwarded_id = message.forward_from.id
        forwarded_name = message.forward_from.full_name
        is_own_message = (message.forward_from.id == user_id)
    elif message.forward_from_chat:
        forwarded_id = message.forward_from_chat.id
        forwarded_name = message.forward_from_chat.title
    elif message.forward_sender_name:
        forwarded_name = message.forward_sender_name

    logging.info(f"📨 Получено пересланное сообщение в {current_time.strftime('%H:%M:%S.%f')}")
    logging.info(f"   Отправитель: {forwarded_name}, своё: {is_own_message}")

    is_new_packet = False
    package_id = None

    if user_id in user_last_message_time:
        last_info = user_last_message_time[user_id]
        time_diff = (current_time - last_info['last_time']).total_seconds()
        logging.info(f"   С последнего сообщения прошло: {time_diff:.3f} сек")

        if time_diff <= PACKET_INTERVAL:
            package_id = last_info['package_id']
            logging.info(f"   ✅ Сообщение в пакет #{package_id}")
        else:
            logging.info(f"   ❌ Интервал превышен, нужен новый пакет")
            is_new_packet = True
    else:
        is_new_packet = True

    if is_new_packet:
        package_id = db.create_package(db_user_id)
        logging.info(f"   🆕 Создан новый пакет #{package_id}")

    user_last_message_time[user_id] = {
        'last_time': current_time,
        'package_id': package_id,
        'notification_sent': False
    }

    message_type, file_id, message_text = packet_service.process_message_type(message)

    db.add_message(
        package_id=package_id,
        forwarded_from_id=forwarded_id,
        forwarded_from_name=forwarded_name,
        is_own_message=is_own_message,
        message_text=message_text,
        message_type=message_type,
        file_id=file_id
    )
    logging.info(f"💾 Сообщение сохранено в пакет #{package_id}")