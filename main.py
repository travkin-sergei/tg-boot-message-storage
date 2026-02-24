import asyncio
import logging
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command

from config import BOT_TOKEN, PACKET_INTERVAL, ADMIN_IDS
from database import db
from services.packet_service import PacketService
from handlers import user_handlers, admin_handlers, message_handlers

# Инициализация
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
packet_service = PacketService()

# Привязываем глобальные объекты к модулям-хендлерам
user_handlers.bot = bot
user_handlers.packet_service = packet_service
admin_handlers.bot = bot
admin_handlers.packet_service = packet_service

# Регистрация обработчиков команд (без лямбд)
dp.message.register(user_handlers.cmd_start, Command("start"))
dp.message.register(user_handlers.cmd_stats, Command("stats"))
dp.message.register(user_handlers.cmd_packets, Command("packets"))
dp.message.register(user_handlers.cmd_new_packet, Command("new_packet"))
dp.message.register(user_handlers.cmd_get_packet, Command("get_packet"))

dp.message.register(admin_handlers.cmd_admin_packet, Command("ap"))
dp.message.register(admin_handlers.cmd_admin_user, Command("auser"))
dp.message.register(admin_handlers.cmd_admin_search, Command("asearch"))
dp.callback_query.register(admin_handlers.callback_admin_packet, F.data.startswith('admin_packet_'))


# Общий обработчик (пересланные сообщения) – должен быть после всех команд
@dp.message()
async def handle_all_messages(message):
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        return
    is_forwarded = (
            message.forward_from is not None or
            message.forward_from_chat is not None or
            message.forward_sender_name is not None or
            message.forward_date is not None
    )
    if is_forwarded:
        await message_handlers.handle_forwarded(message, packet_service)


# Фоновая задача для закрытия пакетов
async def packet_closer():
    while True:
        await asyncio.sleep(0.5)
        current_time = datetime.now()
        users_to_close = []
        for user_id, info in list(user_handlers.user_last_message_time.items()):
            if info.get('notification_sent', False):
                continue
            time_diff = (current_time - info['last_time']).total_seconds()
            if time_diff > PACKET_INTERVAL:
                users_to_close.append((user_id, info['package_id']))
        for user_id, package_id in users_to_close:
            await packet_service.send_packet_summary(bot, user_id, package_id)
            if user_id in user_handlers.user_last_message_time:
                user_handlers.user_last_message_time[user_id]['notification_sent'] = True
            logging.info(f"🔒 Закрыт пакет #{package_id} для пользователя {user_id}")


async def main():
    asyncio.create_task(packet_closer())
    logging.info("🚀 Бот запущен и готов к работе!")
    logging.info(f"⏱️ Интервал пакетирования: {PACKET_INTERVAL} секунд")
    logging.info("📨 Уведомления отправляются ТОЛЬКО при закрытии пакета")
    if ADMIN_IDS:
        logging.info(f"👑 Администраторы: {', '.join(map(str, ADMIN_IDS))}")
        logging.info("📝 Доступные админ-команды: /ap, /auser, /asearch")
    else:
        logging.info("⚠️ Администраторы не настроены. Добавьте ADMIN_IDS в .env файл")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        if db.conn:
            db.conn.close()
            logging.info("🔌 Соединение с БД закрыто")


if __name__ == "__main__":
    asyncio.run(main())
