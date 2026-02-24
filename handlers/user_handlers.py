import logging
from aiogram import types
from datetime import datetime

from database import db
from config import PACKET_INTERVAL, is_admin

# Глобальные объекты (будут установлены из main.py)
bot = None
packet_service = None

user_last_message_time = {}

async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    base_text = (
        "👋 Привет! Я бот для сохранения диалогов.\n\n"
        "📝 Как это работает:\n"
        "1. Ты пересылаешь мне сообщения из диалога\n"
        "2. Я группирую их в пакеты по времени отправки МНЕ\n"
        f"3. Все сообщения, присланные в течение {PACKET_INTERVAL} секунд, попадают в один пакет\n"
        "4. Когда пакет закрывается, я присылаю ОДНО уведомление со статистикой\n\n"
        "✅ Так ты не получаешь уведомление на каждое сообщение\n\n"
        "Команды:\n"
        "/new_packet - начать новый пакет (принудительно)\n"
        "/stats - статистика\n"
        "/get_packet <номер> - показать диалог из пакета\n"
        "/packets - список последних пакетов"
    )
    if is_admin(user_id):
        base_text += (
            "\n\n🔐 Админ-команды:\n"
            "/ap <номер> - показать любой диалог по ID пакета\n"
            "/auser <telegram_id> - показать пакеты пользователя\n"
            "/asearch <username> - поиск по username"
        )
    await message.answer(base_text)

async def cmd_new_packet(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_last_message_time:
        last_package = user_last_message_time[user_id]['package_id']
        await packet_service.send_packet_summary(bot, user_id, last_package)
        del user_last_message_time[user_id]
        await message.answer("✅ Новый пакет создан!")
    else:
        await message.answer("✅ Готов к приему сообщений!")

async def cmd_stats(message: types.Message):
    db_user_id = db.get_user(message.from_user.id, message.from_user.username)
    stats = db.get_package_stats(db_user_id)

    current_info = ""
    if message.from_user.id in user_last_message_time:
        package_id = user_last_message_time[message.from_user.id]['package_id']
        last_time = user_last_message_time[message.from_user.id]['last_time']
        seconds_ago = (datetime.now() - last_time).total_seconds()
        info = db.get_package_info(package_id)
        time_left = max(0, PACKET_INTERVAL - seconds_ago)
        current_info = (
            f"\n📦 Текущий пакет #{package_id}\n"
            f"   Сообщений: {info['total_messages']}\n"
            f"   Участников: {info['participants']}\n"
            f"   ⏱️ Закроется через: {time_left:.1f} сек"
        )

    text = (
        f"📊 Статистика\n"
        f"📦 Всего пакетов: {stats['package_count']}\n"
        f"💬 Всего сообщений: {stats['message_count']}\n"
        f"   👤 Своих: {stats['own_messages']}\n"
        f"   👥 Чужих: {stats['foreign_messages']}"
        f"{current_info}"
    )
    await message.answer(text)

async def cmd_packets(message: types.Message):
    db_user_id = db.get_user(message.from_user.id, message.from_user.username)

    with db.conn.cursor() as cur:
        cur.execute(
            """SELECT p.id, p.created_at as packet_time,
                      COUNT(m.id) as msg_count,
                      COUNT(DISTINCT CASE 
                          WHEN m.is_own_message THEN 'USER_OWN' 
                          ELSE m.forwarded_from_name 
                      END) as participants
               FROM message_packages p
               LEFT JOIN messages m ON p.id = m.package_id
               WHERE p.user_id = %s
               GROUP BY p.id
               ORDER BY p.created_at DESC
               LIMIT 10""",
            (db_user_id,)
        )
        packets = cur.fetchall()

    if not packets:
        await message.answer("📭 У вас пока нет пакетов")
        return

    text = "📦 Последние пакеты:\n\n"
    for p in packets:
        time_str = p[1].strftime('%d.%m %H:%M:%S')
        text += f"#{p[0]} | {time_str} | 📨 {p[2]} | 👥 {p[3]} участников\n"

    text += "\nИспользуй /get_packet <номер> для просмотра"
    await message.answer(text)

async def cmd_get_packet(message: types.Message):
    user_id = message.from_user.id
    logging.info(f"👤 [cmd_get_packet] ВЫЗВАНА пользователем {user_id} с текстом: {message.text}")

    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите номер пакета. Пример: /get_packet 42")
        return

    try:
        package_id = int(args[1])
    except ValueError:
        await message.answer("❌ Номер пакета должен быть числом")
        return

    db_user_id = db.get_user(user_id, message.from_user.username)
    messages = db.get_package_messages(package_id, db_user_id)

    if not messages:
        await message.answer("❌ Пакет не найден или не принадлежит вам")
        return

    await packet_service.send_packet_content(bot, user_id, package_id, messages)