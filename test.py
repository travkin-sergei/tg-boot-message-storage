"""
Этот код надо разбить на пакеты.
Проблемы возникли при разбиении. Все время что-то отваливается!!!
"""

import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Dict
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Конфигурация
BOT_TOKEN = os.getenv('BOT_TOKEN')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASS')

# ID администратора (можно указать несколько через запятую)
ADMIN_IDS = [int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id.strip()]

# Интервал для группировки сообщений (в секундах)
PACKET_INTERVAL = 5

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def escape_markdown(text: str) -> str:
    """Экранирует специальные символы для Markdown"""
    if not text:
        return text
    # Специальные символы в MarkdownV2: _ * [ ] ( ) ~ ` > # + - = | { } . !
    special_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(special_chars)}])', r'\\\1', str(text))


def safe_markdown(text: str, parse_mode: str = "Markdown") -> str:
    """Безопасно подготавливает текст для Markdown"""
    if parse_mode == "MarkdownV2":
        return escape_markdown(text)
    # Для обычного Markdown просто возвращаем как есть, но заменяем проблемные последовательности
    return text.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')


class Database:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        """Подключение к базе данных"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            # Включаем autocommit для предотвращения зависших транзакций
            self.conn.autocommit = True
            logging.info("✅ Подключение к БД успешно")
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к БД: {e}")
            raise

    def reset_connection(self):
        """Сброс подключения при ошибке"""
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        self.connect()

    def execute_with_retry(self, func, *args, **kwargs):
        """Выполнить функцию с повторной попыткой при ошибке"""
        try:
            return func(*args, **kwargs)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.error(f"Ошибка подключения к БД: {e}. Переподключаемся...")
            self.reset_connection()
            return func(*args, **kwargs)

    def get_user(self, telegram_id: int, username: str = None) -> int:
        """Получить или создать пользователя"""

        def _get_user():
            with self.conn.cursor() as cur:
                # Проверяем существование пользователя
                cur.execute(
                    "SELECT id FROM users WHERE telegram_id = %s",
                    (telegram_id,)
                )
                result = cur.fetchone()

                if result:
                    return result[0]

                # Создаем нового пользователя
                cur.execute(
                    "INSERT INTO users (telegram_id, username) VALUES (%s, %s) RETURNING id",
                    (telegram_id, username)
                )
                user_id = cur.fetchone()[0]
                return user_id

        return self.execute_with_retry(_get_user)

    def create_package(self, user_id: int) -> int:
        """Создать новый пакет сообщений"""

        def _create_package():
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO message_packages (user_id) 
                       VALUES (%s) RETURNING id""",
                    (user_id,)
                )
                package_id = cur.fetchone()[0]
                return package_id

        return self.execute_with_retry(_create_package)

    def add_message(self, package_id: int, forwarded_from_id: int,
                    forwarded_from_name: str, is_own_message: bool,
                    message_text: str, message_type: str, file_id: str = None):
        """Добавить сообщение в пакет"""

        def _add_message():
            with self.conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO messages 
                       (package_id, forwarded_from_id, forwarded_from_name, 
                        is_own_message, message_text, message_type, file_id,
                        bot_received_time) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (package_id, forwarded_from_id, forwarded_from_name,
                     is_own_message, message_text, message_type, file_id,
                     datetime.now())
                )

        self.execute_with_retry(_add_message)

    def get_package_stats(self, user_id: int) -> dict:
        """Получить статистику пользователя"""

        def _get_stats():
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    "SELECT COUNT(*) as package_count FROM message_packages WHERE user_id = %s",
                    (user_id,)
                )
                package_count = cur.fetchone()['package_count']

                cur.execute(
                    """SELECT COUNT(*) as message_count FROM messages 
                       WHERE package_id IN (SELECT id FROM message_packages WHERE user_id = %s)""",
                    (user_id,)
                )
                message_count = cur.fetchone()['message_count']

                cur.execute(
                    """SELECT COUNT(*) as own_messages FROM messages 
                       WHERE package_id IN (SELECT id FROM message_packages WHERE user_id = %s)
                       AND is_own_message = TRUE""",
                    (user_id,)
                )
                own_messages = cur.fetchone()['own_messages']

                return {
                    'package_count': package_count,
                    'message_count': message_count,
                    'own_messages': own_messages,
                    'foreign_messages': message_count - own_messages
                }

        return self.execute_with_retry(_get_stats)

    def get_package_messages(self, package_id: int, user_db_id: int = None) -> list:
        """Получить все сообщения из пакета (user_db_id опционален для админа)"""

        def _get_messages():
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                # Если указан user_db_id, проверяем принадлежность пакета
                if user_db_id:
                    cur.execute(
                        "SELECT id FROM message_packages WHERE id = %s AND user_id = %s",
                        (package_id, user_db_id)
                    )
                    if not cur.fetchone():
                        return []

                # Получаем сообщения с информацией о пользователе
                cur.execute(
                    """SELECT m.*, u.telegram_id as user_telegram_id, u.username as user_username 
                       FROM messages m
                       JOIN message_packages p ON m.package_id = p.id
                       JOIN users u ON p.user_id = u.id
                       WHERE m.package_id = %s 
                       ORDER BY m.bot_received_time ASC""",
                    (package_id,)
                )
                return cur.fetchall()

        return self.execute_with_retry(_get_messages)

    def get_package_info(self, package_id: int) -> dict:
        """Получить информацию о пакете"""

        def _get_info():
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    """SELECT 
                        p.id as package_id,
                        p.user_id,
                        u.telegram_id as user_telegram_id,
                        u.username as user_username,
                        COUNT(m.id) as total_messages,
                        SUM(CASE WHEN m.is_own_message THEN 1 ELSE 0 END) as own_messages,
                        COUNT(DISTINCT CASE 
                            WHEN m.is_own_message THEN 'USER_OWN' 
                            ELSE m.forwarded_from_name 
                        END) as participants,
                        MIN(m.bot_received_time) as first_message,
                        MAX(m.bot_received_time) as last_message
                       FROM message_packages p
                       JOIN users u ON p.user_id = u.id
                       LEFT JOIN messages m ON p.id = m.package_id
                       WHERE p.id = %s
                       GROUP BY p.id, u.telegram_id, u.username""",
                    (package_id,)
                )
                return cur.fetchone()

        return self.execute_with_retry(_get_info)

    def get_package_participants(self, package_id: int) -> list:
        """Получить список участников пакета"""

        def _get_participants():
            with self.conn.cursor() as cur:
                cur.execute(
                    """SELECT DISTINCT 
                          CASE 
                            WHEN is_own_message THEN 'Вы' 
                            ELSE forwarded_from_name 
                          END as participant
                       FROM messages 
                       WHERE package_id = %s
                       ORDER BY participant""",
                    (package_id,)
                )
                return [row[0] for row in cur.fetchall()]

        return self.execute_with_retry(_get_participants)

    def search_packages_by_user(self, telegram_id: int = None, username: str = None, limit: int = 20) -> list:
        """Поиск пакетов по пользователю (для админа)"""

        def _search():
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                query = """
                    SELECT p.id, p.created_at, u.telegram_id, u.username,
                           COUNT(m.id) as msg_count
                    FROM message_packages p
                    JOIN users u ON p.user_id = u.id
                    LEFT JOIN messages m ON p.id = m.package_id
                    WHERE 1=1
                """
                params = []

                if telegram_id:
                    query += " AND u.telegram_id = %s"
                    params.append(telegram_id)

                if username:
                    query += " AND u.username ILIKE %s"
                    params.append(f"%{username}%")

                query += " GROUP BY p.id, u.telegram_id, u.username ORDER BY p.created_at DESC LIMIT %s"
                params.append(limit)

                cur.execute(query, params)
                return cur.fetchall()

        return self.execute_with_retry(_search)


# Кэш для отслеживания времени последнего сообщения
user_last_message_time: Dict[int, Dict] = {}

# СОЗДАЕМ ЭКЗЕМПЛЯР БАЗЫ ДАННЫХ
db = Database()


def is_admin(user_id: int) -> bool:
    """Проверка, является ли пользователь администратором"""
    return user_id in ADMIN_IDS


async def send_packet_content(user_id: int, package_id: int, messages: list, admin_view: bool = False,
                              user_info: str = ""):
    """Отправить содержимое пакета пользователю"""

    # Информация о пакете
    packet_time = messages[0]['bot_received_time'].strftime('%d.%m.%Y %H:%M:%S')
    total_msgs = len(messages)
    own_msgs = sum(1 for m in messages if m['is_own_message'])

    # Собираем уникальных участников
    participants = set()
    for m in messages:
        if m['is_own_message']:
            participants.add("Вы")
        else:
            participants.add(m['forwarded_from_name'])

    # Формируем текст без Markdown разметки
    dialog_lines = []

    if admin_view:
        dialog_lines.append(f"🔐 **АДМИН-ПРОСМОТР**")
        if user_info:
            dialog_lines.append(f"{user_info}")
        dialog_lines.append("═" * 30)

    dialog_lines.append(f"📦 Пакет #{package_id}")
    dialog_lines.append(f"📅 Начало: {packet_time}")
    dialog_lines.append(f"📊 Всего: {total_msgs} сообщений")
    dialog_lines.append(f"👥 Участники: {', '.join(participants)}")
    dialog_lines.append("=" * 30)
    dialog_lines.append("")

    for i, msg in enumerate(messages, 1):
        if msg['is_own_message']:
            sender = "👤 Вы"
        else:
            sender = f"👥 {msg['forwarded_from_name']}"

        bot_time = msg['bot_received_time'].strftime('%H:%M:%S')

        dialog_lines.append(f"[{i}] {sender} [{bot_time}]:")
        dialog_lines.append(f"{msg['message_text']}")

        if msg['message_type'] != 'text':
            dialog_lines.append(f"[Тип: {msg['message_type']}]")

        if i < len(messages):
            next_time = messages[i]['bot_received_time']
            time_diff = (next_time - msg['bot_received_time']).total_seconds()
            if time_diff > 1:
                dialog_lines.append(f"пауза {time_diff:.1f} сек")

        dialog_lines.append("-" * 20)

    dialog = "\n".join(dialog_lines)

    # Отправляем без Markdown разметки
    if len(dialog) > 4096:
        for x in range(0, len(dialog), 4096):
            await bot.send_message(user_id, dialog[x:x + 4096])
    else:
        await bot.send_message(user_id, dialog)


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id

    base_text = (
        "👋 Привет! Я бот для сохранения диалогов.\n\n"
        "📝 **Как это работает:**\n"
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

    # Добавляем админские команды
    if is_admin(user_id):
        base_text += (
            "\n\n**🔐 Админ-команды:**\n"
            "/ap <номер> - показать любой диалог по ID пакета (admin packet)\n"
            "/auser <telegram_id> - показать пакеты пользователя\n"
            "/asearch <username> - поиск по username"
        )

    await message.answer(base_text, parse_mode="Markdown")


@dp.message(Command("new_packet"))
async def cmd_new_packet(message: Message):
    """Принудительно начать новый пакет"""
    user_id = message.from_user.id

    if user_id in user_last_message_time:
        last_package = user_last_message_time[user_id]['package_id']
        # Отправляем статистику по закрываемому пакету
        await send_packet_summary(user_id, last_package)
        del user_last_message_time[user_id]
        await message.answer("✅ **Новый пакет создан!**", parse_mode="Markdown")
    else:
        await message.answer("✅ **Готов к приему сообщений!**", parse_mode="Markdown")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Показать статистику"""
    db_user_id = db.get_user(message.from_user.id, message.from_user.username)
    stats = db.get_package_stats(db_user_id)

    # Информация о текущем активном пакете
    current_info = ""
    if message.from_user.id in user_last_message_time:
        package_id = user_last_message_time[message.from_user.id]['package_id']
        last_time = user_last_message_time[message.from_user.id]['last_time']
        seconds_ago = (datetime.now() - last_time).total_seconds()

        # Получаем информацию о пакете
        info = db.get_package_info(package_id)

        time_left = max(0, PACKET_INTERVAL - seconds_ago)
        current_info = (
            f"\n📦 **Текущий пакет #{package_id}**\n"
            f"   Сообщений: {info['total_messages']}\n"
            f"   Участников: {info['participants']}\n"
            f"   ⏱️ Закроется через: {time_left:.1f} сек"
        )

    text = (
        f"📊 **Статистика**\n"
        f"📦 Всего пакетов: {stats['package_count']}\n"
        f"💬 Всего сообщений: {stats['message_count']}\n"
        f"   👤 Своих: {stats['own_messages']}\n"
        f"   👥 Чужих: {stats['foreign_messages']}"
        f"{current_info}"
    )
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("packets"))
async def cmd_packets(message: Message):
    """Показать список последних пакетов"""
    db_user_id = db.get_user(message.from_user.id, message.from_user.username)

    with db.conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """SELECT p.id, p.created_at as packet_time,
                      COUNT(m.id) as msg_count,
                      SUM(CASE WHEN m.is_own_message THEN 1 ELSE 0 END) as own_count,
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

    text = "📦 **Последние пакеты:**\n\n"
    for p in packets:
        foreign = p['msg_count'] - p['own_count']
        time_str = p['packet_time'].strftime('%d.%m %H:%M:%S')
        text += f"#{p['id']} | {time_str} | 📨 {p['msg_count']} | 👥 {p['participants']} участников\n"

    text += "\nИспользуй /get_packet <номер> для просмотра"
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("get_packet"))
async def cmd_get_packet(message: Message):
    """Показать содержимое пакета (только свои)"""
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите номер пакета. Пример: /get_packet 42")
        return

    try:
        package_id = int(args[1])
    except ValueError:
        await message.answer("❌ Номер пакета должен быть числом")
        return

    db_user_id = db.get_user(message.from_user.id, message.from_user.username)
    messages = db.get_package_messages(package_id, db_user_id)

    if not messages:
        await message.answer("❌ Пакет не найден или не принадлежит вам")
        return

    await send_packet_content(message.from_user.id, package_id, messages)


# ============= АДМИНСКИЕ КОМАНДЫ =============

@dp.message(Command("ap"))
async def cmd_admin_packet(message: Message):
    """
    Админ: показать любой диалог по ID пакета
    Использование: /ap 5 - показать все сообщения из пакета #5
    """
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для этой команды")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите номер пакета. Пример: /ap 42")
        return

    try:
        package_id = int(args[1])
    except ValueError:
        await message.answer("❌ Номер пакета должен быть числом")
        return

    # Получаем информацию о пакете
    info = db.get_package_info(package_id)

    if not info:
        await message.answer(f"❌ Пакет #{package_id} не найден")
        return

    # Получаем все сообщения пакета (без проверки принадлежности)
    messages = db.get_package_messages(package_id)

    if not messages:
        await message.answer(f"📦 Пакет #{package_id} пуст")
        return

    # Формируем информацию о пользователе
    user_info = f"👤 Пользователь: {info['user_username'] or 'без username'} (ID: {info['user_telegram_id']})"

    # Отправляем содержимое пакета
    await send_packet_content(
        message.from_user.id,
        package_id,
        messages,
        admin_view=True,
        user_info=user_info
    )


@dp.message(Command("auser"))
async def cmd_admin_user(message: Message):
    """Админ: показать пакеты пользователя по telegram_id"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для этой команды")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите telegram_id пользователя. Пример: /auser 123456789")
        return

    try:
        telegram_id = int(args[1])
    except ValueError:
        await message.answer("❌ telegram_id должен быть числом")
        return

    # Ищем пакеты пользователя
    packages = db.search_packages_by_user(telegram_id=telegram_id, limit=20)

    if not packages:
        await message.answer(f"❌ Пакеты для пользователя {telegram_id} не найдены")
        return

    text = f"📦 **Пакеты пользователя {telegram_id}:**\n\n"
    for p in packages:
        time_str = p['created_at'].strftime('%d.%m %H:%M:%S')
        text += f"#{p['id']} | {time_str} | 📨 {p['msg_count']} сообщений\n"

    text += "\nИспользуй /ap <номер> для просмотра диалога"

    # Добавляем кнопки для быстрого просмотра
    if len(packages) <= 5:
        keyboard = []
        for p in packages:
            keyboard.append([InlineKeyboardButton(
                text=f"📦 Пакет #{p['id']}",
                callback_data=f"admin_packet_{p['id']}"
            )])
        await message.answer(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    else:
        await message.answer(text, parse_mode="Markdown")


@dp.message(Command("asearch"))
async def cmd_admin_search(message: Message):
    """Админ: поиск пакетов по username"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав для этой команды")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите username для поиска. Пример: /asearch john")
        return

    username = args[1].replace('@', '')  # Убираем @ если есть

    # Ищем пакеты по username
    packages = db.search_packages_by_user(username=username, limit=20)

    if not packages:
        await message.answer(f"❌ Пакеты для пользователя с username '{username}' не найдены")
        return

    # Группируем по пользователям
    users_dict = {}
    for p in packages:
        key = p['telegram_id']
        if key not in users_dict:
            users_dict[key] = {
                'telegram_id': p['telegram_id'],
                'username': p['username'],
                'packages': []
            }
        users_dict[key]['packages'].append(p)

    text = f"🔍 **Результаты поиска по username '{username}':**\n\n"

    for user_data in users_dict.values():
        text += f"👤 **{user_data['username'] or user_data['telegram_id']}** (id: {user_data['telegram_id']})\n"
        for p in user_data['packages'][:5]:  # Показываем первые 5 пакетов
            time_str = p['created_at'].strftime('%d.%m %H:%M:%S')
            text += f"  • #{p['id']} | {time_str} | 📨 {p['msg_count']} сообщений\n"
        if len(user_data['packages']) > 5:
            text += f"  • ... и еще {len(user_data['packages']) - 5}\n"
        text += "\n"

    text += "Используй /ap <номер> для просмотра диалога"
    await message.answer(text, parse_mode="Markdown")


@dp.callback_query(lambda c: c.data.startswith('admin_packet_'))
async def callback_admin_packet(callback: CallbackQuery):
    """Обработчик кнопок для просмотра пакета"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У вас нет прав", show_alert=True)
        return

    package_id = int(callback.data.replace('admin_packet_', ''))

    # Получаем информацию о пакете
    info = db.get_package_info(package_id)

    if not info:
        await callback.answer(f"❌ Пакет #{package_id} не найден", show_alert=True)
        return

    messages = db.get_package_messages(package_id)

    if not messages:
        await callback.answer(f"📦 Пакет #{package_id} пуст", show_alert=True)
        return

    await callback.answer()

    user_info = f"👤 Пользователь: {info['user_username'] or 'без username'} (ID: {info['user_telegram_id']})"

    await send_packet_content(
        callback.from_user.id,
        package_id,
        messages,
        admin_view=True,
        user_info=user_info
    )


async def send_packet_summary(user_id: int, package_id: int):
    """Отправить сводку по пакету (без Markdown разметки)"""
    try:
        info = db.get_package_info(package_id)
        participants = db.get_package_participants(package_id)

        # Вычисляем длительность пакета
        duration = (info['last_message'] - info['first_message']).total_seconds()

        # Формируем текст без Markdown разметки
        summary_lines = []
        summary_lines.append(f"📦 Пакет #{package_id} сохранен!")
        summary_lines.append("=" * 30)
        summary_lines.append("📊 Статистика:")
        summary_lines.append(f"• Сообщений: {info['total_messages']}")
        summary_lines.append(f"  👤 Своих: {info['own_messages']}")
        summary_lines.append(f"  👥 Чужих: {info['total_messages'] - info['own_messages']}")
        summary_lines.append(f"• Участников: {info['participants']}")

        # Показываем первых 5 участников
        if participants:
            participants_text = ", ".join(participants[:5])
            if len(participants) > 5:
                participants_text += f" и еще {len(participants) - 5}"
            summary_lines.append(f"  {participants_text}")

        summary_lines.append(f"• Длительность: {duration:.1f} сек")
        summary_lines.append(f"• Начало: {info['first_message'].strftime('%H:%M:%S')}")
        summary_lines.append(f"• Конец: {info['last_message'].strftime('%H:%M:%S')}")
        summary_lines.append("=" * 30)
        summary_lines.append(f"💡 Используй /get_packet {package_id} для просмотра")

        summary = "\n".join(summary_lines)

        # Отправляем без Markdown разметки
        await bot.send_message(user_id, summary)
        logging.info(f"📨 Отправлена сводка по пакету #{package_id} пользователю {user_id}")
    except Exception as e:
        logging.error(f"Ошибка при отправке сводки: {e}")
        # Пробуем отправить упрощенную версию
        try:
            await bot.send_message(
                user_id,
                f"📦 Пакет #{package_id} сохранен!\nИспользуй /get_packet {package_id} для просмотра"
            )
        except:
            pass


@dp.message()
async def handle_forwarded(message: Message):
    """Обработка пересланных сообщений"""
    # Проверяем, является ли сообщение пересланным
    if not message.forward_from and not message.forward_from_chat and not message.forward_sender_name:
        return

    user_id = message.from_user.id
    current_time = datetime.now()

    # Получаем или создаем пользователя в БД
    db_user_id = db.get_user(user_id, message.from_user.username)

    # Определяем отправителя оригинального сообщения
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

    logging.info(f"📨 Получено сообщение в {current_time.strftime('%H:%M:%S.%f')}")
    logging.info(f"   Отправитель: {forwarded_name}, своё: {is_own_message}")

    # Определяем пакет
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

    # Если нужен новый пакет - создаем
    if is_new_packet:
        package_id = db.create_package(db_user_id)
        logging.info(f"   🆕 Создан новый пакет #{package_id}")

    # Обновляем время последнего сообщения
    user_last_message_time[user_id] = {
        'last_time': current_time,
        'package_id': package_id,
        'notification_sent': False
    }

    # Определяем тип сообщения
    message_type = "text"
    file_id = None
    message_text = message.text or message.caption or ""

    if message.photo:
        message_type = "photo"
        file_id = message.photo[-1].file_id
        if not message_text:
            message_text = "[Фото]"
    elif message.video:
        message_type = "video"
        file_id = message.video.file_id
        if not message_text:
            message_text = "[Видео]"
    elif message.document:
        message_type = "document"
        file_id = message.document.file_id
        if not message_text:
            message_text = f"[Документ: {message.document.file_name}]"
    elif message.voice:
        message_type = "voice"
        file_id = message.voice.file_id
        message_text = "[Голосовое]"
    elif message.audio:
        message_type = "audio"
        file_id = message.audio.file_id
        message_text = "[Аудио]"
    elif message.sticker:
        message_type = "sticker"
        file_id = message.sticker.file_id
        message_text = "[Стикер]"
    elif message.video_note:
        message_type = "video_note"
        file_id = message.video_note.file_id
        message_text = "[Кружок]"

    # Сохраняем сообщение
    db.add_message(
        package_id=package_id,
        forwarded_from_id=forwarded_id,
        forwarded_from_name=forwarded_name,
        is_own_message=is_own_message,
        message_text=message_text,
        message_type=message_type,
        file_id=file_id
    )

    # НЕ ОТПРАВЛЯЕМ НИКАКОГО ПОДТВЕРЖДЕНИЯ!


# Фоновая задача для закрытия пакетов
async def packet_closer():
    """Проверяет и закрывает пакеты по истечении интервала"""
    while True:
        await asyncio.sleep(0.5)
        current_time = datetime.now()
        users_to_close = []

        for user_id, info in user_last_message_time.items():
            if info.get('notification_sent', False):
                continue

            time_diff = (current_time - info['last_time']).total_seconds()
            if time_diff > PACKET_INTERVAL:
                users_to_close.append((user_id, info['package_id']))

        for user_id, package_id in users_to_close:
            await send_packet_summary(user_id, package_id)

            if user_id in user_last_message_time:
                user_last_message_time[user_id]['notification_sent'] = True

            logging.info(f"🔒 Закрыт пакет #{package_id} для пользователя {user_id}")


async def main():
    """Запуск бота"""
    # Запускаем фоновую задачу для закрытия пакетов
    asyncio.create_task(packet_closer())

    logging.info("🚀 Бот запущен и готов к работе!")
    logging.info(f"⏱️ Интервал пакетирования: {PACKET_INTERVAL} секунд")
    logging.info("📨 Уведомления отправляются ТОЛЬКО при закрытии пакета")

    if ADMIN_IDS:
        logging.info(f"👑 Администраторы: {', '.join(map(str, ADMIN_IDS))}")
        logging.info("📝 Админ-команда: /ap <номер> - показать диалог по ID пакета")
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
