import logging
from aiogram import types
from aiogram.types import CallbackQuery

from database import db
from config import is_admin
from services.packet_service import PacketService
from keyboards import get_packages_keyboard


async def cmd_admin_packet(message: types.Message, packet_service: PacketService, bot):
    user_id = message.from_user.id
    logging.info(f"👑 [cmd_admin_packet] ВЫЗВАНА администратором {user_id} с текстом: {message.text}")

    if not is_admin(user_id):
        await message.answer("❌ У вас нет прав для этой команды")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите номер пакета. Пример: /ap 42")
        return

    try:
        package_id = int(args[1])
        logging.info(f"🔍 Запрошен пакет #{package_id}")
    except ValueError:
        await message.answer("❌ Номер пакета должен быть числом")
        return

    try:
        info = db.get_package_info(package_id)
        logging.info(f"📊 Информация о пакете: {info}")
    except Exception as e:
        logging.error(f"Ошибка при получении информации о пакете: {e}")
        await message.answer("❌ Ошибка при обращении к БД")
        return

    if not info:
        await message.answer(f"❌ Пакет #{package_id} не найден")
        return

    try:
        messages = db.get_package_messages(package_id)
        logging.info(f"📨 Получено сообщений: {len(messages) if messages else 0}")
    except Exception as e:
        logging.error(f"Ошибка при получении сообщений: {e}")
        await message.answer("❌ Ошибка при загрузке сообщений")
        return

    if not messages:
        await message.answer(f"📦 Пакет #{package_id} пуст")
        return

    user_info = f"👤 Пользователь: {info['user_username'] or 'без username'} (ID: {info['user_telegram_id']})"
    logging.info(f"📤 Отправка пакета #{package_id} администратору {user_id}")

    try:
        await packet_service.send_packet_content(bot, user_id, package_id, messages,
                                                 admin_view=True, user_info=user_info)
        logging.info(f"✅ Пакет #{package_id} успешно отправлен")
        # Можно отправить дополнительное подтверждение
        await message.answer(f"✅ Пакет #{package_id} отправлен!")
    except Exception as e:
        logging.error(f"❌ Ошибка при отправке содержимого пакета: {e}")
        await message.answer(f"❌ Ошибка при отправке: {e}")


async def cmd_admin_user(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
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

    packages = db.search_packages_by_user(telegram_id=telegram_id, limit=20)

    if not packages:
        await message.answer(f"❌ Пакеты для пользователя {telegram_id} не найдены")
        return

    text = f"📦 Пакеты пользователя {telegram_id}:\n\n"
    for p in packages:
        time_str = p['created_at'].strftime('%d.%m %H:%M:%S')
        text += f"#{p['id']} | {time_str} | 📨 {p['msg_count']} сообщений\n"

    text += "\nИспользуй /ap <номер> для просмотра диалога"

    if len(packages) <= 5:
        keyboard = get_packages_keyboard(packages)
        if keyboard:
            await message.answer(text, reply_markup=keyboard)
        else:
            await message.answer(text)
    else:
        await message.answer(text)


async def cmd_admin_search(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("❌ У вас нет прав для этой команды")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("❌ Укажите username для поиска. Пример: /asearch john")
        return

    username = args[1].replace('@', '')
    packages = db.search_packages_by_user(username=username, limit=20)

    if not packages:
        await message.answer(f"❌ Пакеты для пользователя с username '{username}' не найдены")
        return

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

    text = f"🔍 Результаты поиска по username '{username}':\n\n"
    for user_data in users_dict.values():
        text += f"👤 {user_data['username'] or user_data['telegram_id']} (id: {user_data['telegram_id']})\n"
        for p in user_data['packages'][:5]:
            time_str = p['created_at'].strftime('%d.%m %H:%M:%S')
            text += f"  • #{p['id']} | {time_str} | 📨 {p['msg_count']} сообщений\n"
        if len(user_data['packages']) > 5:
            text += f"  • ... и еще {len(user_data['packages']) - 5}\n"
        text += "\n"

    text += "Используй /ap <номер> для просмотра диалога"
    await message.answer(text)


async def callback_admin_packet(callback: CallbackQuery, packet_service: PacketService, bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ У вас нет прав", show_alert=True)
        return

    package_id = int(callback.data.replace('admin_packet_', ''))
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
    await packet_service.send_packet_content(
        bot,
        callback.from_user.id,
        package_id,
        messages,
        admin_view=True,
        user_info=user_info
    )
