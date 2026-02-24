from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def get_packages_keyboard(packages: list) -> InlineKeyboardMarkup:
    if not packages:
        return None
    keyboard = []
    for p in packages[:5]:
        package_id = p['id'] if isinstance(p, dict) else p[0]
        msg_count = p['msg_count'] if isinstance(p, dict) else p[4]
        keyboard.append([InlineKeyboardButton(
            text=f"📦 Пакет #{package_id} ({msg_count} сообщ.)",
            callback_data=f"admin_packet_{package_id}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)