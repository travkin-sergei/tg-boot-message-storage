import logging
from utils.helpers import format_date, format_time, split_long_message

class PacketService:
    @staticmethod
    async def send_packet_content(bot, user_id: int, package_id: int, messages: list,
                                  admin_view: bool = False, user_info: str = ""):
        try:
            logging.info(f"📦 Начинаем формирование пакета #{package_id} для пользователя {user_id}")
            packet_time = format_date(messages[0]['bot_received_time'])
            total_msgs = len(messages)

            participants = set()
            for m in messages:
                if m['is_own_message']:
                    participants.add("Вы")
                else:
                    participants.add(m['forwarded_from_name'])

            dialog_lines = []
            if admin_view:
                dialog_lines.append(f"🔐 АДМИН-ПРОСМОТР")
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
                sender = "👤 Вы" if msg['is_own_message'] else f"👥 {msg['forwarded_from_name']}"
                bot_time = format_time(msg['bot_received_time'])

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
            logging.info(f"📄 Сформирован текст пакета #{package_id}, длина: {len(dialog)}")

            if len(dialog) > 4096:
                parts = split_long_message(dialog)
                logging.info(f"✂️ Пакет #{package_id} разбит на {len(parts)} частей")
                for i, part in enumerate(parts):
                    await bot.send_message(user_id, part)
                    logging.info(f"📨 Отправлена часть {i+1}/{len(parts)} пакета #{package_id}")
            else:
                await bot.send_message(user_id, dialog)
                logging.info(f"📨 Пакет #{package_id} отправлен целиком")
        except Exception as e:
            logging.error(f"❌ Ошибка в send_packet_content для пакета #{package_id}: {e}")
            raise

    @staticmethod
    async def send_packet_summary(bot, user_id: int, package_id: int):
        try:
            from database import db
            info = db.get_package_info(package_id)
            participants = db.get_package_participants(package_id)

            duration = (info['last_message'] - info['first_message']).total_seconds()

            summary_lines = []
            summary_lines.append(f"📦 Пакет #{package_id} сохранен!")
            summary_lines.append("=" * 30)
            summary_lines.append("📊 Статистика:")
            summary_lines.append(f"• Сообщений: {info['total_messages']}")
            summary_lines.append(f"  👤 Своих: {info['own_messages']}")
            summary_lines.append(f"  👥 Чужих: {info['total_messages'] - info['own_messages']}")
            summary_lines.append(f"• Участников: {info['participants']}")

            if participants:
                participants_text = ", ".join(participants[:5])
                if len(participants) > 5:
                    participants_text += f" и еще {len(participants) - 5}"
                summary_lines.append(f"  {participants_text}")

            summary_lines.append(f"• Длительность: {duration:.1f} сек")
            summary_lines.append(f"• Начало: {format_time(info['first_message'])}")
            summary_lines.append(f"• Конец: {format_time(info['last_message'])}")
            summary_lines.append("=" * 30)
            summary_lines.append(f"💡 Используй /get_packet {package_id} для просмотра")

            summary = "\n".join(summary_lines)
            await bot.send_message(user_id, summary)
            logging.info(f"📨 Отправлена сводка по пакету #{package_id} пользователю {user_id}")
        except Exception as e:
            logging.error(f"Ошибка при отправке сводки: {e}")
            try:
                await bot.send_message(user_id, f"📦 Пакет #{package_id} сохранен!\nИспользуй /get_packet {package_id} для просмотра")
            except:
                pass

    @staticmethod
    def process_message_type(message) -> tuple:
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

        return message_type, file_id, message_text