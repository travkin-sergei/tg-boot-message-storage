from datetime import datetime


def format_time(time_obj: datetime) -> str:
    return time_obj.strftime('%H:%M:%S')


def format_date(time_obj: datetime) -> str:
    return time_obj.strftime('%d.%m.%Y %H:%M:%S')


def split_long_message(text: str, max_length: int = 4096) -> list:
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]
