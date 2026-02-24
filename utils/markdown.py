import re

def escape_markdown(text: str) -> str:
    if not text:
        return text
    special_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(special_chars)}])', r'\\\1', str(text))

def safe_markdown(text: str) -> str:
    return text