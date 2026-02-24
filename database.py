import logging
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

class Database:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.conn.autocommit = True
            logging.info("✅ Подключение к БД успешно")
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к БД: {e}")
            raise

    def reset_connection(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        self.connect()

    def execute_with_retry(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.error(f"Ошибка подключения к БД: {e}. Переподключаемся...")
            self.reset_connection()
            return func(*args, **kwargs)

    def get_user(self, telegram_id: int, username: str = None) -> int:
        def _get_user():
            with self.conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE telegram_id = %s", (telegram_id,))
                result = cur.fetchone()
                if result:
                    return result[0]
                cur.execute(
                    "INSERT INTO users (telegram_id, username) VALUES (%s, %s) RETURNING id",
                    (telegram_id, username)
                )
                return cur.fetchone()[0]
        return self.execute_with_retry(_get_user)

    def create_package(self, user_id: int) -> int:
        def _create_package():
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO message_packages (user_id) VALUES (%s) RETURNING id",
                    (user_id,)
                )
                return cur.fetchone()[0]
        return self.execute_with_retry(_create_package)

    def add_message(self, package_id: int, forwarded_from_id: int,
                    forwarded_from_name: str, is_own_message: bool,
                    message_text: str, message_type: str, file_id: str = None):
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
        def _get_stats():
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("SELECT COUNT(*) as package_count FROM message_packages WHERE user_id = %s", (user_id,))
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
        def _get_messages():
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                if user_db_id:
                    cur.execute(
                        "SELECT id FROM message_packages WHERE id = %s AND user_id = %s",
                        (package_id, user_db_id)
                    )
                    if not cur.fetchone():
                        return []
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


db = Database()