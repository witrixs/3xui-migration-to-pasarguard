import os
import json
import re
import secrets
import string
import uuid
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения из .env
load_dotenv()

# ============================================
# УНИВЕРСАЛЬНЫЙ КЛАСС ДЛЯ РАБОТЫ С БД
# ============================================

class DatabaseConnection:
    """Универсальный класс для работы с разными типами БД"""
    
    def __init__(self, db_type, **kwargs):
        self.db_type = db_type.lower()
        self.connection = None
        self.cursor = None
        self._connect(**kwargs)
    
    def _connect(self, **kwargs):
        """Устанавливает соединение с БД в зависимости от типа"""
        if self.db_type == 'sqlite':
            import sqlite3
            db_path = kwargs.get('db_path', 'database.db')
            self.connection = sqlite3.connect(db_path)
            self.cursor = self.connection.cursor()
            
        elif self.db_type in ['postgresql', 'postgres']:
            try:
                import psycopg2
            except ImportError:
                raise ImportError("psycopg2-binary не установлен. Установите: pip install psycopg2-binary")
            
            self.connection = psycopg2.connect(
                host=kwargs.get('host', 'localhost'),
                port=kwargs.get('port', 5432),
                database=kwargs.get('database', 'postgres'),
                user=kwargs.get('user', 'postgres'),
                password=kwargs.get('password', '')
            )
            self.cursor = self.connection.cursor()
            
        elif self.db_type in ['mysql', 'mariadb']:
            try:
                import pymysql
            except ImportError:
                raise ImportError("pymysql не установлен. Установите: pip install pymysql")
            
            self.connection = pymysql.connect(
                host=kwargs.get('host', 'localhost'),
                port=kwargs.get('port', 3306),
                database=kwargs.get('database', 'mysql'),
                user=kwargs.get('user', 'root'),
                password=kwargs.get('password', ''),
                charset='utf8mb4'
            )
            self.cursor = self.connection.cursor()
        else:
            raise ValueError(f"Неподдерживаемый тип БД: {self.db_type}")
    
    def execute(self, query, params=None):
        """Выполняет SQL запрос с параметрами"""
        # Адаптируем плейсхолдеры для разных БД
        if params:
            # PostgreSQL и MySQL используют %s
            # SQLite использует ?
            if self.db_type == 'sqlite':
                # Заменяем %s на ? для SQLite (только если это не часть строки)
                # Безопасная замена: только если %s не внутри кавычек
                query = re.sub(r'(?<!["\'])%s(?!["\'])', '?', query)
            # Для PostgreSQL и MySQL/MariaDB используем %s как есть
            
        self.cursor.execute(query, params or ())
        return self.cursor
    
    def fetchone(self):
        """Получает одну строку результата"""
        return self.cursor.fetchone()
    
    def fetchall(self):
        """Получает все строки результата"""
        return self.cursor.fetchall()
    
    def commit(self):
        """Сохраняет изменения"""
        self.connection.commit()
    
    def close(self):
        """Закрывает соединение"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    @property
    def lastrowid(self):
        """Возвращает ID последней вставленной строки"""
        if self.db_type == 'sqlite':
            return self.cursor.lastrowid
        elif self.db_type in ['postgresql', 'postgres']:
            return self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else None
        elif self.db_type in ['mysql', 'mariadb']:
            return self.cursor.lastrowid
        return None

# ============================================
# ФУНКЦИИ ДЛЯ ГЕНЕРАЦИИ ПАРОЛЕЙ И UUID
# ============================================

def generate_password(min_length=22):
    """Генерирует случайный пароль минимум 22 символа"""
    alphabet = string.ascii_letters + string.digits
    password = ''.join(secrets.choice(alphabet) for _ in range(min_length))
    return password

def is_valid_uuid(value):
    """Проверяет, является ли строка валидным UUID"""
    if not value:
        return False
    try:
        uuid.UUID(value)
        return True
    except (ValueError, AttributeError):
        return False

# ============================================
# ЗАГРУЗКА КОНФИГУРАЦИИ ИЗ .ENV
# ============================================

def get_xui_db_config():
    """Получает конфигурацию БД для 3X-UI"""
    db_type = os.getenv('XUI_DB_TYPE', 'sqlite').lower()
    
    if db_type == 'sqlite':
        return {
            'db_type': 'sqlite',
            'db_path': os.getenv('XUI_DB_PATH', 'x-ui.db')
        }
    elif db_type in ['postgresql', 'postgres']:
        return {
            'db_type': 'postgresql',
            'host': os.getenv('XUI_DB_HOST', 'localhost'),
            'port': int(os.getenv('XUI_DB_PORT', 5432)),
            'database': os.getenv('XUI_DB_NAME', 'xui'),
            'user': os.getenv('XUI_DB_USER', 'postgres'),
            'password': os.getenv('XUI_DB_PASSWORD', '')
        }
    elif db_type in ['mysql', 'mariadb']:
        return {
            'db_type': db_type,
            'host': os.getenv('XUI_DB_HOST', 'localhost'),
            'port': int(os.getenv('XUI_DB_PORT', 3306)),
            'database': os.getenv('XUI_DB_NAME', 'xui'),
            'user': os.getenv('XUI_DB_USER', 'root'),
            'password': os.getenv('XUI_DB_PASSWORD', '')
        }
    else:
        raise ValueError(f"Неподдерживаемый тип БД для XUI: {db_type}")

def get_pasar_db_config():
    """Получает конфигурацию БД для Pasarguard"""
    db_type = os.getenv('PASAR_DB_TYPE', 'sqlite').lower()
    
    if db_type == 'sqlite':
        return {
            'db_type': 'sqlite',
            'db_path': os.getenv('PASAR_DB_PATH', 'db.sqlite3')
        }
    elif db_type in ['postgresql', 'postgres']:
        return {
            'db_type': 'postgresql',
            'host': os.getenv('PASAR_DB_HOST', 'localhost'),
            'port': int(os.getenv('PASAR_DB_PORT', 5432)),
            'database': os.getenv('PASAR_DB_NAME', 'pasarguard'),
            'user': os.getenv('PASAR_DB_USER', 'postgres'),
            'password': os.getenv('PASAR_DB_PASSWORD', '')
        }
    elif db_type in ['mysql', 'mariadb']:
        return {
            'db_type': db_type,
            'host': os.getenv('PASAR_DB_HOST', 'localhost'),
            'port': int(os.getenv('PASAR_DB_PORT', 3306)),
            'database': os.getenv('PASAR_DB_NAME', 'pasarguard'),
            'user': os.getenv('PASAR_DB_USER', 'root'),
            'password': os.getenv('PASAR_DB_PASSWORD', '')
        }
    else:
        raise ValueError(f"Неподдерживаемый тип БД для Pasarguard: {db_type}")

# ============================================
# ОСНОВНОЙ КОД МИГРАЦИИ
# ============================================

print("=" * 80)
print("ПОЛНАЯ МИГРАЦИЯ ИЗ 3X-UI В PASARGUARD")
print("=" * 80)

# Подключение к БД
print("\n[ИНИЦИАЛИЗАЦИЯ] Подключение к базам данных...")

try:
    xui_config = get_xui_db_config()
    pasar_config = get_pasar_db_config()
    
    print(f"  XUI БД: {xui_config['db_type']}")
    print(f"  Pasarguard БД: {pasar_config['db_type']}")
    
    xui = DatabaseConnection(**xui_config)
    pasar = DatabaseConnection(**pasar_config)
    
    xui_cur = xui
    pasar_cur = pasar
    
    print("  ✓ Подключение успешно!")
    
except Exception as e:
    print(f"  ✗ ОШИБКА подключения: {e}")
    exit(1)

# ШАГ 1: Получаем данные из x-ui.db
print("\n[ШАГ 1] Извлечение данных из 3X-UI...")

# получаем все inbounds с их настройками
xui_cur.execute("SELECT id, settings FROM inbounds;")
inbounds = xui_cur.fetchall()

# получаем статистику трафика для клиентов
xui_cur.execute("SELECT inbound_id, email, up, down, expiry_time, total FROM client_traffics;")
traffic_data = {}
for row in xui_cur.fetchall():
    inbound_id, email, up, down, expiry_time, total = row
    key = (inbound_id, email)
    traffic_data[key] = {
        "up": up,
        "down": down,
        "expiry_time": expiry_time,
        "total": total
    }

print(f"  Найдено inbounds: {len(inbounds)}")
print(f"  Найдено записей трафика: {len(traffic_data)}")

# ШАГ 2: Извлекаем всех клиентов
print("\n[ШАГ 2] Обработка клиентов из inbounds...")

clients_data = []

for inbound_id, settings_json in inbounds:
    if not settings_json:
        continue
    
    try:
        settings = json.loads(settings_json)
        clients = settings.get("clients", [])
        
        # обрабатываем каждого клиента
        for client in clients:
            email = client.get("email", "")
            client_id = client.get("id", "")
            enable = client.get("enable", True)
            expiry_time = client.get("expiryTime", 0)
            total_gb = client.get("totalGB", 0)
            
            # получаем статистику трафика из client_traffics
            traffic_key = (inbound_id, email)
            if traffic_key in traffic_data:
                up = traffic_data[traffic_key]["up"]
                down = traffic_data[traffic_key]["down"]
                # используем expiry_time и total из client_traffics, если они есть
                if traffic_data[traffic_key]["expiry_time"]:
                    expiry_time = traffic_data[traffic_key]["expiry_time"]
                if traffic_data[traffic_key]["total"]:
                    total_gb = traffic_data[traffic_key]["total"] / (1024**3)  # конвертируем байты в GB
            else:
                # если нет данных в client_traffics, используем значения из settings
                up = 0
                down = 0
            
            clients_data.append({
                "email": email,
                "client_id": client_id,
                "enable": enable,
                "expiry_time": expiry_time,
                "total_gb": total_gb,
                "up": up,
                "down": down
            })
            
    except json.JSONDecodeError as e:
        print(f"  Ошибка парсинга JSON для inbound {inbound_id}: {e}")
        continue

print(f"  Найдено клиентов: {len(clients_data)}")

# ШАГ 3: Получаем admin_id из Pasarguard
print("\n[ШАГ 3] Получение admin_id из Pasarguard...")
pasar_cur.execute("SELECT id FROM admins LIMIT 1")
admin_result = pasar_cur.fetchone()
if not admin_result:
    print("ОШИБКА: не найден администратор в базе данных!")
    xui.close()
    pasar.close()
    exit(1)

admin_id = admin_result[0]
print(f"  Используется admin_id: {admin_id}")

# ШАГ 4: Получаем group_id
print("\n[ШАГ 4] Получение group_id...")
pasar_cur.execute("SELECT id FROM groups LIMIT 1")
group_result = pasar_cur.fetchone()
if not group_result:
    print("  Предупреждение: группа не найдена, пользователи будут без группы")
    group_id = None
else:
    group_id = group_result[0]
    print(f"  Используется group_id: {group_id}")

# ШАГ 5: Импорт пользователей
print("\n[ШАГ 5] Импорт пользователей в Pasarguard...")

imported = 0
updated = 0
skipped = 0
errors = 0

# Определяем плейсхолдеры для параметров в зависимости от типа БД
# Используем %s везде, метод execute адаптирует для SQLite
pasar_placeholder = '%s'

for client in clients_data:
    email = client["email"]
    client_id = client["client_id"]
    enabled = client["enable"]
    expiry_time = client["expiry_time"]
    total_gb = client["total_gb"]
    up = client["up"]
    down = client["down"]
    used_traffic = up + down
    
    if not email:
        skipped += 1
        continue
    
    # проверяем, существует ли уже пользователь
    pasar_cur.execute("SELECT id FROM users WHERE username = %s", (email,))
    existing = pasar_cur.fetchone()
    
    if existing:
        user_id = existing[0]
        # обновляем существующего пользователя
        try:
            # получаем текущие proxy_settings
            pasar_cur.execute("SELECT proxy_settings FROM users WHERE id = %s", (user_id,))
            result = pasar_cur.fetchone()
            if result and result[0]:
                proxy_settings = json.loads(result[0])
            else:
                proxy_settings = {}
            
            # ВСЕГДА устанавливаем created_at на текущую дату миграции
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            
            # обновляем proxy_settings если нужно
            needs_update = False
            
            # генерируем валидные UUID для VMess и VLESS
            if is_valid_uuid(client_id):
                valid_uuid = client_id
            else:
                # если UUID уже есть в proxy_settings, используем его, иначе генерируем новый
                if "vmess" in proxy_settings and is_valid_uuid(proxy_settings["vmess"].get("id", "")):
                    valid_uuid = proxy_settings["vmess"]["id"]
                elif "vless" in proxy_settings and is_valid_uuid(proxy_settings["vless"].get("id", "")):
                    valid_uuid = proxy_settings["vless"]["id"]
                else:
                    valid_uuid = str(uuid.uuid4())
                    needs_update = True
            
            # генерируем пароли если их нет или они невалидные
            if "shadowsocks" not in proxy_settings or not proxy_settings["shadowsocks"].get("password") or len(proxy_settings["shadowsocks"].get("password", "")) < 22:
                ss_password = generate_password(22)
                needs_update = True
            else:
                ss_password = proxy_settings["shadowsocks"]["password"]
            
            if "trojan" not in proxy_settings or not proxy_settings["trojan"].get("password") or len(proxy_settings["trojan"].get("password", "")) < 22:
                trojan_password = generate_password(22)
                needs_update = True
            else:
                trojan_password = proxy_settings["trojan"].get("password", "")
            
            # обновляем proxy_settings (vmess первым)
            if needs_update:
                proxy_settings = {
                    "vmess": {"id": valid_uuid},
                    "vless": {"id": valid_uuid, "flow": "xtls-rprx-vision"},
                    "trojan": {"password": trojan_password},
                    "shadowsocks": {"password": ss_password, "method": "chacha20-ietf-poly1305"}
                }
            
            # обновляем пользователя
            status = "active" if enabled else "disabled"
            expire_date = None
            if expiry_time and expiry_time > 0:
                if expiry_time > 10000000000:
                    expiry_time = expiry_time / 1000
                expire_date = datetime.fromtimestamp(expiry_time).strftime("%Y-%m-%d %H:%M:%S.000000")
            
            traffic_limit = total_gb * (1024**3) if total_gb else None
            
            # Адаптируем запрос для разных БД
            # ВСЕГДА обновляем created_at на текущую дату миграции
            update_query = """
                UPDATE users SET
                    status = %s,
                    used_traffic = %s,
                    data_limit = %s,
                    expire = %s,
                    proxy_settings = %s,
                    created_at = %s,
                    edit_at = %s
                WHERE id = %s
            """
            pasar_cur.execute(update_query, (
                status,
                used_traffic,
                traffic_limit,
                expire_date,
                json.dumps(proxy_settings),
                created_at,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                user_id
            ))
            
            updated += 1
            print(f"  Обновлен: {email}")
            
        except Exception as e:
            print(f"  ОШИБКА при обновлении {email}: {e}")
            errors += 1
        continue
    
    # создаем нового пользователя
    try:
        # генерируем валидные UUID для VMess и VLESS
        if is_valid_uuid(client_id):
            valid_uuid = client_id
        else:
            valid_uuid = str(uuid.uuid4())
        
        # генерируем валидные пароли
        ss_password = generate_password(22)
        trojan_password = generate_password(22)
        
        # создаем proxy_settings (vmess первым)
        proxy_settings = {
            "vmess": {"id": valid_uuid},
            "vless": {"id": valid_uuid, "flow": "xtls-rprx-vision"},
            "trojan": {"password": trojan_password},
            "shadowsocks": {"password": ss_password, "method": "chacha20-ietf-poly1305"}
        }
        
        # определяем статус
        status = "active" if enabled else "disabled"
        
        # конвертируем expiry_time в DATETIME
        expire_date = None
        if expiry_time and expiry_time > 0:
            if expiry_time > 10000000000:
                expiry_time = expiry_time / 1000
            expire_date = datetime.fromtimestamp(expiry_time).strftime("%Y-%m-%d %H:%M:%S.000000")
        
        traffic_limit = total_gb * (1024**3) if total_gb else None
        
        # Адаптируем запрос для разных БД
        insert_query = """
            INSERT INTO users (
                username, status, used_traffic, data_limit, created_at, 
                admin_id, data_limit_reset_strategy, expire, proxy_settings
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Для PostgreSQL нужно получить lastrowid по-другому
        if pasar_config['db_type'] in ['postgresql', 'postgres']:
            insert_query += " RETURNING id"
        
        pasar_cur.execute(insert_query, (
            email,
            status,
            used_traffic,
            traffic_limit,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            admin_id,
            "no_reset",
            expire_date,
            json.dumps(proxy_settings)
        ))
        
        # Получаем ID вставленной строки
        if pasar_config['db_type'] in ['postgresql', 'postgres']:
            result = pasar_cur.fetchone()
            user_id = result[0] if result else None
        else:
            user_id = pasar_cur.lastrowid
        
        # привязываем к группе если группа существует
        if group_id and user_id:
            try:
                # Адаптируем INSERT OR IGNORE для разных БД
                if pasar_config['db_type'] == 'sqlite':
                    insert_group_query = """
                        INSERT OR IGNORE INTO users_groups_association (user_id, groups_id)
                        VALUES (%s, %s)
                    """
                elif pasar_config['db_type'] in ['postgresql', 'postgres']:
                    insert_group_query = """
                        INSERT INTO users_groups_association (user_id, groups_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """
                else:  # MySQL/MariaDB
                    insert_group_query = """
                        INSERT IGNORE INTO users_groups_association (user_id, groups_id)
                        VALUES (%s, %s)
                    """
                
                pasar_cur.execute(insert_group_query, (user_id, group_id))
            except Exception as e:
                print(f"  Предупреждение: не удалось привязать к группе: {e}")
        
        imported += 1
        print(f"  Импортирован: {email} (traffic: {used_traffic})")
        
    except Exception as e:
        print(f"  ОШИБКА при импорте {email}: {e}")
        errors += 1

# сохраняем изменения
pasar.commit()

# закрываем соединения
xui.close()
pasar.close()

# итоговая статистика
print("\n" + "=" * 80)
print("МИГРАЦИЯ ЗАВЕРШЕНА!")
print("=" * 80)
print(f"Импортировано новых пользователей: {imported}")
print(f"Обновлено существующих пользователей: {updated}")
print(f"Пропущено: {skipped}")
print(f"Ошибок: {errors}")
print(f"Всего обработано: {imported + updated + skipped + errors}")
print("\nВАЖНО: После миграции перезапустите Pasarguard для синхронизации с core config!")
print("=" * 80)
