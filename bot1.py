# -*- coding: utf-8 -*-
"""
bot1.py — Telegram-бот-хостинг Python файлов
Часть 1: настройки, база, пользователи, заморозка
"""

import os
import sys
import time
import pysqlite3

# маршрутизируем sqlite3 на pysqlite3
sys.modules['sqlite3'] = pysqlite3

import sqlite3
import zipfile
import threading
import ast
import urllib.parse
import traceback
import subprocess
import random
import string
import psutil
from typing import List, Set, Tuple
from telebot import TeleBot, types
from telebot.apihelper import ApiTelegramException

# ---------------- SETTINGS ----------------
TOKEN = "8277699763:AAGee0xepUOj0qiRF28XnFxXHVi06RMDpCk"
OWNER_ID = 7328238543

BASE_DIR = "/tmp"
DATA_DIR = "/tmp/data"
FILES_DIR = "/tmp/files"
QUARANTINE_DIR = "/tmp/quarantine"
DB_PATH = "/tmp/db.sqlite3"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(FILES_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

PLANS = {
    "free": {"name": "🪶 Бесплатный", "limit": 2, "ram_mb": 512, "cpu_percent": 5, "total_ram_mb": 800, "total_cpu_percent": 8},
    "premium": {"name": "💎 Премиум", "limit": 20, "ram_mb": 1024, "cpu_percent": 15, "total_ram_mb": 2048, "total_cpu_percent": 25},
    "maximum": {"name": "🚀 Максимальный", "limit": 50, "ram_mb": 2048, "cpu_percent": 30, "total_ram_mb": 4096, "total_cpu_percent": 50},
}

# Опасные модули Python
FORBIDDEN_PYTHON_MODULES: Set[str] = {
    "ctypes", "pty", "resource", "socket", "multiprocessing", 
    "os", "sys", "subprocess", "shutil", "platform", "importlib"
}

FORBIDDEN_PYTHON_PATTERNS: Set[str] = {
    "exec(", "eval(", "os.system", "subprocess.popen", "base64.b64decode",
    "fork(", "shutil.rmtree", "pty.", "ctypes.", "__import__", "compile(",
    "open(", "input(", "breakpoint", "globals()", "locals()", "vars()",
    "dir()", "help()", "memoryview(", "bytearray(", "bytes(", "str.format(",
    "getattr(", "setattr(", "delattr(", "hasattr(", "isinstance(", "type("
}

# Опасные конструкции JavaScript
FORBIDDEN_JS_PATTERNS: Set[str] = {
    "eval(", "Function(", "setTimeout(", "setInterval(", "setImmediate(",
    "exec(", "spawn(", "fork(", "execSync(", "spawnSync(", "forkSync(",
    "require('child_process')", "require('fs')", "require('os')",
    "require('process')", "require('vm')", "require('module')",
    "require('net')", "require('http')", "require('https')",
    "require('dns')", "require('tls')", "require('cluster')",
    "process.exit", "process.kill", "process.abort",
    "fs.readFile", "fs.writeFile", "fs.appendFile", "fs.unlink",
    "fs.rmdir", "fs.mkdir", "fs.rename", "fs.chmod", "fs.chown",
    "os.platform", "os.arch", "os.cpus", "os.totalmem", "os.freemem",
    "vm.runInThisContext", "vm.runInNewContext", "vm.createContext",
    "module._load", "require.main", "require.cache",
    "global.", "globalThis.", "window.", "document.", "XMLHttpRequest",
    "fetch(", "WebSocket", "localStorage", "sessionStorage",
    "indexedDB", "crypto.subtle", "crypto.getRandomValues",
    "import(", "import.meta", "export ", "module.exports",
    "__dirname", "__filename", "require.resolve",
    "Buffer(", "new Buffer", "Buffer.alloc", "Buffer.from",
    "atob(", "btoa(", "escape(", "unescape(",
    "console.trace", "console.debug", "console.profile",
    "debugger", "with (", "delete ", "void ", "typeof ", "instanceof "
}

# Опасные npm пакеты
DANGEROUS_NPM_PACKAGES = {
    'child_process', 'fs', 'os', 'process', 'vm', 'module', 'net', 
    'http', 'https', 'dns', 'tls', 'cluster', 'worker_threads',
    'readline', 'repl', 'v8', 'perf_hooks', 'async_hooks',
    'inspector', 'trace_events', 'wasi', 'worker',
    # Известные вредоносные пакеты
    'eslint-config-', 'eslint-plugin-', 'webpack-bundle-',
    'cross-env.js', 'jquery.js', 'babel-preset-', 'react-scripts-'
}

# Опасные команды в scripts
DANGEROUS_NPM_SCRIPTS = {
    'rm -rf', 'rm -fr', 'rm -f', 'del ', 'remove ', 'format c:',
    'shutdown', 'reboot', 'init', 'kill', 'pkill', 'taskkill',
    'chmod', 'chown', 'mv ', 'cp ', 'dd ', 'cat ', 'echo ',
    'wget', 'curl', 'nc ', 'netcat', 'ssh', 'scp', 'sftp',
    'python ', 'python3 ', 'node ', 'npm ', 'yarn ',
    'eval', 'exec', 'child_process', 'process.exit',
    'require(', 'import(', 'fs.', 'os.', 'net.'
}

SUPPORTED_EXT = (".py", ".zip", ".js")

bot = TeleBot(TOKEN, parse_mode="HTML")

ADMIN_FLOW = {}    # {admin_id: {"mode":..., "data":{}}}
INSTALL_FLOW = {}  # {user_id: {"target_uid": int}}
RUNNING = {}       # {(uid,fname):{"proc":Popen,"started":int,"log":path}}
UPLOAD_FLOW = set() # {user_id}
PROMO_FLOW = set() # {user_id}

# ---------------- MONITORING ----------------
class ResourceMonitor:
    def __init__(self):
        self.process_stats = {}
        self.last_check = time.time()
    
    def get_user_total_usage(self, uid: int) -> Tuple[float, float]:
        """Возвращает суммарное использование CPU и RAM пользователем"""
        total_cpu = 0.0
        total_ram = 0.0
        
        for key, stats in self.process_stats.items():
            if key[0] == uid:  # процессы этого пользователя
                if time.time() - stats['last_update'] < 30:  # только активные
                    total_cpu += stats['cpu']
                    total_ram += stats['ram_mb']
        
        return total_cpu, total_ram
    
    def update_process_stats(self):
        """Обновляет статистику по всем запущенным процессам"""
        current_time = time.time()
        if current_time - self.last_check < 5:  # Проверяем каждые 5 секунд
            return
        
        self.last_check = current_time
        
        # Сначала собираем статистику по всем процессам
        for key, info in list(RUNNING.items()):
            proc = info.get("proc")
            if proc and proc.poll() is None:
                try:
                    # Получаем информацию о процессе
                    ps_proc = psutil.Process(proc.pid)
                    cpu_percent = ps_proc.cpu_percent()
                    memory_info = ps_proc.memory_info()
                    memory_mb = memory_info.rss / 1024 / 1024
                    
                    self.process_stats[key] = {
                        'cpu': cpu_percent,
                        'ram_mb': memory_mb,
                        'last_update': current_time
                    }
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    # Процесс уже завершился
                    if key in self.process_stats:
                        del self.process_stats[key]
        
        # Теперь проверяем лимиты для каждого процесса
        for key, stats in list(self.process_stats.items()):
            if time.time() - stats['last_update'] > 30:  # пропускаем старые
                continue
                
            uid = key[0]
            user_ram_limit = plan_ram_mb(uid)
            user_cpu_limit = plan_cpu_percent(uid)
            user_total_ram_limit = plan_total_ram_mb(uid)
            user_total_cpu_limit = plan_total_cpu_percent(uid)
            
            # Получаем суммарное использование пользователя
            total_cpu, total_ram = self.get_user_total_usage(uid)
            
            reason = None
            
            # 1. Проверка индивидуальных лимитов процесса
            if stats['ram_mb'] > user_ram_limit:
                reason = f"Индивидуальный лимит RAM: {stats['ram_mb']:.1f}MB/{user_ram_limit}MB"
            elif stats['cpu'] > user_cpu_limit:
                reason = f"Индивидуальный лимит CPU: {stats['cpu']:.1f}%/{user_cpu_limit}%"
            # 2. Проверка суммарных лимитов пользователя
            elif total_ram > user_total_ram_limit:
                reason = f"Суммарный лимит RAM: {total_ram:.1f}MB/{user_total_ram_limit}MB"
            elif total_cpu > user_total_cpu_limit:
                reason = f"Суммарный лимит CPU: {total_cpu:.1f}%/{user_total_cpu_limit}%"
            
            if reason:
                # Находим самый ресурсоемкий процесс пользователя для остановки
                worst_process = self.find_worst_process(uid)
                if worst_process:
                    self.kill_process(worst_process, reason)
    
    def find_worst_process(self, uid: int):
        """Находит самый ресурсоемкий процесс пользователя"""
        user_processes = []
        
        for key, stats in self.process_stats.items():
            if key[0] == uid and time.time() - stats['last_update'] < 30:
                # Считаем "тяжесть" процесса как сумму нормализованных CPU и RAM
                cpu_weight = stats['cpu'] / plan_cpu_percent(uid)
                ram_weight = stats['ram_mb'] / plan_ram_mb(uid)
                total_weight = cpu_weight + ram_weight
                
                user_processes.append((key, total_weight, stats))
        
        if user_processes:
            # Сортируем по убыванию "тяжести" и берем самый тяжелый
            user_processes.sort(key=lambda x: x[1], reverse=True)
            return user_processes[0][0]
        
        return None
    
    def kill_process(self, key, reason):
        """Останавливает процесс с указанием причины"""
        uid, fname = key
        try:
            proc = RUNNING[key]["proc"]
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except Exception:
                proc.kill()
        except Exception:
            pass
        
        RUNNING.pop(key, None)
        if key in self.process_stats:
            del self.process_stats[key]
        
        # Уведомляем пользователя
        bot.send_message(uid, f"🛑 Процесс {fname} остановлен: {reason}")
    
    def get_system_stats(self):
        """Возвращает общую статистику системы"""
        total_users = 0
        active_scripts = 0
        total_cpu = 0.0
        total_ram = 0.0
        
        with db_connect() as conn:
            total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        
        for key, stats in self.process_stats.items():
            if time.time() - stats['last_update'] < 30:  # Только активные процессы
                active_scripts += 1
                total_cpu += stats['cpu']
                total_ram += stats['ram_mb']
        
        return {
            'total_users': total_users,
            'active_scripts': active_scripts,
            'total_cpu_usage': total_cpu,
            'total_ram_usage_mb': total_ram
        }

# Создаем монитор ресурсов
resource_monitor = ResourceMonitor()

# ---------------- DB ----------------
def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                plan TEXT DEFAULT 'free',
                expiry INTEGER DEFAULT 0,
                banned INTEGER DEFAULT 0,
                frozen INTEGER DEFAULT 0,
                protection_disabled INTEGER DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS files(
                user_id INTEGER,
                filename TEXT,
                imports TEXT DEFAULT '',
                PRIMARY KEY(user_id, filename)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quarantine(
                qid INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                filename TEXT,
                imports TEXT,
                saved_path TEXT,
                status TEXT DEFAULT 'pending',
                created INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS promocodes(
                code TEXT PRIMARY KEY,
                plan TEXT NOT NULL,
                days INTEGER NOT NULL,
                max_activations INTEGER DEFAULT 1,
                activations_count INTEGER DEFAULT 0,
                used_by INTEGER DEFAULT NULL,
                used_at INTEGER DEFAULT NULL,
                created_at INTEGER NOT NULL,
                created_by INTEGER NOT NULL
            )
        """)
        conn.commit()

# ---------------- USERS ----------------
def ensure_user(uid: int):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO users(user_id) VALUES(?)", (uid,))
        conn.commit()

def get_user(uid: int):
    ensure_user(uid)
    with db_connect() as conn:
        cur = conn.cursor()
        return cur.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

def set_user_plan(uid: int, plan: str, days: int):
    ensure_user(uid)
    now = int(time.time())
    expiry = 0 if plan == "free" else now + max(1, days) * 86400
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET plan=?, expiry=? WHERE user_id=?", (plan, expiry, uid))
        if plan != "free":
            cur.execute("UPDATE users SET frozen=0 WHERE user_id=?", (uid,))
        conn.commit()
    check_and_update_freeze(uid)

def add_days_to_subscription(uid: int, days: int):
    """Добавляет дни к текущей подписке пользователя"""
    u = get_user(uid)
    if u["plan"] == "free":
        return False
    
    now = int(time.time())
    current_expiry = u["expiry"]
    if current_expiry < now:
        new_expiry = now + days * 86400
    else:
        new_expiry = current_expiry + days * 86400
    
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET expiry=? WHERE user_id=?", (new_expiry, uid))
        conn.commit()
    
    return True

def reset_user_subscription(uid: int):
    """Обнуляет подписку пользователя (переводит на бесплатный тариф)"""
    ensure_user(uid)
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET plan='free', expiry=0 WHERE user_id=?", (uid,))
        conn.commit()
    check_and_update_freeze(uid)

def get_subscription_days_left(uid: int) -> int:
    """
    Возвращает количество оставшихся дней подписки
    """
    u = get_user(uid)
    if u["plan"] == "free":
        return 0  # Бесплатный план не имеет срока
    
    expiry = u["expiry"]
    if expiry == 0:
        return 0
    
    now = int(time.time())
    if now >= expiry:
        return 0  # Подписка истекла
    
    days_left = (expiry - now) // 86400  # Конвертируем секунды в дни
    return max(0, days_left)

def set_ban(uid: int, val: int):
    ensure_user(uid)
    with db_connect() as conn:
        conn.execute("UPDATE users SET banned=? WHERE user_id=?", (val, uid))
        conn.commit()

def set_protection_disabled(uid: int, val: int):
    ensure_user(uid)
    with db_connect() as conn:
        conn.execute("UPDATE users SET protection_disabled=? WHERE user_id=?", (val, uid))
        conn.commit()

def count_user_files(uid: int) -> int:
    with db_connect() as conn:
        r = conn.execute("SELECT COUNT(*) AS c FROM files WHERE user_id=?", (uid,)).fetchone()
        return int(r["c"]) if r else 0

# ---------------- PROMOCODES ----------------
def create_promocode(code: str, plan: str, days: int, created_by: int, max_activations: int = 1):
    """Создает промокод"""
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO promocodes(code, plan, days, max_activations, activations_count, created_at, created_by) VALUES(?,?,?,?,?,?,?)",
            (code, plan, days, max_activations, 0, int(time.time()), created_by)
        )
        conn.commit()

def get_promocode(code: str):
    """Получает информацию о промокоде"""
    with db_connect() as conn:
        return conn.execute("SELECT * FROM promocodes WHERE code=?", (code,)).fetchone()

def use_promocode(code: str, used_by: int):
    """Активирует промокод"""
    with db_connect() as conn:
        cur = conn.cursor()
        # Проверяем не использовал ли уже пользователь этот промокод
        cur.execute("SELECT * FROM promocodes WHERE code=? AND used_by=?", (code, used_by))
        if cur.fetchone():
            return False, "❌ Вы уже активировали этот промокод"
        
        # Проверяем лимит активаций
        cur.execute("SELECT activations_count, max_activations FROM promocodes WHERE code=?", (code,))
        result = cur.fetchone()
        if result and result["activations_count"] >= result["max_activations"]:
            return False, "❌ Лимит активаций промокода исчерпан"
        
        # Активируем промокод
        cur.execute(
            "UPDATE promocodes SET used_by=?, used_at=?, activations_count = activations_count + 1 WHERE code=? AND used_by IS NULL",
            (used_by, int(time.time()), code)
        )
        conn.commit()
        return (cur.rowcount > 0, "✅ Промокод активирован")

def generate_promocode(length=8):
    """Генерирует случайный промокод"""
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def can_apply_promocode(user_plan: str, promo_plan: str) -> Tuple[bool, str]:
    """
    Проверяет можно ли применить промокод к текущему плану пользователя
    Возвращает (можно_ли_применить, сообщение_об_ошибке)
    """
    plan_hierarchy = {"free": 0, "premium": 1, "maximum": 2}
    
    user_level = plan_hierarchy.get(user_plan, 0)
    promo_level = plan_hierarchy.get(promo_plan, 0)
    
    if user_level > promo_level:
        return False, f"❌ У вас уже активен тариф {PLANS[user_plan]['name']}, который лучше чем {PLANS[promo_plan]['name']}"
    elif user_level == promo_level:
        return True, f"✅ Промокод добавит дни к вашему текущему тарифу {PLANS[user_plan]['name']}"
    else:
        return True, f"✅ Промокод повысит ваш тариф до {PLANS[promo_plan]['name']}"

# ---------------- FILES ----------------
def user_dir(uid: int) -> str:
    path = os.path.join(FILES_DIR, str(uid))
    os.makedirs(path, exist_ok=True)
    return path

def quarantine_path(filename: str) -> str:
    ts = int(time.time())
    safe = f"{ts}_{urllib.parse.quote_plus(filename)}"
    return os.path.join(QUARANTINE_DIR, safe)

def list_files(uid: int) -> List[str]:
    with db_connect() as conn:
        rows = conn.execute("SELECT filename FROM files WHERE user_id=?", (uid,)).fetchall()
    return [r["filename"] for r in rows]

def add_file_record(uid: int, filename: str, imports: List[str]):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO files(user_id, filename, imports) VALUES(?,?,?)",
            (uid, filename, ",".join(imports or []))
        )
        conn.commit()
    check_and_update_freeze(uid)

def remove_file_record(uid: int, filename: str):
    with db_connect() as conn:
        conn.execute("DELETE FROM files WHERE user_id=? AND filename=?", (uid, filename))
        conn.commit()
    check_and_update_freeze(uid)

# ---------------- QUARANTINE FUNCTIONS ----------------
def get_quarantine_file(qid: int):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM quarantine WHERE qid=?", (qid,)).fetchone()

def approve_quarantine_file(qid: int):
    """Разрешает файл из карантина и добавляет его пользователю"""
    q_file = get_quarantine_file(qid)
    if not q_file:
        return False
    
    uid = q_file["user_id"]
    filename = q_file["filename"]
    imports = q_file["imports"].split(",") if q_file["imports"] else []
    saved_path = q_file["saved_path"]
    
    # Перемещаем файл из карантина в папку пользователя
    user_path = os.path.join(user_dir(uid), filename)
    try:
        os.rename(saved_path, user_path)
        
        # Добавляем запись в базу
        add_file_record(uid, filename, imports)
        
        # Обновляем статус в карантине
        with db_connect() as conn:
            conn.execute("UPDATE quarantine SET status='approved' WHERE qid=?", (qid,))
            conn.commit()
        
        return True
    except Exception as e:
        print(f"Ошибка при одобрении файла из карантина: {e}")
        return False

# ---------------- FREEZE ----------------
def plan_limit(uid: int) -> int:
    u = get_user(uid)
    return PLANS.get(u["plan"], PLANS["free"])["limit"]

def plan_ram_mb(uid: int) -> int:
    u = get_user(uid)
    return PLANS.get(u["plan"], PLANS["free"])["ram_mb"]

def plan_cpu_percent(uid: int) -> int:
    u = get_user(uid)
    return PLANS.get(u["plan"], PLANS["free"])["cpu_percent"]

def plan_total_ram_mb(uid: int) -> int:
    u = get_user(uid)
    return PLANS.get(u["plan"], PLANS["free"])["total_ram_mb"]

def plan_total_cpu_percent(uid: int) -> int:
    u = get_user(uid)
    return PLANS.get(u["plan"], PLANS["free"])["total_cpu_percent"]

def stop_all_for_user(uid: int):
    keys = [k for k in list(RUNNING.keys()) if k[0] == uid]
    for key in keys:
        try:
            proc = RUNNING[key]["proc"]
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except Exception:
                proc.kill()
        except Exception:
            pass
        RUNNING.pop(key, None)

def check_and_update_freeze(uid: int):
    total = count_user_files(uid)
    limit = plan_limit(uid)
    frozen = 1 if total > limit else 0
    u = get_user(uid)
    prev = u["frozen"]
    if prev == frozen:
        return
    with db_connect() as conn:
        conn.execute("UPDATE users SET frozen=? WHERE user_id=?", (frozen, uid))
        conn.commit()
    if prev == 1 and frozen == 0:
        bot.send_message(uid, "✅ Лимит в норме. Файлы разморожены.")
    elif prev == 0 and frozen == 1:
        stop_all_for_user(uid)
        bot.send_message(
            uid,
            "🧊 Превышен лимит тарифа. Все файлы заморожены, скрипты остановлены.\n"
            "Удалите лишние файлы, чтобы продолжить работу."
        )

# ---------------- SECURITY SCAN ----------------
def extract_python_imports(code: str) -> List[str]:
    """
    Быстро и безопасно достаёт список импортов из Python-кода.
    """
    try:
        tree = ast.parse(code)
    except Exception:
        return []
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])
    return list(imports)

def extract_js_requires(code: str) -> List[str]:
    """
    Извлекает require() и import из JavaScript кода.
    """
    imports = set()
    lines = code.split('\n')
    for line in lines:
        line = line.strip()
        # Поиск require('module')
        if 'require(' in line:
            start = line.find('require(') + 8
            if start < len(line):
                # Ищем строку в кавычках
                quote_char = None
                for i in range(start, len(line)):
                    if line[i] in ['"', "'", '`']:
                        quote_char = line[i]
                        module_start = i + 1
                        break
                
                if quote_char:
                    module_end = line.find(quote_char, module_start)
                    if module_end != -1:
                        module_name = line[module_start:module_end]
                        imports.add(module_name)
        
        # Поиск import from 'module'
        if 'import' in line and 'from' in line:
            parts = line.split('from')
            if len(parts) > 1:
                module_part = parts[1].strip()
                if module_part and module_part[0] in ['"', "'", '`']:
                    end_quote = module_part.find(module_part[0], 1)
                    if end_quote != -1:
                        module_name = module_part[1:end_quote]
                        imports.add(module_name)
        
        # Поиск import 'module'
        if 'import' in line and 'from' not in line:
            import_part = line.split('import')[1].strip()
            if import_part and import_part[0] in ['"', "'", '`']:
                end_quote = import_part.find(import_part[0], 1)
                if end_quote != -1:
                    module_name = import_part[1:end_quote]
                    imports.add(module_name)
    
    return list(imports)

def scan_package_json(package_content: str) -> Tuple[bool, List[str]]:
    """
    Сканирует package.json на наличие опасных модулей
    """
    import json
    dangerous_found = []
    
    try:
        data = json.loads(package_content)
        
        # Проверяем dependencies
        if 'dependencies' in data:
            for dep in data['dependencies']:
                if dep in DANGEROUS_NPM_PACKAGES:
                    dangerous_found.append(f"dependency: {dep}")
        
        # Проверяем devDependencies
        if 'devDependencies' in data:
            for dep in data['devDependencies']:
                if dep in DANGEROUS_NPM_PACKAGES:
                    dangerous_found.append(f"devDependency: {dep}")
        
        # Проверяем scripts на опасные команды
        if 'scripts' in data:
            for script_name, script_cmd in data['scripts'].items():
                script_cmd_lower = script_cmd.lower()
                for dangerous_cmd in DANGEROUS_NPM_SCRIPTS:
                    if dangerous_cmd in script_cmd_lower:
                        dangerous_found.append(f"script '{script_name}': {dangerous_cmd}")
        
        return (len(dangerous_found) > 0, dangerous_found)
    
    except json.JSONDecodeError:
        return (False, ["Невалидный JSON"])
    except Exception as e:
        return (False, [f"Ошибка анализа: {str(e)}"])

def scan_for_forbidden(code: str, file_extension: str) -> Tuple[bool, List[str]]:
    """
    Проверяет код на запрещённые модули или конструкции.
    """
    bad_found = []
    
    if file_extension == '.py':
        imports = extract_python_imports(code)
        
        # Проверка модулей Python
        for imp in imports:
            if imp in FORBIDDEN_PYTHON_MODULES:
                bad_found.append(f"Python модуль: {imp}")
        
        # Проверка опасных конструкций Python
        code_lower = code.lower()
        for pat in FORBIDDEN_PYTHON_PATTERNS:
            if pat in code_lower:
                bad_found.append(f"Python конструкция: {pat}")
    
    elif file_extension == '.js':
        imports = extract_js_requires(code)
        
        # Проверка опасных модулей JavaScript
        dangerous_js_modules = {
            'child_process', 'fs', 'os', 'process', 'vm', 'module', 
            'net', 'http', 'https', 'dns', 'tls', 'cluster'
        }
        for imp in imports:
            if imp in dangerous_js_modules:
                bad_found.append(f"JavaScript модуль: {imp}")
        
        # Проверка опасных конструкций JavaScript
        for pat in FORBIDDEN_JS_PATTERNS:
            if pat in code:
                bad_found.append(f"JavaScript конструкция: {pat}")
    
    return (len(bad_found) > 0, bad_found)

def add_quarantine(uid: int, filename: str, imports: List[str], saved_path: str) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO quarantine(user_id, filename, imports, saved_path, created) VALUES(?,?,?,?,?)",
            (uid, filename, ",".join(imports), saved_path, int(time.time()))
        )
        conn.commit()
        return cur.lastrowid

# ---------------- FILE UPLOAD ----------------
@bot.message_handler(content_types=['document'])
def handle_upload(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    if u["banned"]:
        return bot.reply_to(msg, "🚫 Вы заблокированы в системе.")
    if u["frozen"]:
        return bot.reply_to(msg, "🧊 У вас превышен лимит, загрузка недоступна.")
    if not msg.document:
        return bot.reply_to(msg, "Отправь .py, .js или .zip файл.")
    name = msg.document.file_name
    if not name.endswith(SUPPORTED_EXT):
        return bot.reply_to(msg, "⚠️ Можно загружать только .py, .js или .zip файлы.")

    # сохраняем файл
    file_info = bot.get_file(msg.document.file_id)
    downloaded = bot.download_file(file_info.file_path)
    save_dir = user_dir(uid)
    save_path = os.path.join(save_dir, name)
    with open(save_path, "wb") as f:
        f.write(downloaded)

    # если архив — распаковываем и проверяем
    imported_all = []
    package_json_found = False
    package_json_dangerous = False
    package_json_warnings = []
    
    if name.endswith(".zip"):
        with zipfile.ZipFile(save_path, "r") as zip_ref:
            zip_ref.extractall(save_dir)
        os.remove(save_path)
        
        # Проверяем наличие package.json
        package_json_path = os.path.join(save_dir, "package.json")
        if os.path.exists(package_json_path):
            package_json_found = True
            with open(package_json_path, "r", encoding="utf-8", errors="ignore") as f:
                package_content = f.read()
            
            dangerous, warnings = scan_package_json(package_content)
            if dangerous:
                package_json_dangerous = True
                package_json_warnings = warnings
        
        # Обрабатываем файлы
        for inner in os.listdir(save_dir):
            if inner.endswith((".py", ".js")):
                p = os.path.join(save_dir, inner)
                with open(p, "r", encoding="utf-8", errors="ignore") as f:
                    code = f.read()
                
                file_ext = '.py' if inner.endswith('.py') else '.js'
                if file_ext == '.py':
                    imports = extract_python_imports(code)
                else:
                    imports = extract_js_requires(code)
                
                bad, bads = scan_for_forbidden(code, file_ext)
                imported_all.extend(imports)
                
                # Если package.json опасный или файл опасный - в карантин
                if (bad or package_json_dangerous) and not u["protection_disabled"]:
                    qpath = quarantine_path(inner)
                    os.rename(p, qpath)
                    
                    # Добавляем предупреждения из package.json если есть
                    all_warnings = bads + package_json_warnings
                    qid = add_quarantine(uid, inner, imports, qpath)
                    
                    warning_msg = f"🚫 Найдена запрещённая библиотека в {inner}: {', '.join(all_warnings)}"
                    if package_json_dangerous:
                        warning_msg += f"\n⚠️ Обнаружен опасный package.json"
                    
                    bot.send_message(uid, warning_msg)
                    bot.send_message(OWNER_ID,
                        f"⚠️ Пользователь <a href='tg://user?id={uid}'>{uid}</a> "
                        f"попытался загрузить опасный код.\n"
                        f"<b>Файл:</b> {inner}\n"
                        f"<b>Тип:</b> {file_ext}\n"
                        f"<b>Package.json:</b> {'ОПАСНЫЙ' if package_json_dangerous else 'безопасный'}\n"
                        f"<b>Обнаружено:</b> {', '.join(all_warnings)}",
                        parse_mode='HTML',
                        reply_markup=types.InlineKeyboardMarkup(row_width=3).add(
                            types.InlineKeyboardButton("🚫 Забанить", callback_data=f"adm_ban:{uid}"),
                            types.InlineKeyboardButton("🚷 Игнорировать", callback_data=f"adm_ignore:{qid}"),
                            types.InlineKeyboardButton("✅ Разрешить", callback_data=f"adm_approve:{qid}")
                        )
                    )
                    continue
                
                add_file_record(uid, inner, imports)
    else:
        with open(save_path, "r", encoding="utf-8", errors="ignore") as f:
            code = f.read()
        
        file_ext = '.py' if name.endswith('.py') else '.js'
        if file_ext == '.py':
            imports = extract_python_imports(code)
        else:
            imports = extract_js_requires(code)
        
        bad, bads = scan_for_forbidden(code, file_ext)
        if bad and not u["protection_disabled"]:
            qpath = quarantine_path(name)
            os.rename(save_path, qpath)
            qid = add_quarantine(uid, name, imports, qpath)
            bot.send_message(uid, f"🚫 Найдена запрещённая библиотека: {', '.join(bads)}")
            bot.send_message(OWNER_ID,
                f"⚠️ Пользователь <a href='tg://user?id={uid}'>{uid}</a> "
                f"попытался загрузить опасный код.\n"
                f"<b>Файл:</b> {name}\n"
                f"<b>Тип:</b> {file_ext}\n"
                f"<b>Обнаружено:</b> {', '.join(bads)}",
                parse_mode='HTML',
                reply_markup=types.InlineKeyboardMarkup(row_width=3).add(
                    types.InlineKeyboardButton("🚫 Забанить", callback_data=f"adm_ban:{uid}"),
                    types.InlineKeyboardButton("🚷 Игнорировать", callback_data=f"adm_ignore:{qid}"),
                    types.InlineKeyboardButton("✅ Разрешить", callback_data=f"adm_approve:{qid}")
                )
            )
            return
        add_file_record(uid, name, imports)
        imported_all = imports

    # итог
    total = count_user_files(uid)
    limit = plan_limit(uid)
    if total > limit:
        check_and_update_freeze(uid)
        return

    success_msg = f"✅ Загружено: <b>{name}</b>\n📦 Импорты: <code>{', '.join(imported_all)}</code>"
    if package_json_found:
        if package_json_dangerous:
            success_msg += f"\n⚠️ <b>Внимание:</b> package.json содержит опасные зависимости!"
        else:
            success_msg += f"\n📋 package.json проверен и безопасен"
    
    bot.send_message(uid, success_msg, parse_mode="HTML")

# ---------------- UTILS ----------------
def safe_edit_text(func, text, chat_id, msg_id, **kw):
    try:
        func(text, chat_id, msg_id, **kw)
    except ApiTelegramException as e:
        if "message is not modified" in str(e):
            pass
        else:
            raise

def running_status(uid: int, fname: str) -> Tuple[str, str]:
    """
    Возвращает (emoji, текст статуса)
    """
    key = (uid, fname)
    if key in RUNNING:
        proc = RUNNING[key]["proc"]
        if proc.poll() is None:
            started = RUNNING[key]["started"]
            diff = int(time.time() - started)
            mins = round(diff / 60, 1)
            
            # Добавляем информацию об использовании ресурсов
            resource_info = ""
            if key in resource_monitor.process_stats:
                stats = resource_monitor.process_stats[key]
                resource_info = f" (CPU: {stats['cpu']:.1f}%, RAM: {stats['ram_mb']:.1f}MB)"
            
            return "🟢", f"Работает ({mins} мин.){resource_info}"
    return "🔴", "Остановлен"

# ---------------- FILE CONTROL ----------------
def start_script_for_user(uid: int, fname: str):
    """Запуск файла в отдельном процессе"""
    # ПРОВЕРКА ЗАМОРОЗКИ
    u = get_user(uid)
    if u["frozen"]:
        return False, "🧊 Заморожено: запуск недоступен."
    
    path = os.path.join(user_dir(uid), fname)
    if not os.path.exists(path):
        return False, "❌ Файл не найден."
    if (uid, fname) in RUNNING and RUNNING[(uid, fname)]["proc"].poll() is None:
        return False, "⚠️ Уже запущен."
    
    log_path = path + ".log"
    try:
        if fname.endswith('.py'):
            proc = subprocess.Popen(
                [sys.executable, path],
                stdout=open(log_path, "a", encoding="utf-8"),
                stderr=subprocess.STDOUT,
            )
        elif fname.endswith('.js'):
            proc = subprocess.Popen(
                ['node', path],
                stdout=open(log_path, "a", encoding="utf-8"),
                stderr=subprocess.STDOUT,
            )
        else:
            return False, "❌ Неподдерживаемый формат файла."
        
        RUNNING[(uid, fname)] = {"proc": proc, "started": int(time.time()), "log": log_path}
        
        # Обновляем мониторинг ресурсов
        resource_monitor.update_process_stats()
        
        return True, "🟡 Запускается..."
    except Exception as e:
        return False, f"Ошибка запуска: {e}"

def stop_script_for_user(uid: int, fname: str):
    key = (uid, fname)
    if key not in RUNNING:
        return False, "⚠️ Не запущен."
    proc = RUNNING[key]["proc"]
    try:
        proc.terminate()
        proc.wait(timeout=3)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    RUNNING.pop(key, None)
    
    # Удаляем из мониторинга ресурсов
    if key in resource_monitor.process_stats:
        del resource_monitor.process_stats[key]
    
    return True, "🛑 Остановлен."

# ---------------- FILE MENU ----------------
def file_actions_kb(uid: int, fname: str):
    kb = types.InlineKeyboardMarkup(row_width=3)
    key = (uid, fname)
    running = key in RUNNING and RUNNING[key]["proc"].poll() is None
    
    # Первая строка: Запуск/Остановка + Обновить
    if running:
        kb.add(
            types.InlineKeyboardButton("🛑 Стоп", callback_data=f"stop:{fname}"),
            types.InlineKeyboardButton("🔄 Обновить", callback_data=f"refresh:{fname}")
        )
    else:
        kb.add(
            types.InlineKeyboardButton("▶️ Запустить", callback_data=f"run:{fname}"),
            types.InlineKeyboardButton("🔄 Обновить", callback_data=f"refresh:{fname}")
        )
    
    # Вторая строка: Логи + Установить библиотеку
    kb.add(
        types.InlineKeyboardButton("📜 Логи", callback_data=f"log:{fname}"),
        types.InlineKeyboardButton("⚙️ Установить", callback_data=f"install:{fname}")
    )
    
    # Третья строка: Удалить + Назад
    kb.add(
        types.InlineKeyboardButton("🗑️ Удалить", callback_data=f"del:{fname}"),
        types.InlineKeyboardButton("⬅️ Назад", callback_data="back")
    )
    
    return kb

def file_menu_text_kb(uid: int, fname: str):
    emoji, status = running_status(uid, fname)
    file_type = "🐍 Python" if fname.endswith('.py') else "📜 JavaScript" if fname.endswith('.js') else "📄 Файл"
    text = f"{file_type} <b>{fname}</b>\n{emoji} {status}"
    kb = file_actions_kb(uid, fname)
    return text, kb

# ---------------- FILE LIST ----------------
def files_kb(uid: int):
    kb = types.InlineKeyboardMarkup()
    u = get_user(uid)
    frozen = u["frozen"]
    files = list_files(uid)
    if not files:
        kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back"))
        return kb
    for f in files:
        key = (uid, f)
        running = key in RUNNING and RUNNING[key]["proc"].poll() is None
        if frozen:
            prefix = "🧊 "
        else:
            prefix = "🟢 " if running else "⚪ "
        # Добавляем иконку типа файла
        file_icon = "🐍 " if f.endswith('.py') else "📜 " if f.endswith('.js') else "📄 "
        kb.add(types.InlineKeyboardButton(f"{prefix}{file_icon}{f}", callback_data=f"file:{urllib.parse.quote_plus(f)}"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back"))
    return kb

# ---------------- CALLBACKS ----------------
@bot.callback_query_handler(func=lambda c: c.data.startswith("file:"))
def open_file_menu(c):
    uid = c.from_user.id
    fname = urllib.parse.unquote_plus(c.data.split(":", 1)[1])
    text, kb = file_menu_text_kb(uid, fname)
    bot.edit_message_text(text, c.message.chat.id, c.message.id, reply_markup=kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith(("run:", "stop:", "refresh:", "log:", "del:", "install:")) )
def h_file_actions(c):
    cmd, raw = c.data.split(":", 1)
    fname = urllib.parse.unquote_plus(raw)
    uid = c.from_user.id

    # ПРОВЕРКА ЗАМОРОЗКИ ПЕРЕД ЗАПУСКОМ И УСТАНОВКОЙ
    u = get_user(uid)
    if u["frozen"] and cmd in ["run", "install"]:
        return bot.answer_callback_query(c.id, "🧊 Заморожено: операция недоступна.", show_alert=True)

    def redraw():
        text, kb = file_menu_text_kb(uid, fname)
        safe_edit_text(bot.edit_message_text, text, c.message.chat.id, c.message.id, reply_markup=kb, parse_mode="HTML")

    if cmd == "run":
        safe_edit_text(bot.edit_message_text, f"📄 <b>{fname}</b>\n🟡 Запускается…", c.message.chat.id, c.message.id, reply_markup=file_actions_kb(uid, fname))
        ok, txt = start_script_for_user(uid, fname)
        try: bot.answer_callback_query(c.id, txt, show_alert=False)
        except: pass
        redraw()
        return
    if cmd == "stop":
        ok, txt = stop_script_for_user(uid, fname)
        bot.answer_callback_query(c.id, txt, show_alert=not ok)
        redraw()
        return
    if cmd == "refresh":
        # Обновляем статистику ресурсов перед отображением
        resource_monitor.update_process_stats()
        bot.answer_callback_query(c.id, "🔄 Обновлено", show_alert=False)
        redraw()
        return
    if cmd == "log":
        log_path = os.path.join(user_dir(uid), fname + ".log")
        try:
            if os.path.exists(log_path):
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                    text = "".join(lines[-500:]) if lines else "# Логов нет"
                
                if len(text) > 4000:
                    text = text[-4000:]
                
                # Определяем язык для подсветки синтаксиса
                language = "python" if fname.endswith('.py') else "javascript" if fname.endswith('.js') else "text"
                
                # Отправляем логи как код
                if text.strip():
                    # Разбиваем на части если слишком длинное сообщение
                    if len(text) > 4000:
                        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
                        for i, part in enumerate(parts):
                            bot.send_message(c.message.chat.id, f"```{language}\n{part}\n```", parse_mode="Markdown")
                            if i == 0:  # После первой части делаем небольшую паузу
                                time.sleep(0.5)
                    else:
                        bot.send_message(c.message.chat.id, f"```{language}\n{text}\n```", parse_mode="Markdown")
                else:
                    bot.send_message(c.message.chat.id, f"```{language}\n# Логов нет\n```", parse_mode="Markdown")
            else:
                bot.send_message(c.message.chat.id, "```text\n# Файл логов не найден\n```", parse_mode="Markdown")
        except Exception as e:
            error_msg = f"❌ Не удалось прочитать логи: {str(e)}"
            bot.answer_callback_query(c.id, error_msg, show_alert=True)
        return
    if cmd == "del":
        path = os.path.join(user_dir(uid), fname)
        if (uid, fname) in RUNNING:
            stop_script_for_user(uid, fname)
        if os.path.exists(path):
            os.remove(path)
        remove_file_record(uid, fname)
        safe_edit_text(bot.edit_message_text, "📁 Твои файлы:", c.message.chat.id, c.message.id, reply_markup=files_kb(uid))
        bot.answer_callback_query(c.id, "🗑️ Удалено.", show_alert=False)
        return
    if cmd == "install":
        install_text = (
            "⚙️ <b>Установка библиотек</b>\n\n"
            "Поддерживаемые менеджеры:\n"
            "• <code>название</code> - установка через pip (Python)\n"
            "• <code>npm название</code> - установка через npm (JavaScript)\n\n"
            "Примеры:\n"
            "<code>requests</code> - установит Python библиотеку\n"
            "<code>npm express</code> - установит npm пакет\n\n"
            "Введи название библиотеки:"
        )
        bot.send_message(c.message.chat.id, install_text, parse_mode="HTML")
        INSTALL_FLOW[uid] = {"target_uid": uid}
        return

# ---------------- INSTALL ----------------
@bot.message_handler(func=lambda m: m.from_user.id in INSTALL_FLOW)
def handle_install(msg):
    uid = msg.from_user.id
    name = msg.text.strip()
    if "&" in name or ";" in name:
        bot.reply_to(msg, "❌ Символы `&` и `;` запрещены.")
        INSTALL_FLOW.pop(uid, None)
        return
    
    # Определяем тип установки (pip или npm)
    if name.startswith("npm "):
        # npm установка
        package_name = name[4:].strip()
        if "&" in package_name or ";" in package_name:
            bot.reply_to(msg, "❌ Символы `&` и `;` запрещены.")
            INSTALL_FLOW.pop(uid, None)
            return
        
        bot.reply_to(msg, f"📦 Устанавливаю npm пакет: {package_name} ...")
        try:
            proc = subprocess.run(['npm', 'install', package_name], capture_output=True, text=True)
            if proc.returncode == 0:
                bot.reply_to(msg, f"✅ Установлен npm пакет: {package_name}")
            else:
                bot.reply_to(msg, f"⚠️ Ошибка установки npm:\n<pre>{proc.stderr[:4000]}</pre>", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(msg, f"❌ Ошибка npm: {e}")
    else:
        # pip установка
        bot.reply_to(msg, f"📦 Устанавливаю: {name} ...")
        try:
            proc = subprocess.run([sys.executable, "-m", "pip", "install", name], capture_output=True, text=True)
            if proc.returncode == 0:
                bot.reply_to(msg, f"✅ Установлено: {name}")
            else:
                bot.reply_to(msg, f"⚠️ Ошибка установки:\n<pre>{proc.stderr[:4000]}</pre>", parse_mode="HTML")
        except Exception as e:
            bot.reply_to(msg, f"❌ Ошибка: {e}")
    
    INSTALL_FLOW.pop(uid, None)

# ---------------- ADMIN PANEL ----------------
def admin_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("💎 Выдать подписку", callback_data="adm_plan"),
        types.InlineKeyboardButton("🛡️ Вкл/Выкл защиту", callback_data="adm_protect"),
    )
    kb.add(
        types.InlineKeyboardButton("🚫 Заблокировать", callback_data="adm_banuser"),
        types.InlineKeyboardButton("✅ Разблокировать", callback_data="adm_unbanuser")
    )
    kb.add(
        types.InlineKeyboardButton("🔄 Обнулить подписку", callback_data="adm_resetsub"),
        types.InlineKeyboardButton("🎫 Создать промокод", callback_data="adm_promo")
    )
    kb.add(
        types.InlineKeyboardButton("📊 Мониторинг", callback_data="adm_monitor")
    )
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back"))
    return kb

@bot.callback_query_handler(func=lambda c: c.data == "adm_plan")
def adm_plan_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    ADMIN_FLOW[c.from_user.id] = {"mode": "plan_user"}
    bot.send_message(c.message.chat.id, "💎 Введи user_id:")

@bot.callback_query_handler(func=lambda c: c.data == "adm_protect")
def adm_protect_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    ADMIN_FLOW[c.from_user.id] = {"mode": "toggle_protect"}
    bot.send_message(c.message.chat.id, "🛡️ Введи user_id для включения/выключения защиты:")

@bot.callback_query_handler(func=lambda c: c.data == "adm_banuser")
def adm_banuser_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    ADMIN_FLOW[c.from_user.id] = {"mode": "ban_user"}
    bot.send_message(c.message.chat.id, "🚫 Введи user_id для блокировки:")

@bot.callback_query_handler(func=lambda c: c.data == "adm_unbanuser")
def adm_unbanuser_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    ADMIN_FLOW[c.from_user.id] = {"mode": "unban_user"}
    bot.send_message(c.message.chat.id, "✅ Введи user_id для разблокировки:")

@bot.callback_query_handler(func=lambda c: c.data == "adm_resetsub")
def adm_resetsub_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    ADMIN_FLOW[c.from_user.id] = {"mode": "reset_sub"}
    bot.send_message(c.message.chat.id, "🔄 Введи user_id для обнуления подписки:")

@bot.callback_query_handler(func=lambda c: c.data == "adm_promo")
def adm_promo_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    ADMIN_FLOW[c.from_user.id] = {"mode": "promo_plan"}
    bot.send_message(c.message.chat.id, "🎫 Выбери план для промокода:", reply_markup=promo_plan_kb())

@bot.callback_query_handler(func=lambda c: c.data == "adm_monitor")
def adm_monitor_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    
    # Обновляем статистику
    resource_monitor.update_process_stats()
    stats = resource_monitor.get_system_stats()
    
    # Получаем информацию о запущенных процессах
    active_processes = []
    for key, proc_info in RUNNING.items():
        uid, fname = key
        proc = proc_info["proc"]
        if proc.poll() is None:
            # Получаем статистику ресурсов для процесса
            resource_stats = resource_monitor.process_stats.get(key, {})
            cpu_usage = resource_stats.get('cpu', 0)
            ram_usage = resource_stats.get('ram_mb', 0)
            
            active_processes.append({
                'user_id': uid,
                'filename': fname,
                'cpu': cpu_usage,
                'ram': ram_usage
            })
    
    # Формируем сообщение
    monitor_text = (
        "📊 <b>Мониторинг системы</b>\n\n"
        f"👥 Всего пользователей: <code>{stats['total_users']}</code>\n"
        f"🟢 Активных скриптов: <code>{stats['active_scripts']}</code>\n"
        f"⚙️ Общее использование CPU: <code>{stats['total_cpu_usage']:.1f}%</code>\n"
        f"💾 Общее использование RAM: <code>{stats['total_ram_usage_mb']:.1f} MB</code>\n\n"
    )
    
    if active_processes:
        monitor_text += "<b>Запущенные процессы:</b>\n"
        for proc in active_processes:
            monitor_text += (
                f"• <code>{proc['filename']}</code> (User: {proc['user_id']}) - "
                f"CPU: {proc['cpu']:.1f}%, RAM: {proc['ram']:.1f}MB\n"
            )
    else:
        monitor_text += "🔴 Нет активных процессов"
    
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🔄 Обновить", callback_data="adm_monitor"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin"))
    
    bot.edit_message_text(monitor_text, c.message.chat.id, c.message.id, reply_markup=kb, parse_mode="HTML")

def promo_plan_kb():
    kb = types.InlineKeyboardMarkup(row_width=3)
    for k, v in PLANS.items():
        kb.add(types.InlineKeyboardButton(v["name"], callback_data=f"promo_plan_{k}"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="admin"))
    return kb

@bot.callback_query_handler(func=lambda c: c.data.startswith("promo_plan_"))
def promo_plan_select(c):
    if c.from_user.id != OWNER_ID:
        return
    plan = c.data.split("_", 2)[2]
    ADMIN_FLOW[c.from_user.id] = {"mode": "promo_days", "data": {"plan": plan}}
    bot.send_message(c.message.chat.id, f"Выбран план: {PLANS[plan]['name']}\nВведи количество дней для промокода:")

@bot.message_handler(func=lambda m: m.from_user.id in ADMIN_FLOW and m.from_user.id == OWNER_ID)
def admin_flow_handler(m):
    flow = ADMIN_FLOW[m.from_user.id]
    mode = flow.get("mode")
    
    if mode == "plan_user":
        try:
            target = int(m.text.strip())
            flow["data"] = {"target": target}
            flow["mode"] = "plan_select"
            ADMIN_FLOW[m.from_user.id] = flow
            
            kb = types.InlineKeyboardMarkup(row_width=3)
            for k, v in PLANS.items():
                kb.add(types.InlineKeyboardButton(v["name"], callback_data=f"plan_{k}"))
            bot.reply_to(m, f"Выбери план для {target}:", reply_markup=kb)
        except ValueError:
            bot.reply_to(m, "❌ Неверный user_id. Введите число.")
            
    elif mode == "plan_days":
        try:
            days = int(m.text.strip())
            target = flow["data"]["target"]
            plan = flow["data"]["plan"]
            
            set_user_plan(target, plan, days)
            ADMIN_FLOW.pop(m.from_user.id, None)
            
            bot.reply_to(m, f"✅ Выдан план {PLANS[plan]['name']} пользователю {target} на {days} дн.")
            if target != OWNER_ID:
                bot.send_message(target, f"💎 Вам выдан план {PLANS[plan]['name']} на {days} дн.")
        except ValueError:
            bot.reply_to(m, "❌ Неверное количество дней. Введите число.")
            
    elif mode == "toggle_protect":
        try:
            target = int(m.text.strip())
            u = get_user(target)
            current = u["protection_disabled"]
            new_val = 0 if current else 1
            set_protection_disabled(target, new_val)
            
            status = "❌ ВЫКЛЮЧЕНА" if new_val else "✅ ВКЛЮЧЕНА"
            bot.reply_to(m, f"🛡️ Защита для пользователя {target} {status}")
            ADMIN_FLOW.pop(m.from_user.id, None)
            
            # Уведомляем пользователя
            if target != OWNER_ID:
                bot.send_message(target, f"🛡️ Ваша защита {status.lower()}")
        except ValueError:
            bot.reply_to(m, "❌ Неверный user_id. Введите число.")
            
    elif mode == "ban_user":
        try:
            target = int(m.text.strip())
            set_ban(target, 1)
            bot.reply_to(m, f"🚫 Пользователь {target} заблокирован")
            ADMIN_FLOW.pop(m.from_user.id, None)
            
            # Уведомляем пользователя
            if target != OWNER_ID:
                bot.send_message(target, "🚫 Вы заблокированы в системе.")
        except ValueError:
            bot.reply_to(m, "❌ Неверный user_id. Введите число.")
            
    elif mode == "unban_user":
        try:
            target = int(m.text.strip())
            set_ban(target, 0)
            bot.reply_to(m, f"✅ Пользователь {target} разблокирован")
            ADMIN_FLOW.pop(m.from_user.id, None)
            
            # Уведомляем пользователя
            if target != OWNER_ID:
                bot.send_message(target, "✅ Вы разблокированы в системе.")
        except ValueError:
            bot.reply_to(m, "❌ Неверный user_id. Введите число.")
            
    elif mode == "reset_sub":
        try:
            target = int(m.text.strip())
            reset_user_subscription(target)
            bot.reply_to(m, f"🔄 Подписка пользователя {target} обнулена (бесплатный тариф)")
            ADMIN_FLOW.pop(m.from_user.id, None)
            
            # Уведомляем пользователя
            if target != OWNER_ID:
                bot.send_message(target, "🔄 Ваша подписка была обнулена администратором. Вы переведены на бесплатный тариф.")
        except ValueError:
            bot.reply_to(m, "❌ Неверный user_id. Введите число.")
            
    elif mode == "promo_days":
        try:
            days = int(m.text.strip())
            plan = flow["data"]["plan"]
            flow["mode"] = "promo_activations"
            flow["data"]["days"] = days
            ADMIN_FLOW[m.from_user.id] = flow
            
            bot.reply_to(m, f"Выбран план: {PLANS[plan]['name']} на {days} дней\n\nВведи максимальное количество активаций (по умолчанию 1):")
        except ValueError:
            bot.reply_to(m, "❌ Неверное количество дней. Введите число.")
            
    elif mode == "promo_activations":
        try:
            max_activations = int(m.text.strip()) if m.text.strip() else 1
            plan = flow["data"]["plan"]
            days = flow["data"]["days"]
            flow["mode"] = "promo_name"
            flow["data"]["max_activations"] = max_activations
            ADMIN_FLOW[m.from_user.id] = flow
            
            # Генерируем случайный промокод
            promo_code = generate_promocode()
            bot.reply_to(m, 
                f"🎫 Промокод для {PLANS[plan]['name']} на {days} дней\n"
                f"👥 Макс. активаций: {max_activations}\n\n"
                f"Сгенерированный промокод: <code>{promo_code}</code>\n\n"
                f"Введи свой промокод или отправь 'готово' для использования сгенерированного", 
                parse_mode="HTML"
            )
        except ValueError:
            bot.reply_to(m, "❌ Неверное количество активаций. Введите число.")
            
    elif mode == "promo_name":
        promo_name = m.text.strip()
        if promo_name.lower() == 'готово':
            # Используем сгенерированный промокод
            promo_code = generate_promocode()
        else:
            promo_code = promo_name.upper()
        
        plan = flow["data"]["plan"]
        days = flow["data"]["days"]
        max_activations = flow["data"]["max_activations"]
        
        create_promocode(promo_code, plan, days, m.from_user.id, max_activations)
        ADMIN_FLOW.pop(m.from_user.id, None)
        
        bot.reply_to(m, 
            f"✅ Промокод создан!\n\n"
            f"🎫 <code>{promo_code}</code>\n"
            f"📋 План: {PLANS[plan]['name']}\n"
            f"⏰ Дней: {days}\n"
            f"👥 Макс. активаций: {max_activations}", 
            parse_mode="HTML"
        )

@bot.callback_query_handler(func=lambda c: c.data.startswith("plan_"))
def select_plan_cb(c):
    if c.from_user.id != OWNER_ID:
        return
    plan = c.data.split("_", 1)[1]
    flow = ADMIN_FLOW.get(c.from_user.id, {})
    data = flow.get("data", {})
    data["plan"] = plan
    flow["mode"] = "plan_days"
    flow["data"] = data
    ADMIN_FLOW[c.from_user.id] = flow
    bot.send_message(c.message.chat.id, f"Выбран план: {PLANS[plan]['name']}\nВведи количество дней:")

@bot.callback_query_handler(func=lambda c: c.data.startswith("adm_ban:"))
def adm_ban_callback(c):
    if c.from_user.id != OWNER_ID:
        return
    uid = int(c.data.split(":")[1])
    set_ban(uid, 1)
    bot.answer_callback_query(c.id, f"Пользователь {uid} заблокирован")
    bot.edit_message_reply_markup(c.message.chat.id, c.message.id, reply_markup=None)
    bot.send_message(uid, "🚫 Вы заблокированы в системе.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("adm_ignore:"))
def adm_ignore_callback(c):
    if c.from_user.id != OWNER_ID:
        return
    qid = int(c.data.split(":")[1])
    with db_connect() as conn:
        conn.execute("UPDATE quarantine SET status='ignored' WHERE qid=?", (qid,))
        conn.commit()
    bot.answer_callback_query(c.id, "Файл проигнорирован")
    bot.edit_message_reply_markup(c.message.chat.id, c.message.id, reply_markup=None)

@bot.callback_query_handler(func=lambda c: c.data.startswith("adm_approve:"))
def adm_approve_callback(c):
    if c.from_user.id != OWNER_ID:
        return
    qid = int(c.data.split(":")[1])
    
    if approve_quarantine_file(qid):
        q_file = get_quarantine_file(qid)
        if q_file:
            uid = q_file["user_id"]
            filename = q_file["filename"]
            
            bot.answer_callback_query(c.id, f"Файл {filename} разрешен")
            bot.edit_message_reply_markup(c.message.chat.id, c.message.id, reply_markup=None)
            
            # Уведомляем пользователя
            bot.send_message(uid, f"✅ Ваш файл <b>{filename}</b> был проверен администратором и разрешен к загрузке на хостинг!", parse_mode="HTML")
    else:
        bot.answer_callback_query(c.id, "❌ Ошибка при разрешении файла", show_alert=True)

# ---------------- PROMOCODE HANDLERS ----------------
@bot.callback_query_handler(func=lambda c: c.data == "promo")
def enter_promo(c):
    uid = c.from_user.id
    PROMO_FLOW.add(uid)
    
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("❌ Отмена", callback_data="back"))
    
    bot.send_message(c.message.chat.id, "🎫 Введите промокод:", reply_markup=kb)

@bot.message_handler(func=lambda m: m.from_user.id in PROMO_FLOW)
def handle_promo(msg):
    uid = msg.from_user.id
    promo_code = msg.text.strip().upper()
    
    promo = get_promocode(promo_code)
    if not promo:
        bot.reply_to(msg, "❌ Промокод не найден.")
        PROMO_FLOW.discard(uid)
        return
    
    # Проверяем не использовал ли уже пользователь этот промокод
    if promo["used_by"] == uid:
        bot.reply_to(msg, "❌ Вы уже активировали этот промокод.")
        PROMO_FLOW.discard(uid)
        return
    
    # Проверяем лимит активаций
    if promo["activations_count"] >= promo["max_activations"]:
        bot.reply_to(msg, "❌ Лимит активаций промокода исчерпан.")
        PROMO_FLOW.discard(uid)
        return
    
    # Получаем текущий план пользователя
    user = get_user(uid)
    current_plan = user["plan"]
    promo_plan = promo["plan"]
    
    # Проверяем можно ли применить промокод
    can_apply, message = can_apply_promocode(current_plan, promo_plan)
    
    if not can_apply:
        bot.reply_to(msg, message)
        PROMO_FLOW.discard(uid)
        return
    
    # Показываем пользователю что произойдет
    bot.reply_to(msg, f"{message}\n\nАктивирую промокод...")
    
    success, result_message = use_promocode(promo_code, uid)
    if success:
        if current_plan == promo_plan:
            # Добавляем дни к текущей подписке
            add_days_to_subscription(uid, promo["days"])
            new_days = get_subscription_days_left(uid)
            
            bot.reply_to(msg, 
                f"✅ Промокод активирован!\n\n"
                f"🎫 Добавлено дней: {promo['days']}\n"
                f"📋 Текущий план: {PLANS[promo_plan]['name']}\n"
                f"⏰ Теперь у вас: {new_days} дней\n"
                f"📊 Активаций использовано: {promo['activations_count'] + 1}/{promo['max_activations']}"
            )
        else:
            # Повышаем тариф
            set_user_plan(uid, promo_plan, promo["days"])
            
            bot.reply_to(msg, 
                f"✅ Промокод активирован!\n\n"
                f"🎫 Вы получили: {PLANS[promo_plan]['name']}\n"
                f"⏰ На {promo['days']} дней\n"
                f"📊 Активаций использовано: {promo['activations_count'] + 1}/{promo['max_activations']}"
            )
        
        PROMO_FLOW.discard(uid)
    else:
        bot.reply_to(msg, result_message)
        PROMO_FLOW.discard(uid)

# ---------------- MAIN MENU & NAVIGATION ----------------
def main_menu_kb(uid: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("📤 Загрузить файл", callback_data="upload"),
        types.InlineKeyboardButton("📁 Мои файлы", callback_data="myfiles")
    )
    kb.add(
        types.InlineKeyboardButton("⚡ Проверить скорость", callback_data="speed"),
        types.InlineKeyboardButton("💎 Купить подписку", callback_data="buy")
    )
    kb.add(
        types.InlineKeyboardButton("🎫 Ввести промокод", callback_data="promo")
    )
    if uid == OWNER_ID:
        kb.add(types.InlineKeyboardButton("👑 Админ-панель", callback_data="admin"))
    return kb

@bot.message_handler(commands=['start'])
def on_start(msg):
    uid = msg.from_user.id
    ensure_user(uid)
    u = get_user(uid)
    plan = u["plan"]
    lim = plan_limit(uid)
    days_left = get_subscription_days_left(uid)
    ram_mb = plan_ram_mb(uid)
    cpu_percent = plan_cpu_percent(uid)
    total_ram_mb = plan_total_ram_mb(uid)
    total_cpu_percent = plan_total_cpu_percent(uid)

    # Формируем текст с информацией о подписке
    if plan == "free":
        subscription_info = "🪶 Бесплатный план (без срока)"
    else:
        if days_left > 0:
            subscription_info = f"{PLANS[plan]['name']} • Осталось: {days_left} дн."
        else:
            subscription_info = f"{PLANS[plan]['name']} • ❌ ИСТЕКЛА"
            # Автоматически переводим на бесплатный план если подписка истекла
            if u["plan"] != "free":
                set_user_plan(uid, "free", 0)

    text = (f"🏠 <b>Главное меню</b>\n"
            f"📋 План: {subscription_info}\n"
            f"📁 Лимит файлов: {lim}\n"
            f"💾 RAM на файл: {ram_mb} MB | Суммарно: {total_ram_mb} MB\n"
            f"⚙️ CPU на файл: {cpu_percent}% | Суммарно: {total_cpu_percent}%\n"
            f"📝 Поддерживаемые языки: Python 🐍, JavaScript 📜")
    
    bot.send_message(uid, text, reply_markup=main_menu_kb(uid), parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data == "back")
def back_to_main(c):
    uid = c.from_user.id
    u = get_user(uid)
    plan = u["plan"]
    lim = plan_limit(uid)
    days_left = get_subscription_days_left(uid)
    ram_mb = plan_ram_mb(uid)
    cpu_percent = plan_cpu_percent(uid)
    total_ram_mb = plan_total_ram_mb(uid)
    total_cpu_percent = plan_total_cpu_percent(uid)

    # Формируем текст с информацией о подписке
    if plan == "free":
        subscription_info = "🪶 Бесплатный план (без срока)"
    else:
        if days_left > 0:
            subscription_info = f"{PLANS[plan]['name']} • Осталось: {days_left} дн."
        else:
            subscription_info = f"{PLANS[plan]['name']} • ❌ ИСТЕКЛА"
            # Автоматически переводим на бесплатный план если подписка истекла
            if u["plan"] != "free":
                set_user_plan(uid, "free", 0)

    text = (f"🏠 <b>Главное меню</b>\n"
            f"📋 План: {subscription_info}\n"
            f"📁 Лимит файлов: {lim}\n"
            f"💾 RAM на файл: {ram_mb} MB | Суммарно: {total_ram_mb} MB\n"
            f"⚙️ CPU на файл: {cpu_percent}% | Суммарно: {total_cpu_percent}%")
    
    safe_edit_text(bot.edit_message_text, text, c.message.chat.id, c.message.id, 
                   reply_markup=main_menu_kb(uid), parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data == "admin")
def open_admin(c):
    if c.from_user.id != OWNER_ID:
        return
    bot.edit_message_text("👑 Панель администратора:", c.message.chat.id, c.message.id, reply_markup=admin_kb())

@bot.callback_query_handler(func=lambda c: c.data == "myfiles")
def show_files(c):
    uid = c.from_user.id
    files = list_files(uid)
    if not files:
        text = "📁 У вас пока нет файлов.\nЗагрузите .py, .js или .zip файл через меню."
    else:
        text = "📁 Твои файлы:"
    safe_edit_text(bot.edit_message_text, text, c.message.chat.id, c.message.id, reply_markup=files_kb(uid))

@bot.callback_query_handler(func=lambda c: c.data == "upload")
def ask_upload(c):
    uid = c.from_user.id
    u = get_user(uid)
    if u["frozen"]:
        bot.answer_callback_query(c.id, "🧊 Заморожено: загрузка недоступна.", show_alert=True)
        return
    
    info_text = (
        "📤 <b>Загрузка файлов</b>\n\n"
        "Поддерживаемые форматы:\n"
        "• 🐍 <code>.py</code> - Python скрипты\n"
        "• 📜 <code>.js</code> - JavaScript скрипты\n"
        "• 📦 <code>.zip</code> - Архивы с файлами\n\n"
        "Отправьте файл для загрузки:"
    )
    
    bot.answer_callback_query(c.id, "📤 Отправьте .py, .js или .zip файл")
    bot.send_message(c.message.chat.id, info_text, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data == "buy")
def buy_plan(c):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back"))
    
    plans_text = (
        "💎 <b>Тарифные планы</b>\n\n"
        "🪶 <b>Бесплатный</b>\n"
        "• Файлов: 2\n"
        "• RAM на файл: 512 MB | Суммарно: 800 MB\n"
        "• CPU на файл: 5% | Суммарно: 8%\n"
        "• Срок: бессрочно\n\n"
        "💎 <b>Премиум</b>\n"
        "• Файлов: 20\n"
        "• RAM на файл: 1024 MB | Суммарно: 2048 MB\n"
        "• CPU на файл: 15% | Суммарно: 25%\n"
        "• Срок: по выбору\n\n"
        "🚀 <b>Максимальный</b>\n"
        "• Файлов: 50\n"
        "• RAM на файл: 2048 MB | Суммарно: 4096 MB\n"
        "• CPU на файл: 30% | Суммарно: 50%\n"
        "• Срок: по выбору\n\n"
        "Для покупки обратитесь: @Arkadarootfurry"
    )
    
    bot.edit_message_text(plans_text, c.message.chat.id, c.message.id, reply_markup=kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data == "speed")
def speed_test(c):
    bot.answer_callback_query(c.id, "⚡ Проверяем производительность...")
    
    results = []
    
    # 1. Проверка пинга
    try:
        import speedtest
        st = speedtest.Speedtest()
        st.get_best_server()
        ping_result = st.results.ping
        results.append(f"📶 Пинг: <code>{ping_result:.1f} ms</code>")
    except ImportError:
        results.append("📶 Пинг: <code>Модуль speedtest не установлен</code>")
    except Exception as e:
        results.append(f"📶 Пинг: <code>Ошибка: {str(e)}</code>")
    
    # 2. Проверка использования CPU
    try:
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        results.append(f"⚙️ CPU: <code>{cpu_percent}%</code>")
    except ImportError:
        results.append("⚙️ CPU: <code>Модуль psutil не установлен</code>")
    except Exception as e:
        results.append(f"⚙️ CPU: <code>Ошибка: {str(e)}</code>")
    
    # 3. Проверка использования RAM
    try:
        import psutil
        memory = psutil.virtual_memory()
        ram_used = memory.used / (1024 ** 3)  # в GB
        ram_total = memory.total / (1024 ** 3)  # в GB
        ram_percent = memory.percent
        results.append(f"💾 RAM: <code>{ram_percent}% ({ram_used:.1f} GB / {ram_total:.1f} GB)</code>")
    except ImportError:
        results.append("💾 RAM: <code>Модуль psutil не установлен</code>")
    except Exception as e:
        results.append(f"💾 RAM: <code>Ошибка: {str(e)}</code>")
    
    # 4. Проверка диска
    try:
        import psutil
        disk = psutil.disk_usage('/')
        disk_used = disk.used / (1024 ** 3)  # в GB
        disk_total = disk.total / (1024 ** 3)  # в GB
        disk_percent = disk.percent
        results.append(f"💽 Диск: <code>{disk_percent}% ({disk_used:.1f} GB / {disk_total:.1f} GB)</code>")
    except ImportError:
        results.append("💽 Диск: <code>Модуль psutil не установлен</code>")
    except Exception as e:
        results.append(f"💽 Диск: <code>Ошибка: {str(e)}</code>")
    
    # 5. Простой тест производительности Python
    try:
        import time
        start_time = time.time()
        # Простой математический тест
        for i in range(1000000):
            x = i * i
        python_speed = time.time() - start_time
        results.append(f"🐍 Python тест: <code>{python_speed:.3f} сек</code>")
    except Exception as e:
        results.append(f"🐍 Python тест: <code>Ошибка: {str(e)}</code>")
    
    text = "⚡ <b>Производительность сервера:</b>\n" + "\n".join(results)
    bot.send_message(c.message.chat.id, text, parse_mode="HTML")

# ---------------- BACKGROUND MONITORING ----------------
def background_monitor():
    """Фоновая задача для мониторинга ресурсов"""
    while True:
        try:
            resource_monitor.update_process_stats()
            time.sleep(10)  # Проверяем каждые 10 секунд
        except Exception as e:
            print(f"Ошибка в мониторинге: {e}")
            time.sleep(30)

# Запускаем фоновый мониторинг
monitor_thread = threading.Thread(target=background_monitor, daemon=True)
monitor_thread.start()

# ---------------- RUN ----------------
if __name__ == "__main__":
    init_db()
    print("Bot started.")

    bot.infinity_polling(skip_pending=True)
