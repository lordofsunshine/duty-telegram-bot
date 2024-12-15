import telebot
import time
from datetime import datetime, timedelta
import pytz
import threading
import logging
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from logging.handlers import RotatingFileHandler
import emoji
from telebot.apihelper import ApiTelegramException

# Конфигурация
BOT_TOKEN = 'YOUR_TELEGRAM_TOKEN'
MONGODB_URI = 'YOUR_MONGODB_URI'
DB_NAME = 'duty_bot_db'
TIMEZONE = 'Europe/Moscow'
DUTY_MESSAGE_TIME = '07:30'
CREATOR_ID = 5427664683

# Настройки работы бота
CONFIG = {
    # Включить или выключить работу в праздничные дни (True/False)
    "WORK_ON_HOLIDAYS": False,
    # Включить или выключить работу в выходные дни (True/False)
    "WORK_ON_WEEKENDS": False
}

log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
log_file = 'bot_log.txt'
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger('DutyBot')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

bot = telebot.TeleBot(BOT_TOKEN, threaded=False)
telebot.apihelper.RETRY_ON_ERROR = True
telebot.apihelper.READ_TIMEOUT = 90

try:
    client = MongoClient(MONGODB_URI)
    client.admin.command('ping')
    logger.info("✅ Подключение к MongoDB успешно установлено")
    db = client[DB_NAME]
except ConnectionFailure:
    logger.error("❌ Ошибка подключения к MongoDB")
    exit(1)

duty_schedule = [
    ("Алексей П.", "Екатерина С."),
    ("Дмитрий К.", "Ольга В."),
    ("Сергей М.", "Анна Р."),
    ("Игорь Л.", "Наталья Ж."),
    ("Владимир Н.", "Татьяна Ф."),
    ("Андрей Г.", "Мария Д."),
    ("Павел Ш.", "Елена Б."),
    ("Николай Т.", "Ирина К."),
    ("Максим З.", "Светлана Х."),
    ("Артем Ю.", "Юлия Щ."),
    ("Виктор Е.", "Людмила Ц."),
    ("Григорий Ч.", "Валентина Э."),
    ("Евгений Я.", "Ксения У.")
]

holidays = [
    "01-01", "01-02", "01-03", "01-04", "01-05", "01-06", "01-07", "01-08",
    "02-23", "02-24", "03-08", "05-01", "05-09", "06-12", "11-04"
]

bot_active = True
bot_instance = None

def initialize_db():
    settings = db.settings.find_one({"_id": "bot_settings"})
    if not settings:
        db.settings.insert_one({
            "_id": "bot_settings",
            "current_duty_index": 0,
            "last_reset_date": datetime.now(pytz.timezone(TIMEZONE)),
            "group_id": None,
            "thread_id": None,
            "last_message_date": None,
            "is_active": True
        })
        logger.info("📚 База данных инициализирована")

def is_working_day(date):
    if not CONFIG["WORK_ON_WEEKENDS"] and date.weekday() >= 5:
        return False
    if not CONFIG["WORK_ON_HOLIDAYS"] and date.strftime("%m-%d") in holidays:
        return False
    return True

def get_duty_pair():
    try:
        settings = db.settings.find_one({"_id": "bot_settings"})
        if not settings:
            initialize_db()
            settings = db.settings.find_one({"_id": "bot_settings"})

        current_index = settings.get("current_duty_index", 0)
        duty_pair = duty_schedule[current_index]

        next_index = (current_index + 1) % len(duty_schedule)
        db.settings.update_one(
            {"_id": "bot_settings"},
            {"$set": {"current_duty_index": next_index}}
        )

        if next_index == 0:
            reset_duty_cycle()

        return duty_pair
    except Exception as e:
        logger.error(f"❌ Ошибка в get_duty_pair: {e}")
        return duty_schedule[0]

def reset_duty_cycle():
    try:
        db.settings.update_one(
            {"_id": "bot_settings"},
            {
                "$set": {
                    "current_duty_index": 0,
                    "last_reset_date": datetime.now(pytz.timezone(TIMEZONE))
                }
            }
        )
        db.duty_history.delete_many({})
        logger.info("🔄 Цикл дежурств сброшен")
    except Exception as e:
        logger.error(f"❌ Ошибка в reset_duty_cycle: {e}")

def should_send_message():
    try:
        if not bot_active:
            logger.info("🛑 Бот неактивен, сообщение не будет отправлено")
            return False
            
        settings = db.settings.find_one({"_id": "bot_settings"})
        if not settings:
            logger.info("⚠️ Настройки не найдены, инициализация базы данных")
            initialize_db()
            return True

        moscow_tz = pytz.timezone(TIMEZONE)
        current_date = datetime.now(moscow_tz).date()
        last_message_date = settings.get("last_message_date")
        
        if last_message_date:
            last_message_date = last_message_date.date() if isinstance(last_message_date, datetime) else last_message_date
            if current_date > last_message_date and is_working_day(current_date):
                logger.info(f"✅ Сообщение должно быть отправлено. Текущая дата: {current_date}, Последняя дата отправки: {last_message_date}")
                return True
            else:
                logger.info(f"❌ Сообщение не должно быть отправлено. Текущая дата: {current_date}, Последняя дата отправки: {last_message_date}")
                return False
        else:
            logger.info("✅ Первая отправка сообщения")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка в should_send_message: {e}")
        return False

def send_message_with_retry(chat_id, message, parse_mode='HTML', message_thread_id=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            if message_thread_id:
                bot.send_message(chat_id, message, parse_mode=parse_mode, message_thread_id=message_thread_id)
            else:
                bot.send_message(chat_id, message, parse_mode=parse_mode)
            return True
        except ApiTelegramException as e:
            if e.error_code == 429:
                retry_after = e.result_json.get('parameters', {}).get('retry_after', 5)
                logger.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
            else:
                logger.error(f"Telegram API error: {e}")
                time.sleep(5)
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            time.sleep(5)
    logger.error("Failed to send message after maximum retries")
    return False

def send_duty_message():
    try:
        if not bot_active:
            logger.info("🛑 Бот остановлен, сообщение не отправлено")
            return
            
        if not should_send_message():
            logger.info("ℹ️ Сообщение не требуется отправлять сегодня")
            return

        settings = db.settings.find_one({"_id": "bot_settings"})
        if not settings:
            logger.error("❌ Настройки не найдены")
            return

        group_id = settings.get("group_id")
        thread_id = settings.get("thread_id")
        
        if not group_id:
            logger.error("❌ ID группы не установлен")
            return

        moscow_tz = pytz.timezone(TIMEZONE)
        current_date = datetime.now(moscow_tz)
        
        if not is_working_day(current_date):
            logger.info(f"📅 Пропуск сообщения - нерабочий день: {current_date}")
            return

        duty_pair = get_duty_pair()
        
        message = (
            f"🔔 <b>Сегодня дежурят:</b>\n\n"
            f"1️⃣ {duty_pair[0]}\n"
            f"2️⃣ {duty_pair[1]}\n\n"
            f"📅 Дата: {current_date.strftime('%d.%m.%Y')}\n"
            f"🕒 Время: {current_date.strftime('%H:%M')}"
        )
        
        if send_message_with_retry(group_id, message, parse_mode='HTML', message_thread_id=thread_id):
            logger.info(f"✅ Сообщение успешно отправлено {'в топик ' + str(thread_id) if thread_id else ''}")
            
            db.settings.update_one(
                {"_id": "bot_settings"},
                {"$set": {"last_message_date": current_date}}
            )
            
            db.duty_history.insert_one({
                "date": current_date,
                "duty1": duty_pair[0],
                "duty2": duty_pair[1]
            })
        else:
            logger.error("❌ Не удалось отправить сообщение после нескольких попыток")
    except Exception as e:
        logger.error(f"❌ Ошибка в send_duty_message: {e}")

def is_creator(user_id):
    return str(user_id) == str(CREATOR_ID)

@bot.message_handler(commands=['start'])
def send_welcome(message):
    try:
        if not is_creator(message.from_user.id):
            logger.info(f"⚠️ Попытка выполнить команду /start пользователем {message.from_user.id}")
            return
            
        chat_id = message.chat.id
        thread_id = message.message_thread_id if hasattr(message,'message_thread_id') else None
        
        if message.chat.type in ['group', 'supergroup']:
            set_group_and_thread_id(chat_id, thread_id)
            
            location_info = "в этот топик" if thread_id else "в эту группу"
            bot.reply_to(
                message,
                f"✨ Привет! Я бот для отправки информации о дежурных. "
                f"Я буду отправлять сообщения {location_info} каждый рабочий день в {DUTY_MESSAGE_TIME} по Московскому времени.",
                message_thread_id=thread_id
            )
            logger.info(f"✅ Бот успешно запущен в чате {chat_id}")
        else:
            bot.reply_to(message, "👋 Привет! Пожалуйста, добавьте меня в группу и отправьте там команду /start")
    except Exception as e:
        logger.error(f"❌ Ошибка в send_welcome: {e}")

@bot.message_handler(commands=['stop'])
def stop_bot(message):
    try:
        if not is_creator(message.from_user.id):
            logger.info(f"⚠️ Попытка выполнить команду /stop пользователем {message.from_user.id}")
            return
            
        global bot_active
        bot_active = False
        bot.reply_to(message, "🛑 Бот остановлен. Отправка сообщений о дежурных приостановлена.")
        logger.info("🛑 Бот остановлен")
    except Exception as e:
        logger.error(f"❌ Ошибка в stop_bot: {e}")

@bot.message_handler(commands=['resume'])
def resume_bot(message):
    try:
        if not is_creator(message.from_user.id):
            logger.info(f"⚠️ Попытка выполнить команду /resume пользователем {message.from_user.id}")
            return
            
        global bot_active
        bot_active = True
        bot.reply_to(message, "✅ Бот возобновил работу. Отправка сообщений о дежурных активирована.")
        logger.info("✅ Бот возобновил работу")
    except Exception as e:
        logger.error(f"❌ Ошибка в resume_bot: {e}")

def set_group_and_thread_id(group_id, thread_id=None):
    try:
        update_data = {"group_id": group_id}
        if thread_id is not None:
            update_data["thread_id"] = thread_id
        
        db.settings.update_one(
            {"_id": "bot_settings"},
            {"$set": update_data},
            upsert=True
        )
        logger.info(f"✅ Установлены: ID группы: {group_id}, ID топика: {thread_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в set_group_and_thread_id: {e}")

def check_and_send_if_needed():
    try:
        moscow_tz = pytz.timezone(TIMEZONE)
        current_time = datetime.now(moscow_tz)
        target_time = datetime.strptime(DUTY_MESSAGE_TIME, "%H:%M").time()
        
        logger.info(f"🕒 Текущее время: {current_time.strftime('%H:%M:%S')}, Целевое время: {target_time.strftime('%H:%M:%S')}")
        
        if current_time.time() >= target_time:
            logger.info("🔍 Время отправки наступило, проверка необходимости отправки сообщения...")
            if should_send_message():
                logger.info("📤 Отправка запланированного сообщения...")
                send_duty_message()
            else:
                logger.info("ℹ️ Условия для отправки сообщения не выполнены")
        else:
            logger.info("⏳ Ожидание времени отправки")
    except Exception as e:
        logger.error(f"❌ Ошибка в check_and_send_if_needed: {e}")

def schedule_checker():
    while True:
        try:
            check_and_send_if_needed()
            time.sleep(30)
        except Exception as e:
            logger.error(f"❌ Ошибка в schedule_checker: {e}")

def main():
    try:
        initialize_db()
        
        moscow_tz = pytz.timezone(TIMEZONE)
        current_time = datetime.now(moscow_tz)
        logger.info(f"🕒 Текущее время в Москве: {current_time.strftime('%H:%M:%S')}")
        
        if current_time.time() >= datetime.strptime(DUTY_MESSAGE_TIME, "%H:%M").time():
            logger.info("🔍 Проверка необходимости отправки сообщения при запуске...")
            if should_send_message():
                send_duty_message()
        
        scheduler_thread = threading.Thread(target=schedule_checker)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        logger.info("⚡ Планировщик запущен")
        
        logger.info("🚀 Бот запущен и готов к работе")
        
        global bot_instance
        bot_instance = bot
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")

if __name__ == '__main__':
    main()
