import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

from app.core.config import settings
from app.services.simple_gpt import gpt_service
from app.services.simple_db import db_service

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(
    token=settings.BOT_TOKEN,
)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Приветственное сообщение"""
    welcome_text = """
Привет! Я бот для анализа видео-статистики.

Я умею отвечать на вопросы о видео в базе данных. 

Просто спроси меня:
• Сколько всего видео?
• Какое видео самое популярное?
• Покажи последние 5 видео
• Сколько просмотров у видео с ID ...?
• Статистика по пользователю user123

Примеры запросов:
"Среднее количество просмотров"
"Сколько видео за последнюю неделю"
"Сколько всего видео есть в системе?"

Также есть команды:
/start - это сообщение
/stats - статистика базы
"""
    await message.answer(welcome_text)

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Показать статистику базы данных"""
    await message.answer("Получаю статистику...")
    
    stats = await db_service.get_stats()
    await message.answer(stats)

@dp.message()
async def handle_text(message: Message):
    """Обработка текстовых запросов пользователя"""
    user_query = message.text.strip()
    
    if not user_query:
        return
    
    await bot.send_chat_action(message.chat.id, "typing")
    
    status_msg = await message.answer(f"Анализирую запрос: <i>{user_query}</i>")
    
    try:

        db_schema = await db_service.get_schema()
        
        sql = await gpt_service.ask_gpt(user_query, db_schema)
        
        if not sql:
            await status_msg.edit_text("Не удалось понять ваш запрос. Попробуйте сформулировать иначе.")
            return
        
        await status_msg.edit_text(f"Сгенерирован SQL:\n{sql[:200]}")
        
        results = await db_service.execute_query(sql)
        
        if not results:
            await status_msg.edit_text("По вашему запросу ничего не найдено.")
            return
        
        response = format_results(user_query, results)
        
        await status_msg.edit_text(response)
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await status_msg.edit_text(f"Произошла ошибка: {str(e)[:100]}")

def format_results(query: str, results: list) -> str:
    """Форматирование результатов в читаемый вид"""
    if not results:
        return "Ничего не найдено"
    
    if len(results) == 1 and len(results[0]) == 1:
        value = list(results[0].values())[0]
        return f"Результат: <b>{value}</b>"
    
    total = len(results)
    response = f"Найдено записей: <b>{total}</b>\n\n"
    
    max_display = 10
    display_results = results[:max_display]
    
    if display_results:
        headers = list(display_results[0].keys())
        
        response += " | ".join([f"<b>{h}</b>" for h in headers]) + "\n"
        response += "--- | " * (len(headers) - 1) + "---\n"
        
        for row in display_results:
            row_values = [str(row.get(h, ""))[:30] for h in headers]
            response += " | ".join(row_values) + "\n"
        
        if total > max_display:
            response += f"\n... и еще <b>{total - max_display}</b> записей"
    
    return response

@dp.errors()
async def error_handler(event, exception):
    """Обработчик ошибок"""
    logger.error(f"Ошибка: {exception}")
    # Можно отправить сообщение админу
    if settings.ADMIN_CHAT_ID:
        try:
            await bot.send_message(settings.ADMIN_CHAT_ID, f"Ошибка: {exception}")
        except:
            pass

async def main():
    """Основная функция запуска бота"""
    await db_service.connect()
    
    logger.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())