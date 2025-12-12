import asyncio
import logging
import re
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

def contains_non_numeric_keywords(query: str) -> bool:
    """Проверяет, содержит ли запрос ключевые слова, которые предполагают нечисловой ответ"""
    query_lower = query.lower()
    
    # Ключевые слова для вопросов с нечисловыми ответами
    non_numeric_patterns = [
        r'\bкакие\b', r'\bкакая\b', r'\bкаков\b',
        r'\bкто\b', r'\bчто\b', r'\bгде\b', r'\bкуда\b', r'\bоткуда\b',
        r'\bпочему\b', r'\bзачем\b', r'\bкак\b', r'\bкогда\b',
        r'\bназови\b', r'\bперечисли\b', r'\bпокажи\b', r'\bвыведи\b',
        r'\bрасскажи\b', r'\bопиши\b', r'\bобъясни\b', r'\bдай\b',
        r'\bтоп\b', r'\bсписок\b', r'\bтаблица\b', r'\bрейтинг\b',
        r'\bлучшие\b', r'\bхудшие\b', r'\bпоследние\b', r'\bпервые\b',
        r'\bкаковы\b', r'\bчем\b', r'\bкому\b', r'\bкого\b',
        r'\bо чем\b', r'\bпро что\b', r'\bкаким\b', r'\bкакими\b'
    ]
    
    # Проверяем, содержит ли запрос неподходящие ключевые слова
    for pattern in non_numeric_patterns:
        if re.search(pattern, query_lower):
            return True
    
    return False

def format_numeric_result(results: list) -> str:
    """Форматирует результаты запроса в простое текстовое представление чисел"""
    if not results:
        return "Нет данных"
    
    # Если результат содержит только одну строку и одну колонку
    if len(results) == 1:
        row = results[0]
        if len(row) == 1:
            # Извлекаем первое (и единственное) значение
            value = list(row.values())[0]
            if value is None:
                return "Нет данных"
            
            # Преобразуем в строку, убирая дробные нули
            if isinstance(value, (int, float)):
                if isinstance(value, float) and value.is_integer():
                    return str(int(value))
                return str(value)
            return str(value)
    
    # Пытаемся извлечь числовые значения из результатов
    numeric_values = []
    for row in results:
        for key, value in row.items():
            if isinstance(value, (int, float)):
                if isinstance(value, float) and value.is_integer():
                    numeric_values.append(int(value))
                else:
                    numeric_values.append(value)
    
    if numeric_values:
        if len(numeric_values) == 1:
            return str(numeric_values[0])
        else:
            # Возвращаем все числовые значения через запятую
            return ", ".join(str(v) for v in numeric_values)
    
    return "Нет числовых данных для ответа"

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Приветственное сообщение"""
    welcome_text = """
Привет! Я бот для анализа видео-статистики.

Я умею отвечать только на количественные вопросы о видео в базе данных. 

Я отвечаю только числами на вопросы, которые можно ответить числом или количеством.

Примеры подходящих запросов:
"Сколько всего видео?"
"Общее количество просмотров"
"Среднее число лайков"
"Сумма комментариев"
"Максимум отчетов"

Примеры НЕподходящих запросов (на них я не отвечаю):
"Какое видео самое популярное?"
"Какие видео были загружены вчера?"
"Кто загрузил больше всего видео?"
"Покажи последние 5 видео"
"Топ 10 видео по просмотрам"

Я не отвечаю на вопросы со словами: "какой", "какие", "кто", "что", "где", "почему", "покажи", "топ", "список" и т.д.
"""
    await message.answer(welcome_text)

@dp.message()
async def handle_text(message: Message):
    """Обработка текстовых запросов пользователя"""
    user_query = message.text.strip()
    
    if not user_query:
        return
    
    # Проверяем, содержит ли запрос ключевые слова для нечисловых ответов
    if contains_non_numeric_keywords(user_query):
        await message.answer(
            "Я отвечаю только на количественные вопросы, которые можно ответить числом.\n\n"
            "Не могу ответить на вопросы со словами: 'какой', 'какие', 'кто', 'что', 'покажи', 'топ', 'список' и т.д.\n\n"
            "Попробуйте спросить иначе, например:\n"
            "• Сколько всего видео?\n"
            "• Общее количество просмотров\n"
            "• Среднее число лайков\n"
            "• Максимальное количество комментариев"
        )
        return
    
    await bot.send_chat_action(message.chat.id, "typing")
    
    status_msg = await message.answer(f"Анализирую запрос: <i>{user_query}</i>")
    
    try:
        db_schema = await db_service.get_schema()
        
        sql = await gpt_service.ask_gpt(user_query, db_schema)
        
        if not sql:
            await status_msg.edit_text("Не удалось сгенерировать запрос. Попробуйте сформулировать иначе.")
            return
        
        # Проверяем, что запрос начинается с SELECT (безопасность)
        if not sql.strip().upper().startswith('SELECT'):
            await status_msg.edit_text("Сгенерирован некорректный запрос.")
            return
        
        await status_msg.edit_text(f"📋 SQL запрос сгенерирован...")
        logger.info(f"SQL запрос: {sql}")
        
        results = await db_service.execute_query(sql)
        
        if not results:
            await status_msg.edit_text("По вашему запросу данных не найдено.")
            return
        
        # Форматируем результат как простое число/числа
        formatted_result = format_numeric_result(results)
        
        # Отправляем только числовой ответ
        await status_msg.edit_text(f"{formatted_result}")
        
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        await status_msg.edit_text(f"Произошла ошибка при обработке запроса")

@dp.errors()
async def error_handler(event, exception):
    """Обработчик ошибок"""
    logger.error(f"Ошибка: {exception}", exc_info=True)
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