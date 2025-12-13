import logging
from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.types import Message

from app.core.config import settings
from app.services.gpt_service import gpt_service
from app.services import db_service
from app.services.cache_service import cache_service
from .base import contains_non_numeric_keywords, format_numeric_result

logger = logging.getLogger(__name__)
router = Router()

@router.message(Command("start"))
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

@router.message()
async def handle_text(message: Message, bot: Bot):
    """Обработка текстовых запросов пользователя"""
    user_query = message.text.strip()
    
    if not user_query:
        return
    
    if len(user_query) < 10:
        await message.answer(
            "Ваш запрос слишком короткий.\n\n"
            "Пожалуйста, сформулируйте вопрос подробнее (не менее 10 символов).\n\n"
            "Примеры:\n"
            "• Сколько всего видео в базе?\n"
            "• Какое среднее количество просмотров?\n"
            "• Сколько видео за последний месяц?"
        )
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
    
    # Проверяем кеш
    cached_result = await cache_service.get_cached_result(user_query)
    if cached_result:
        await message.answer(cached_result)
        return
    

    await bot.send_chat_action(message.chat.id, "typing")
        
    try:
        db_schema = await db_service.get_schema()
        
        sql = await gpt_service.ask_gpt(user_query, db_schema)
        
        if not sql:
            await message.answer("Не удалось сгенерировать запрос. Попробуйте сформулировать иначе.")
            return
        
        # Проверяем, что запрос начинается с SELECT (безопасность)
        if not sql.strip().upper().startswith('SELECT'):
            await message.answer("Сгенерирован некорректный запрос.")
            return
        
        logger.info(f"SQL запрос: {sql}")
        
        results = await db_service.execute_query(sql)
        
        if not results:
            await message.answer("По вашему запросу данных не найдено.")
            return
        
        # Форматируем результат как простое число/числа
        formatted_result = format_numeric_result(results)
        
        await cache_service.save_to_cache(user_query, formatted_result)

        # Отправляем только числовой ответ
        await message.answer(f"{formatted_result}")     

    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        await message.answer(f"Произошла ошибка при обработке запроса")