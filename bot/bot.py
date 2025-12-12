import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from app.core.config import settings
from app.services.simple_db import db_service
from app.handlers import base_router, user_router

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Основная функция запуска бота"""
    
    # Инициализация бота
    bot = Bot(
        token=settings.BOT_TOKEN,
    )
    
    # Инициализация диспетчера
    dp = Dispatcher()
    
    # Подключаем роутеры
    dp.include_router(user_router)
    dp.include_router(base_router)
    
    # Подключаемся к базе данных
    await db_service.connect()
    
    # Запуск бота
    logger.info("Бот запущен и готов к работе!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())