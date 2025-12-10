import asyncio
from aiogram import Bot, Dispatcher
from app.core.config import settings

BOT_TOKEN = settings.BOT_TOKEN


async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    try:
        print('bot starting...')
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
    

if __name__ == "__main__":
    asyncio.run(main())