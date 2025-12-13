import aiohttp
import json
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class SimpleYandexGPT:
    def __init__(self):
        self.api_key = settings.YANDEX_API_KEY
        self.folder_id = settings.YANDEX_FOLDER_ID
        self.url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

        # Логирование настроек API
        if not self.api_key or not self.folder_id:
            logger.error("YandexGPT API ключи не настроены!")
        else:
            logger.info(f"YandexGPT API настроен (folder_id: {self.folder_id[:10]}...)")
    
    
    async def ask_gpt(self, user_query: str, db_schema: str) -> str:
        """Преобразовать запрос пользователя в SQL (асинхронная версия)"""
            
        prompt = f"""
        Ты SQL-эксперт. Преобразуй запрос на русском языке в SQL для PostgreSQL.
        
        ВАЖНО: 
        1.Генерируй ТОЛЬКО SELECT-запросы, которые возвращают ЧИСЛЕННЫЕ результаты:
        - COUNT() - для подсчёта количества
        - SUM() - для суммирования
        - AVG() - для среднего значения
        - MAX()/MIN() - для максимальных/минимальных значений
        2. Для работы с датами используй следующие правила:
       - При сравнении дат используй функцию DATE() или диапазон
       - НЕ используй простое равенство с датой (created_at = '2025-11-27' НЕПРАВИЛЬНО!)
       - Используй DATE(created_at) = '2025-11-27' или диапазон created_at >= '2025-11-27' AND created_at < '2025-11-28'
        3. Формат дат в базе: TIMESTAMP WITH TIME ZONE

        Схема базы данных:
        {db_schema}
        
        Пользователь спрашивает: {user_query}
        
        Генерируй запрос так, чтобы он возвращал ОДНО числовое значение или несколько чисел, если это необходимо.
        Используй только агрегирующие функции (COUNT, SUM, AVG, MAX, MIN).

        Примеры правильных запросов для работы с датами:
        - "Сколько разных видео получали просмотры 27 ноября 2025" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27'
        - "Сколько видео создано 2025-11-10" → SELECT COUNT(*) FROM videos WHERE DATE(video_created_at) = '2025-11-10'
        - "Сколько видео за ноябрь 2025" → SELECT COUNT(*) FROM videos WHERE video_created_at >= '2025-11-01' AND video_created_at < '2025-12-01'
        
        Генерируй запрос так, чтобы он возвращал ОДНО числовое значение.
        Используй только агрегирующие функции (COUNT, SUM, AVG, MAX, MIN).
        
        Верни ТОЛЬКО SQL-запрос без пояснений и без форматирования markdown.
        Примеры правильных запросов:
        - SELECT COUNT(*) FROM videos;
        - SELECT SUM(views_count) FROM videos;
        - SELECT AVG(likes_count) FROM videos;
        - SELECT MAX(comments_count) FROM videos;
        """
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "x-folder-id": self.folder_id,
            "Content-Type": "application/json"
        }
        
        data = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt-lite",
            "completionOptions": {
                "temperature": 0.1,  # Низкая температура для более детерминированных ответов
                "maxTokens": 500
            },
            "messages": [
                {"role": "user", "text": prompt}
            ]
        }
        
        try:
            logger.info(f"Отправка запроса к YandexGPT API для: {user_query}")
            async with aiohttp.ClientSession() as session:
                async with session.post(self.url, headers=headers, json=data, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        sql = result['result']['alternatives'][0]['message']['text'].strip()
                        # Очищаем от markdown и лишних символов
                        sql = sql.replace('```sql', '').replace('```', '').strip()
                        # Убираем точку с запятой в конце если есть
                        sql = sql.rstrip(';')
                        logger.info(f"Сгенерирован SQL: {sql}")
                        return sql
                    else:
                        text = await response.text()
                        logger.error(f"Ошибка API: {response.status}, {text}")
                        return None
        except Exception as e:
            logger.error(f"Ошибка YandexGPT: {e}", exc_info=True)
            return None

gpt_service = SimpleYandexGPT()