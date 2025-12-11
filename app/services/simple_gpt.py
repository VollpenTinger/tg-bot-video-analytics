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
        
        Схема базы данных:
        {db_schema}
        
        Пользователь спрашивает: {user_query}
        
        Верни ТОЛЬКО SQL-запрос без пояснений.
        """
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "x-folder-id": self.folder_id,
            "Content-Type": "application/json"
        }
        
        data = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt-lite",
            "completionOptions": {
                "temperature": 0.3,
                "maxTokens": 1000
            },
            "messages": [
                {"role": "user", "text": prompt}
            ]
        }
        
        try:
            logger.info(f"Отправка запроса к YandexGPT API...")
            async with aiohttp.ClientSession() as session:
                async with session.post(self.url, headers=headers, json=data, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        sql = result['result']['alternatives'][0]['message']['text'].strip()
                        # Очищаем от markdown
                        sql = sql.replace('```sql', '').replace('```', '').strip()
                        return sql
                    else:
                        text = await response.text()
                        print(f"Ошибка API: {response.status}, {text}")
                        return None
        except Exception as e:
            print(f"Ошибка YandexGPT: {e}")
            return None

gpt_service = SimpleYandexGPT()