import asyncpg
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

class SimpleDatabase:
    def __init__(self):
        self.pool = None
    
    async def connect(self):
        """Подключиться к базе данных"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                dsn=settings.database_url,
                min_size=1,
                max_size=5
            )
    
    async def get_schema(self) -> str:
        """Получить простую схему базы данных"""
        return """
        Таблицы в базе данных:
        1. videos - информация о видео
           Колонки: id, creator_id, video_created_at, views_count, likes_count, 
                    comments_count, reports_count, created_at, updated_at
        
        2. video_snapshots - снапшоты статистики видео
           Колонки: id, video_id, views_count, likes_count, comments_count, reports_count,
                    delta_views_count, delta_likes_count, delta_comments_count, 
                    delta_reports_count, created_at, updated_at
        """
    
    async def execute_query(self, sql: str) -> list:
        """Выполнить SQL-запрос и вернуть результаты"""
        if not sql.strip().upper().startswith('SELECT'):
            logger.warning(f"Попытка выполнить не SELECT запрос: {sql}")
            return []
        
        await self.connect()
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql)
                result = []
                for row in rows:
                    # Преобразуем каждую строку в словарь
                    row_dict = {}
                    for key in row.keys():
                        row_dict[key] = row[key]
                    result.append(row_dict)
                return result
        except Exception as e:
            logger.error(f"Ошибка выполнения SQL запроса: {e}, SQL: {sql}")
            return []
    
    async def get_stats(self) -> dict:
        """Получить базовую статистику (оставлено для обратной совместимости, если нужно)"""
        await self.connect()
        
        try:
            async with self.pool.acquire() as conn:
                videos_count = await conn.fetchval("SELECT COUNT(*) FROM videos")
                snapshots_count = await conn.fetchval("SELECT COUNT(*) FROM video_snapshots")
                
                return {
                    "videos": videos_count,
                    "snapshots": snapshots_count
                }
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {"videos": 0, "snapshots": 0}

db_service = SimpleDatabase()