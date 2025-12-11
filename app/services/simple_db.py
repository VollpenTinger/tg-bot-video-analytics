import asyncpg
from app.core.config import settings

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
            return []
        
        await self.connect()
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]
    
    async def get_stats(self) -> dict:
        """Получить базовую статистику"""
        await self.connect()
        
        async with self.pool.acquire() as conn:
            videos_count = await conn.fetchval("SELECT COUNT(*) FROM videos")
            snapshots_count = await conn.fetchval("SELECT COUNT(*) FROM video_snapshots")
            
            return {
                "videos": videos_count,
                "snapshots": snapshots_count,
                "message": f"В базе {videos_count} видео и {snapshots_count} снапшотов"
            }

db_service = SimpleDatabase()