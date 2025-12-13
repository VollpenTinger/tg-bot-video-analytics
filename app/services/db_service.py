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
ТАБЛИЦЫ БАЗЫ ДАННЫХ:

1. ТАБЛИЦА "videos" (основная информация о видео):
   - id: UUID (первичный ключ, уникальный идентификатор видео)
   - creator_id: UUID (идентификатор креатора, который загрузил видео)
   - video_created_at: TIMESTAMP (дата создания видео)
   - views_count: INTEGER (общее количество просмотров - ИТОГОВАЯ статистика)
   - likes_count: INTEGER (общее количество лайков - ИТОГОВАЯ статистика)
   - comments_count: INTEGER (общее количество комментариев - ИТОГОВАЯ статистика)
   - reports_count: INTEGER (общее количество жалоб - ИТОГОВАЯ статистика)
   - created_at: TIMESTAMP (дата записи в базу)
   - updated_at: TIMESTAMP (дата обновления)

2. ТАБЛИЦА "video_snapshots" (исторические снапшоты статистики):
   - id: UUID (первичный ключ)
   - video_id: UUID (ссылка на видео из таблицы videos, ВНЕШНИЙ КЛЮЧ)
   - views_count: INTEGER (количество просмотров на момент снапшота)
   - likes_count: INTEGER (количество лайков на момент снапшота)
   - comments_count: INTEGER (количество комментариев на момент снапшота)
   - reports_count: INTEGER (количество жалоб на момент снапшота)
   - delta_views_count: INTEGER (изменение просмотров с предыдущего снапшота)
   - delta_likes_count: INTEGER (изменение лайков с предыдущего снапшота)
   - delta_comments_count: INTEGER (изменение комментариев с предыдущего снапшота)
   - delta_reports_count: INTEGER (изменение жалоб с предыдущего снапшота)
   - created_at: TIMESTAMP (дата создания снапшота)
   - updated_at: TIMESTAMP (дата обновления снапшота)

ВАЖНЫЕ СВЯЗИ:
1. video_snapshots.video_id → videos.id (один ко многим)
   Одно видео может иметь множество снапшотов статистики

КЛЮЧЕВЫЕ ПРАВИЛА ДЛЯ ЗАПРОСОВ:
1. Для получения ИТОГОВОЙ статистики видео используй таблицу "videos"
   - В ней уже содержатся итоговые значения (views_count, likes_count и т.д.)
   - Не нужно обращаться к video_snapshots для итоговой статистики
   
2. Для получения ИСТОРИЧЕСКОЙ статистики или изменений используй таблицу "video_snapshots"
   - Используй ее только если в запросе упоминается "снапшот", "изменение", "динамика", "история"
   
3. Если запрос содержит слова "итоговая статистика", "итоговый", "по итогам", "общее" - используй таблицу "videos"

4. Для фильтрации по креатору используй поле videos.creator_id

ПРИМЕРЫ ПРАВИЛЬНЫХ SQL-ЗАПРОСОВ:

1. Запрос: "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 набрали больше 10000 просмотров по итоговой статистике?"
   SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63' AND views_count > 10000

2. Запрос: "Общее количество просмотров всех видео у креатора X"
   SQL: SELECT SUM(views_count) FROM videos WHERE creator_id = 'X'

3. Запрос: "Среднее количество лайков на видео"
   SQL: SELECT AVG(likes_count) FROM videos

4. Запрос: "Сколько всего видео в базе?"
   SQL: SELECT COUNT(*) FROM videos

5. Запрос: "Максимальное количество комментариев на видео"
   SQL: SELECT MAX(comments_count) FROM videos

6. Запрос: "Изменение просмотров за последний день (для снапшотов)"
   SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'

ВАЖНО: Для запросов об итоговой статистике видео НЕ используй таблицу video_snapshots!
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