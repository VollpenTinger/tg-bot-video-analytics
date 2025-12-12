import json
import hashlib
import logging
from typing import Optional
import redis.asyncio as redis
from datetime import datetime

from app.core.config import settings

logger = logging.getLogger(__name__)

class CacheService:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.ttl = settings.REDIS_CACHE_TTL
        self.enabled = settings.ENABLE_CACHE
        
    async def connect(self):
        """Подключиться к Redis"""
        if not self.enabled:
            logger.info("Кеширование отключено в настройках")
            return
            
        try:
            logger.info(f"🔄 Подключаюсь к Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
            
            connection_kwargs = {
                "host": settings.REDIS_HOST,
                "port": settings.REDIS_PORT,
                "db": settings.REDIS_DB,
                "encoding": "utf-8",
                "decode_responses": True,
                "socket_connect_timeout": 5,
                "retry_on_timeout": True
            }
            
            # Добавляем пароль только если он есть и не пустой
            if settings.REDIS_PASSWORD and settings.REDIS_PASSWORD.strip():
                connection_kwargs["password"] = settings.REDIS_PASSWORD
                logger.info("🔐 Используется аутентификация с паролем")
            else:
                logger.info("🔓 Подключение без пароля")
            
            self.redis_client = redis.Redis(**connection_kwargs)
            
            # Проверяем подключение
            pong = await self.redis_client.ping()
            logger.info(f"✅ Успешно подключились к Redis, ping: {pong}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Redis: {e}", exc_info=True)
            self.redis_client = None
            
    async def disconnect(self):
        """Отключиться от Redis"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("🔌 Отключились от Redis")
            
    def _get_cache_key(self, query: str) -> str:
        """Генерация ключа для кеша на основе запроса"""
        query_hash = hashlib.md5(query.strip().lower().encode()).hexdigest()
        return f"cache:query:{query_hash}"
    
    async def get_cached_result(self, query: str) -> Optional[str]:
        """Получить закешированный результат"""
        if not self.enabled or not self.redis_client:
            logger.debug("Кеширование отключено или Redis не подключен")
            return None
            
        try:
            cache_key = self._get_cache_key(query)
            cached = await self.redis_client.get(cache_key)
            
            if cached:
                logger.info(f"✅ Найден кеш для запроса: '{query[:30]}...'")
                return cached
            else:
                logger.debug(f"❌ Кеш не найден для запроса: '{query[:30]}...'")
        except Exception as e:
            logger.error(f"Ошибка получения из кеша: {e}", exc_info=True)
            
        return None
        
    async def save_to_cache(self, query: str, result: str) -> None:
        """Сохранить результат в кеш"""
        if not self.enabled or not self.redis_client:
            logger.debug("Кеширование отключено или Redis не подключен")
            return
            
        try:
            # Проверяем, нужно ли кешировать (с учетом MIN_CACHE_LENGTH)
            should_cache = await self._should_cache_query(query)
            
            if should_cache:
                cache_key = self._get_cache_key(query)
                await self.redis_client.setex(cache_key, self.ttl, result)
                logger.info(f"💾 Результат сохранен в кеш: '{query[:30]}...' -> {result}")
            else:
                logger.debug(f"⚠️ Запрос не достиг лимита для кеширования: '{query[:30]}...'")
                
        except Exception as e:
            logger.error(f"Ошибка сохранения в кеш: {e}", exc_info=True)
            
    async def _should_cache_query(self, query: str) -> bool:
        """Определить, нужно ли кешировать запрос"""
        try:
            # Сначала проверяем, есть ли уже кеш
            cache_key = self._get_cache_key(query)
            existing = await self.redis_client.get(cache_key)
            if existing:
                return False  # Уже есть в кеше, не нужно повторно сохранять
            
            # Проверяем статистику использования
            stats_key = f"stats:query:{hashlib.md5(query.strip().lower().encode()).hexdigest()}"
            
            # Увеличиваем счетчик использования
            usage_count = await self.redis_client.hincrby(stats_key, "usage_count", 1)
            
            if usage_count == 1:
                # Первое использование
                await self.redis_client.hset(stats_key, mapping={
                    "first_used": datetime.now().isoformat(),
                    "last_used": datetime.now().isoformat(),
                    "query": query[:500]
                })
                await self.redis_client.expire(stats_key, 7 * 24 * 3600)
            else:
                # Обновляем время последнего использования
                await self.redis_client.hset(stats_key, "last_used", datetime.now().isoformat())
            
            # Кешируем, если достигли порога
            if usage_count >= settings.MIN_CACHE_LENGTH:
                logger.info(f"📈 Запрос достиг порога кеширования (использован {usage_count} раз): '{query[:30]}...'")
                return True
            else:
                logger.debug(f"📊 Запрос использован {usage_count} раз (нужно {settings.MIN_CACHE_LENGTH}): '{query[:30]}...'")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка проверки статистики запроса: {e}", exc_info=True)
            return False

# Создаем глобальный экземпляр сервиса кеширования
cache_service = CacheService()