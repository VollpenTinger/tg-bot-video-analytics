import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
import json

from app.services.cache_service import CacheService


class TestCacheService:
    """Тесты для сервиса кеширования"""
    
    @pytest.fixture
    def cache_service(self):
        """Создание экземпляра сервиса для тестов"""
        service = CacheService()
        service.enabled = True
        service.ttl = 3600
        return service
    
    @pytest.fixture
    def mock_redis(self):
        """Mock для Redis клиента"""
        mock = AsyncMock()
        mock.ping = AsyncMock(return_value=True)
        mock.get = AsyncMock()
        mock.setex = AsyncMock()
        mock.hincrby = AsyncMock()
        mock.hset = AsyncMock()
        mock.expire = AsyncMock()
        return mock
    
    @pytest.mark.asyncio
    async def test_connect_success(self, cache_service, mock_redis):
        """Тест успешного подключения к Redis"""
        with patch('app.services.cache_service.redis.Redis', return_value=mock_redis):
            with patch('app.services.cache_service.settings') as mock_settings:
                mock_settings.ENABLE_CACHE = True
                mock_settings.REDIS_HOST = 'localhost'
                mock_settings.REDIS_PORT = 6379
                mock_settings.REDIS_DB = 0
                mock_settings.REDIS_PASSWORD = ''
                
                await cache_service.connect()
                
                assert cache_service.redis_client == mock_redis
                mock_redis.ping.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connect_with_password(self, cache_service, mock_redis):
        """Тест подключения к Redis с паролем"""
        with patch('app.services.cache_service.redis.Redis', return_value=mock_redis):
            with patch('app.services.cache_service.settings') as mock_settings:
                mock_settings.ENABLE_CACHE = True
                mock_settings.REDIS_HOST = 'localhost'
                mock_settings.REDIS_PORT = 6379
                mock_settings.REDIS_DB = 0
                mock_settings.REDIS_PASSWORD = 'secret'
                
                await cache_service.connect()
                
                assert cache_service.redis_client == mock_redis
    
    @pytest.mark.asyncio
    async def test_connect_disabled(self, cache_service):
        """Тест отключенного кеширования"""
        cache_service.enabled = False
        
        await cache_service.connect()
        
        assert cache_service.redis_client is None
    
    @pytest.mark.asyncio
    async def test_get_cache_key(self, cache_service):
        """Тест генерации ключа кеша"""
        query = "Сколько всего видео?"
        cache_key = cache_service._get_cache_key(query)
        
        assert cache_key.startswith("cache:query:")
        # MD5 хеш имеет длину 32 символа, а не 16
        assert len(cache_key) == 32 + len("cache:query:")
    
    @pytest.mark.asyncio
    async def test_get_cached_result_found(self, cache_service, mock_redis):
        """Тест получения закешированного результата (найден)"""
        cache_service.redis_client = mock_redis
        cache_key = cache_service._get_cache_key("тестовый запрос")
        mock_redis.get.return_value = "42"
        
        result = await cache_service.get_cached_result("тестовый запрос")
        
        assert result == "42"
        mock_redis.get.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cached_result_not_found(self, cache_service, mock_redis):
        """Тест получения закешированного результата (не найден)"""
        cache_service.redis_client = mock_redis
        mock_redis.get.return_value = None
        
        result = await cache_service.get_cached_result("тестовый запрос")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_save_to_cache_success(self, cache_service, mock_redis):
        """Тест сохранения в кеш"""
        cache_service.redis_client = mock_redis
        
        with patch.object(cache_service, '_should_cache_query', AsyncMock(return_value=True)):
            await cache_service.save_to_cache("тестовый запрос", "результат")
            
            mock_redis.setex.assert_called_once()
            args = mock_redis.setex.call_args[0]
            assert args[1] == cache_service.ttl
            assert args[2] == "результат"
    
    @pytest.mark.asyncio
    async def test_should_cache_query_new(self, cache_service, mock_redis):
        """Тест проверки необходимости кеширования (новый запрос)"""
        cache_service.redis_client = mock_redis
        
        with patch('app.services.cache_service.settings') as mock_settings:
            mock_settings.MIN_CACHE_LENGTH = 3
            
            # Настроим мок: нет кеша, первый вызов hincrby возвращает 1
            mock_redis.get.return_value = None
            mock_redis.hincrby.return_value = 1
            
            result = await cache_service._should_cache_query("тестовый запрос")
            
            assert result is False  # Не достигли порога
            mock_redis.hincrby.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_should_cache_query_reached_threshold(self, cache_service, mock_redis):
        """Тест проверки необходимости кеширования (достигнут порог)"""
        cache_service.redis_client = mock_redis
        
        with patch('app.services.cache_service.settings') as mock_settings:
            mock_settings.MIN_CACHE_LENGTH = 3
            
            # Настроим мок: нет кеша, вызов hincrby возвращает 3 (достигли порога)
            mock_redis.get.return_value = None
            mock_redis.hincrby.return_value = 3
            
            result = await cache_service._should_cache_query("тестовый запрос")
            
            assert result is True  # Достигли порога
    
    @pytest.mark.asyncio
    async def test_disconnect(self, cache_service, mock_redis):
        """Тест отключения от Redis"""
        cache_service.redis_client = mock_redis
        
        await cache_service.disconnect()
        
        mock_redis.close.assert_called_once()