import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from app.services.simple_db import SimpleDatabase


class TestSimpleDatabase:
    """Тесты для работы с базой данных"""
    
    @pytest.fixture
    def db_service(self):
        """Создание экземпляра сервиса для тестов"""
        return SimpleDatabase()
    
    @pytest.fixture
    def mock_connection(self):
        """Mock для соединения с БД"""
        mock = AsyncMock()
        
        # Создаем mock для строки результата
        mock_row = MagicMock()
        mock_row.keys.return_value = ['id', 'count', 'name']
        mock_row.__getitem__.side_effect = lambda key: {
            'id': 1,
            'count': 42,
            'name': 'test'
        }.get(key)
        
        mock.fetch = AsyncMock(return_value=[mock_row])
        mock.fetchval = AsyncMock(return_value=10)
        
        return mock
    
    @pytest.fixture
    def mock_pool(self):
        """Mock для пула соединений"""
        mock = AsyncMock()
        
        # Создаем mock для контекстного менеджера
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock()
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        
        mock.acquire = AsyncMock(return_value=mock_acquire_context)
        return mock
    
    @pytest.mark.asyncio
    async def test_connect(self, db_service):
        """Тест подключения к базе данных"""
        with patch('app.services.simple_db.asyncpg.create_pool', AsyncMock()) as mock_create_pool:
            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool
            
            await db_service.connect()
            
            assert db_service.pool == mock_pool
            mock_create_pool.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_schema(self, db_service):
        """Тест получения схемы базы данных"""
        schema = await db_service.get_schema()
        
        assert "Таблицы в базе данных:" in schema
        assert "videos" in schema
        assert "video_snapshots" in schema
    
    @pytest.mark.asyncio
    async def test_execute_query_select(self, db_service, mock_pool):
        """Тест выполнения SELECT запроса"""
        # Настраиваем mock
        mock_connection = AsyncMock()
        mock_row = MagicMock()
        mock_row.keys.return_value = ['id', 'count', 'name']
        mock_row.__getitem__.side_effect = lambda key: {
            'id': 1,
            'count': 42,
            'name': 'test'
        }.get(key)
        mock_connection.fetch = AsyncMock(return_value=[mock_row])
        
        # Настраиваем контекстный менеджер
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_acquire_context
        
        db_service.pool = mock_pool
        
        sql = "SELECT * FROM videos WHERE id = 1"
        result = await db_service.execute_query(sql)
        
        assert len(result) == 1
        assert result[0]['id'] == 1
        assert result[0]['count'] == 42
    
    @pytest.mark.asyncio
    async def test_execute_query_non_select(self, db_service):
        """Тест попытки выполнения не-SELECT запроса"""
        sql = "DELETE FROM videos WHERE id = 1"
        result = await db_service.execute_query(sql)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_execute_query_empty_result(self, db_service, mock_pool):
        """Тест выполнения запроса с пустым результатом"""
        mock_connection = AsyncMock()
        mock_connection.fetch = AsyncMock(return_value=[])
        
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_acquire_context
        
        db_service.pool = mock_pool
        
        sql = "SELECT * FROM videos WHERE id = 999"
        result = await db_service.execute_query(sql)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_execute_query_exception(self, db_service, mock_pool):
        """Тест обработки исключения при выполнении запроса"""
        mock_connection = AsyncMock()
        mock_connection.fetch = AsyncMock(side_effect=Exception("Database error"))
        
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_acquire_context
        
        db_service.pool = mock_pool
        
        sql = "SELECT invalid_column FROM videos"
        result = await db_service.execute_query(sql)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_stats_success(self, db_service, mock_pool):
        """Тест получения статистики (успешный случай)"""
        mock_connection = AsyncMock()
        # Используем side_effect для последовательных вызовов
        mock_connection.fetchval = AsyncMock(side_effect=[100, 500])
        
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_acquire_context
        
        db_service.pool = mock_pool
        
        stats = await db_service.get_stats()
        
        assert stats['videos'] == 100
        assert stats['snapshots'] == 500
    
    @pytest.mark.asyncio
    async def test_get_stats_exception(self, db_service, mock_pool):
        """Тест получения статистики с исключением"""
        mock_connection = AsyncMock()
        mock_connection.fetchval = AsyncMock(side_effect=Exception("Connection error"))
        
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_acquire_context
        
        db_service.pool = mock_pool
        
        stats = await db_service.get_stats()
        
        assert stats['videos'] == 0
        assert stats['snapshots'] == 0