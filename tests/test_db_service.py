import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from app.services.db_service import SimpleDatabase


class TestSimpleDatabase:
    """Тесты для работы с базой данных"""
    
    @pytest.fixture
    def db_service(self):
        """Создание экземпляра сервиса для тестов"""
        return SimpleDatabase()
    
    @pytest.mark.asyncio
    async def test_connect(self, db_service):
        """Тест подключения к базе данных"""
        with patch('app.services.db_service.asyncpg.create_pool', AsyncMock()) as mock_create_pool:
            # Мокаем настройки
            with patch('app.services.db_service.settings') as mock_settings:
                # Настраиваем database_url как property
                type(mock_settings).database_url = 'postgresql://user:pass@localhost/db'
                
                mock_pool = AsyncMock()
                mock_create_pool.return_value = mock_pool
                
                await db_service.connect()
                
                assert db_service.pool == mock_pool
                mock_create_pool.assert_called_once_with(
                    dsn='postgresql://user:pass@localhost/db',
                    min_size=1,
                    max_size=5
                )
    
    @pytest.mark.asyncio
    async def test_get_schema(self, db_service):
        """Тест получения схемы базы данных"""
        schema = await db_service.get_schema()
        
        assert "Таблицы в базе данных:" in schema
        assert "videos" in schema
        assert "video_snapshots" in schema
    
    @pytest.mark.asyncio
    async def test_execute_query_select(self, db_service):
        """Тест выполнения SELECT запроса"""
        # Создаем мок для соединения
        mock_conn = AsyncMock()
        
        # Создаем мок для строки результата
        mock_row = MagicMock()
        mock_row.keys.return_value = ['id', 'count', 'name']
        mock_row.__getitem__.side_effect = lambda key: {
            'id': 1,
            'count': 42,
            'name': 'test'
        }.get(key)
        
        mock_conn.fetch = AsyncMock(return_value=[mock_row])
        
        # Создаем мок для пула
        mock_pool = AsyncMock()
        
        # Создаем мок для контекстного менеджера acquire
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_pool.acquire = AsyncMock(return_value=mock_acquire_context)
        
        # Назначаем мок пула в сервис
        db_service.pool = mock_pool
        
        sql = "SELECT * FROM videos WHERE id = 1"
        result = await db_service.execute_query(sql)
        
        assert len(result) == 1
        assert result[0]['id'] == 1
        assert result[0]['count'] == 42
        
        # Проверяем, что был вызван connect
        # (хотя в этом тесте мы установили pool напрямую)
        mock_pool.acquire.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_query_non_select(self, db_service):
        """Тест попытки выполнения не-SELECT запроса"""
        sql = "DELETE FROM videos WHERE id = 1"
        result = await db_service.execute_query(sql)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_execute_query_empty_result(self, db_service):
        """Тест выполнения запроса с пустым результатом"""
        # Создаем мок для соединения
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        
        # Создаем мок для пула
        mock_pool = AsyncMock()
        
        # Создаем мок для контекстного менеджера acquire
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_pool.acquire = AsyncMock(return_value=mock_acquire_context)
        
        # Назначаем мок пула в сервис
        db_service.pool = mock_pool
        
        sql = "SELECT * FROM videos WHERE id = 999"
        result = await db_service.execute_query(sql)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_execute_query_exception(self, db_service):
        """Тест обработки исключения при выполнении запроса"""
        # Создаем мок для соединения
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Database error"))
        
        # Создаем мок для пула
        mock_pool = AsyncMock()
        
        # Создаем мок для контекстного менеджера acquire
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_pool.acquire = AsyncMock(return_value=mock_acquire_context)
        
        # Назначаем мок пула в сервис
        db_service.pool = mock_pool
        
        sql = "SELECT invalid_column FROM videos"
        result = await db_service.execute_query(sql)
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_stats_success(self, db_service):
        """Тест получения статистики (успешный случай)"""
        # Создаем мок для соединения
        mock_conn = AsyncMock()
        # Настраиваем fetchval возвращать 100 и 500
        mock_conn.fetchval = AsyncMock(side_effect=[100, 500])
        
        # Создаем мок для пула
        mock_pool = AsyncMock()
        
        # Создаем мок для контекстного менеджера acquire
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_pool.acquire = AsyncMock(return_value=mock_acquire_context)
        
        # Назначаем мок пула в сервис
        db_service.pool = mock_pool
        
        stats = await db_service.get_stats()
        
        assert stats['videos'] == 100
        assert stats['snapshots'] == 500
        
        # Проверяем вызовы SQL запросов
        assert mock_conn.fetchval.call_count == 2
        mock_conn.fetchval.assert_any_call("SELECT COUNT(*) FROM videos")
        mock_conn.fetchval.assert_any_call("SELECT COUNT(*) FROM video_snapshots")
    
    @pytest.mark.asyncio
    async def test_get_stats_exception(self, db_service):
        """Тест получения статистики с исключением"""
        # Создаем мок для соединения
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(side_effect=Exception("Connection error"))
        
        # Создаем мок для пула
        mock_pool = AsyncMock()
        
        # Создаем мок для контекстного менеджера acquire
        mock_acquire_context = AsyncMock()
        mock_acquire_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_pool.acquire = AsyncMock(return_value=mock_acquire_context)
        
        # Назначаем мок пула в сервис
        db_service.pool = mock_pool
        
        stats = await db_service.get_stats()
        
        assert stats['videos'] == 0
        assert stats['snapshots'] == 0