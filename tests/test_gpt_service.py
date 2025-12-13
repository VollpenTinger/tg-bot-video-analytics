import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from app.services.gpt_service import SimpleYandexGPT


class TestSimpleYandexGPT:
    """Тесты для работы с Yandex GPT"""
    
    @pytest.fixture
    def gpt_service(self):
        """Создание экземпляра сервиса для тестов"""
        service = SimpleYandexGPT()
        service.api_key = "test-api-key"
        service.folder_id = "test-folder-id"
        service.url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        return service
    
    @pytest.mark.asyncio
    async def test_ask_gpt_success(self, gpt_service):
        """Тест успешного запроса к Yandex GPT"""
        # Создаем мок для ответа
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "result": {
                "alternatives": [{
                    "message": {
                        "text": "SELECT COUNT(*) FROM videos;"
                    }
                }]
            }
        })
        
        # Создаем мок для контекстного менеджера post
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)
        
        # Создаем мок для сессии
        mock_session = AsyncMock()
        mock_session.post = AsyncMock(return_value=mock_post_context)
        
        with patch('app.services.gpt_service.aiohttp.ClientSession', return_value=mock_session):
            result = await gpt_service.ask_gpt(
                "Сколько всего видео?",
                "Таблицы: videos, snapshots"
            )
            
            # Очищаем SQL от возможных форматирований
            if result:
                result = result.replace('```sql', '').replace('```', '').strip().rstrip(';')
            
            assert result == "SELECT COUNT(*) FROM videos"
            
            # Проверяем вызов
            mock_session.post.assert_called_once()
            call_args = mock_session.post.call_args
            
            # Проверяем URL и заголовки
            assert call_args[0][0] == gpt_service.url
            assert call_args[1]['headers']['Authorization'] == f"Bearer {gpt_service.api_key}"
            assert call_args[1]['headers']['x-folder-id'] == gpt_service.folder_id
    
    @pytest.mark.asyncio
    async def test_ask_gpt_api_error(self, gpt_service):
        """Тест ошибки API"""
        # Создаем мок для ответа с ошибкой
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        
        # Создаем мок для контекстного менеджера post
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)
        
        # Создаем мок для сессии
        mock_session = AsyncMock()
        mock_session.post = AsyncMock(return_value=mock_post_context)
        
        with patch('app.services.gpt_service.aiohttp.ClientSession', return_value=mock_session):
            result = await gpt_service.ask_gpt("Сколько всего видео?", "")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_ask_gpt_exception(self, gpt_service):
        """Тест исключения при запросе"""
        # Создаем мок для сессии, которая выбрасывает исключение
        mock_session = AsyncMock()
        mock_session.post = AsyncMock(side_effect=Exception("Network error"))
        
        with patch('app.services.gpt_service.aiohttp.ClientSession', return_value=mock_session):
            result = await gpt_service.ask_gpt("Сколько всего видео?", "")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_ask_gpt_empty_response(self, gpt_service):
        """Тест пустого ответа от API"""
        # Создаем мок для ответа без данных
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={})
        
        # Создаем мок для контекстного менеджера post
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)
        
        # Создаем мок для сессии
        mock_session = AsyncMock()
        mock_session.post = AsyncMock(return_value=mock_post_context)
        
        with patch('app.services.gpt_service.aiohttp.ClientSession', return_value=mock_session):
            result = await gpt_service.ask_gpt("Сколько всего видео?", "")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_ask_gpt_sql_cleaning(self, gpt_service):
        """Тест очистки SQL от форматирования"""
        test_cases = [
            ("```sql\nSELECT COUNT(*) FROM videos;\n```", "SELECT COUNT(*) FROM videos"),
            ("SELECT COUNT(*) FROM videos;", "SELECT COUNT(*) FROM videos"),
            ("```SELECT COUNT(*) FROM videos```", "SELECT COUNT(*) FROM videos"),
            ("  SELECT COUNT(*) FROM videos  ", "SELECT COUNT(*) FROM videos"),
        ]
        
        for raw_sql, expected in test_cases:
            # Создаем мок для ответа
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={
                "result": {
                    "alternatives": [{
                        "message": {
                            "text": raw_sql
                        }
                    }]
                }
            })
            
            # Создаем мок для контекстного менеджера post
            mock_post_context = AsyncMock()
            mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_post_context.__aexit__ = AsyncMock(return_value=None)
            
            # Создаем мок для сессии
            mock_session = AsyncMock()
            mock_session.post = AsyncMock(return_value=mock_post_context)
            
            with patch('app.services.gpt_service.aiohttp.ClientSession', return_value=mock_session):
                result = await gpt_service.ask_gpt("тест", "")
                
                assert result == expected