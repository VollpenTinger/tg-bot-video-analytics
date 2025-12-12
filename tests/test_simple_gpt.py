import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import aiohttp

from app.services.simple_gpt import SimpleYandexGPT


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
        mock_response = {
            "result": {
                "alternatives": [{
                    "message": {
                        "text": "SELECT COUNT(*) FROM videos;"
                    }
                }]
            }
        }
        
        # Создаем mock для response
        mock_response_obj = AsyncMock()
        mock_response_obj.status = 200
        mock_response_obj.json = AsyncMock(return_value=mock_response)
        
        # Создаем mock для контекстного менеджера post()
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response_obj)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)
        
        # Создаем mock для сессии
        mock_session = AsyncMock()
        mock_session.post.return_value = mock_post_context
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await gpt_service.ask_gpt(
                "Сколько всего видео?",
                "Таблицы: videos, snapshots"
            )
            
            assert result == "SELECT COUNT(*) FROM videos"
            mock_session.post.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_ask_gpt_api_error(self, gpt_service):
        """Тест ошибки API"""
        mock_response_obj = AsyncMock()
        mock_response_obj.status = 500
        mock_response_obj.text = AsyncMock(return_value="Internal Server Error")
        
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response_obj)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)
        
        mock_session = AsyncMock()
        mock_session.post.return_value = mock_post_context
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await gpt_service.ask_gpt("Сколько всего видео?", "")
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_ask_gpt_exception(self, gpt_service):
        """Тест исключения при запросе"""
        mock_session = AsyncMock()
        mock_session.post = AsyncMock(side_effect=Exception("Network error"))
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await gpt_service.ask_gpt("Сколько всего видео?", "")
            
            assert result is None