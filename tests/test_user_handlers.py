import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from aiogram.types import Message, Chat, User
from app.services.db_service import db_service
from app.services.gpt_service import gpt_service
from app.services.cache_service import cache_service
from app.handlers.user_handlers import handle_text, cmd_start


class TestUserHandlers:
    """Тесты для обработчиков пользовательских сообщений"""
    
    @pytest.fixture
    def mock_message(self):
        """Создание mock сообщения"""
        message = MagicMock(spec=Message)
        message.chat = MagicMock(spec=Chat)
        message.chat.id = 123
        message.from_user = MagicMock(spec=User)
        message.from_user.id = 123
        message.message_id = 1
        
        return message
    
    @pytest.mark.asyncio
    async def test_cmd_start(self, mock_message):
        """Тест команды /start"""
        mock_message.text = "/start"
        
        with patch.object(mock_message, 'answer', AsyncMock()) as mock_answer:
            await cmd_start(mock_message)
            
            mock_answer.assert_called_once()
            args, kwargs = mock_answer.call_args
            response_text = args[0] if args else kwargs.get('text', '')
            
            assert "Привет! Я бот для анализа видео-статистики." in response_text
            assert "Примеры подходящих запросов:" in response_text
    
    @pytest.mark.asyncio
    async def test_handle_text_too_short(self, mock_message):
        """Тест обработки слишком короткого запроса"""
        mock_message.text = "коротко"
        
        with patch.object(mock_message, 'answer', AsyncMock()) as mock_answer:
            await handle_text(mock_message, bot=AsyncMock())
            
            mock_answer.assert_called_once()
            args, kwargs = mock_answer.call_args
            response_text = args[0] if args else kwargs.get('text', '')
            
            assert "слишком короткий" in response_text.lower()
    
    @pytest.mark.asyncio
    async def test_handle_text_with_non_numeric_keywords(self, mock_message):
        """Тест обработки запроса с нечисловыми ключевыми словами"""
        mock_message.text = "Какое видео самое популярное?"
        
        with patch('app.handlers.user_handlers.contains_non_numeric_keywords', return_value=True):
            with patch.object(mock_message, 'answer', AsyncMock()) as mock_answer:
                await handle_text(mock_message, bot=AsyncMock())
                
                mock_answer.assert_called_once()
                args, kwargs = mock_answer.call_args
                response_text = args[0] if args else kwargs.get('text', '')
                
                assert "отвечаю только на количественные вопросы" in response_text.lower()
    
    @pytest.mark.asyncio
    async def test_handle_text_cached(self, mock_message):
        """Тест обработки запроса с найденным кешем"""
        mock_message.text = "Сколько всего видео?"
        
        mock_bot = AsyncMock()
        mock_cache_service = AsyncMock()
        mock_cache_service.get_cached_result.return_value = "42"
        
        with patch('app.handlers.user_handlers.cache_service', mock_cache_service):
            with patch('app.handlers.user_handlers.contains_non_numeric_keywords', return_value=False):
                with patch.object(mock_message, 'answer', AsyncMock()) as mock_answer:
                    await handle_text(mock_message, bot=mock_bot)
                    
                    mock_answer.assert_called_once_with("42")
                    mock_bot.send_chat_action.assert_not_called()