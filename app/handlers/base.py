import re
import logging
from aiogram import Router, types
from aiogram.filters import Command
from aiogram.types import Message

logger = logging.getLogger(__name__)
router = Router()

def contains_non_numeric_keywords(query: str) -> bool:
    """Проверяет, содержит ли запрос ключевые слова, которые предполагают нечисловой ответ"""
    query_lower = query.lower()
    
    # Ключевые слова для вопросов с нечисловыми ответами
    non_numeric_patterns = [
        r'\bкакие\b', r'\bкакая\b', r'\bкаков\b',
        r'\bкто\b', r'\bчто\b', r'\bгде\b', r'\bкуда\b', r'\bоткуда\b',
        r'\bпочему\b', r'\bзачем\b', r'\bкак\b', r'\bкогда\b',
        r'\bназови\b', r'\bперечисли\b', r'\bпокажи\b', r'\bвыведи\b',
        r'\bрасскажи\b', r'\bопиши\b', r'\bобъясни\b', r'\bдай\b',
        r'\bтоп\b', r'\bсписок\b', r'\bтаблица\b', r'\bрейтинг\b',
        r'\bлучшие\b', r'\bхудшие\b', r'\bпоследние\b', r'\bпервые\b',
        r'\bкаковы\b', r'\bчем\b', r'\bкому\b', r'\bкого\b',
        r'\bо чем\b', r'\bпро что\b', r'\bкаким\b', r'\bкакими\b'
    ]
    
    # Проверяем, содержит ли запрос неподходящие ключевые слова
    for pattern in non_numeric_patterns:
        if re.search(pattern, query_lower):
            return True
    
    return False

def format_numeric_result(results: list) -> str:
    """Форматирует результаты запроса в простое текстовое представление чисел"""
    if not results:
        return "Нет данных"
    
    # Если результат содержит только одну строку и одну колонку
    if len(results) == 1:
        row = results[0]
        if len(row) == 1:
            # Извлекаем первое (и единственное) значение
            value = list(row.values())[0]
            if value is None:
                return "Нет данных"
            
            # Преобразуем в строку, убирая дробные нули
            if isinstance(value, (int, float)):
                if isinstance(value, float) and value.is_integer():
                    return str(int(value))
                return str(value)
            return str(value)
    
    # Пытаемся извлечь числовые значения из результатов
    numeric_values = []
    for row in results:
        for key, value in row.items():
            if isinstance(value, (int, float)):
                if isinstance(value, float) and value.is_integer():
                    numeric_values.append(int(value))
                else:
                    numeric_values.append(value)
    
    if numeric_values:
        if len(numeric_values) == 1:
            return str(numeric_values[0])
        else:
            # Возвращаем все числовые значения через запятую
            return ", ".join(str(v) for v in numeric_values)
    
    return "Нет числовых данных для ответа"