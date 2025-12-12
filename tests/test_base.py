import pytest
from app.handlers.base import contains_non_numeric_keywords, format_numeric_result


class TestBaseFunctions:
    """Тесты для базовых функций из base.py"""
    
    def test_contains_non_numeric_keywords_positive(self):
        """Тест обнаружения нечисловых ключевых слов (положительные случаи)"""
        test_cases = [
            "какой видео самое популярное?",
            "какие видео были загружены вчера?",
            "кто загрузил больше всего видео?",
            "покажи последние 5 видео",
            "топ 10 видео по просмотрам",
            "сколько всего видео и какой самый популярный?",
            "какая разница между этими видео?",
            "почему это видео набрало столько просмотров?",
        ]
        
        for query in test_cases:
            assert contains_non_numeric_keywords(query) is True, f"Не распознан запрос: {query}"
    
    def test_contains_non_numeric_keywords_negative(self):
        """Тест обнаружения нечисловых ключевых слов (отрицательные случаи)"""
        test_cases = [
            "Сколько всего видео?",
            "Общее количество просмотров",
            "Среднее число лайков",
            "Сумма комментариев",
            "Максимум отчетов",
            "Минимальное количество дизлайков",
            "Сколько видео создано в ноябре?",
            "Общая сумма всех просмотров",
        ]
        
        for query in test_cases:
            assert contains_non_numeric_keywords(query) is False, f"Ложно распознан запрос: {query}"
    
    def test_format_numeric_result_empty(self):
        """Тест форматирования пустого результата"""
        result = format_numeric_result([])
        assert result == "Нет данных"
    
    def test_format_numeric_result_single_int(self):
        """Тест форматирования одного целого числа"""
        result = format_numeric_result([{"count": 42}])
        assert result == "42"
    
    def test_format_numeric_result_single_float(self):
        """Тест форматирования одного дробного числа"""
        result = format_numeric_result([{"avg": 42.5}])
        assert result == "42.5"
    
    def test_format_numeric_result_single_float_integer(self):
        """Тест форматирования дробного числа, которое является целым"""
        result = format_numeric_result([{"count": 42.0}])
        assert result == "42"
    
    def test_format_numeric_result_multiple_rows(self):
        """Тест форматирования нескольких строк"""
        result = format_numeric_result([
            {"views": 100, "likes": 50},
            {"views": 200, "likes": 75}
        ])
        # Должен вернуть все числовые значения через запятую
        assert result == "100, 50, 200, 75"
    
    def test_format_numeric_result_mixed_types(self):
        """Тест форматирования смешанных типов данных"""
        result = format_numeric_result([
            {"name": "Video1", "views": 100},
            {"name": "Video2", "views": 200}
        ])
        # Должен вернуть только числовые значения
        assert result == "100, 200"
    
    def test_format_numeric_result_none_values(self):
        """Тест форматирования с None значениями"""
        result = format_numeric_result([{"count": None}])
        assert result == "Нет данных"
    
    def test_format_numeric_result_complex(self):
        """Тест сложного форматирования"""
        result = format_numeric_result([
            {"id": 1, "total_views": 1000, "avg_likes": 25.5},
            {"id": 2, "total_views": 2000, "avg_likes": 30.0}
        ])
        # Исправлено: теперь функция возвращает ВСЕ числовые значения
        assert result == "1, 1000, 25.5, 2, 2000, 30"