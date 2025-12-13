FROM python:3.11-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Создаем не-root пользователя
RUN groupadd -r botuser && useradd -r -g botuser botuser

WORKDIR /opt

# Копирование requirements первым (для кеширования слоев)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY . . 

# Создаем директории для логов
RUN mkdir -p /opt/logs && chown -R botuser:botuser /opt/logs

# Права на файлы
RUN chown -R botuser:botuser /opt

# Переключаемся на не-root пользователя
USER botuser

# Проверка здоровья (опционально)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Команда запуска (переопределяется в docker-compose)
CMD ["python", "bot/bot.py"]
