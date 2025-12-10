FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/app /opt/scripts /opt/data /opt/bot

WORKDIR /opt

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN ln -sf /opt/app /app && \
    ln -sf /opt/scripts /scripts && \
    ln -sf /opt/bot /bot

ENV PYTHONPATH=/opt/app:$PYTHONPATH

WORKDIR /opt

CMD ["python", "--version"]