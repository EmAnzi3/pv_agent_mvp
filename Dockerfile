FROM mcr.microsoft.com/playwright/python:v1.51.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY sql ./sql
COPY README.md .

CMD ["python", "-m", "app.main", "scheduler"]