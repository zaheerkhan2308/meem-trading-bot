FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV HF_HOME=/app/model_cache

# Railway injects PORT at runtime
EXPOSE 8000

CMD ["python", "main.py"]
