FROM python:3.11-slim

WORKDIR /app

# Install CPU-only PyTorch first to avoid the 2 GB CUDA download
RUN pip install --no-cache-dir torch==2.7.1 --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV TRANSFORMERS_CACHE=/app/model_cache

# Railway injects PORT at runtime
EXPOSE 8000

CMD ["python", "main.py"]
