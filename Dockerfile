FROM python:3.12-slim

WORKDIR /app

# Install CPU-only PyTorch first to avoid the 2 GB CUDA download
RUN pip install --no-cache-dir torch==2.7.1 --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV HF_HOME=/app/model_cache

# Bake FinBERT into the image so runtime never needs to download it.
# low_cpu_mem_usage loads weights one tensor at a time, keeping peak RAM below 2× model size.
RUN python -c "\
from transformers import AutoTokenizer, AutoModelForSequenceClassification; \
AutoTokenizer.from_pretrained('ProsusAI/finbert'); \
AutoModelForSequenceClassification.from_pretrained('ProsusAI/finbert', low_cpu_mem_usage=True)"

# Railway injects PORT at runtime
EXPOSE 8000

CMD ["python", "main.py"]
