FROM python:3.12-slim-bookworm

RUN apt-get update \
  && apt-get install -y --no-install-recommends p7zip-full ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tecdoc_etl ./tecdoc_etl
COPY scripts ./scripts
COPY documentation ./documentation

ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/data
EXPOSE 3000

CMD ["sh", "-c", "exec uvicorn tecdoc_etl.main:app --host 0.0.0.0 --port ${PORT:-3000}"]
