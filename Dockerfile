# syntax=docker/dockerfile:1.6
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/
RUN chown -R appuser:appuser /app
USER appuser

ENV OUTPUT_DIR=/app/output
RUN mkdir -p "$OUTPUT_DIR"

ENTRYPOINT ["python", "main.py"]
