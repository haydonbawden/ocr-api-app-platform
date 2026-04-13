FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080 \
    APP_HOME=/app \
    TMP_DIR=/tmp/ocr-api \
    MAX_UPLOAD_MB=50 \
    OCR_TIMEOUT_SECONDS=900 \
    API_KEY=change-me

WORKDIR ${APP_HOME}

RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    ghostscript \
    qpdf \
    pngquant \
    unpaper \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
# Pillow and PyMuPDF required for autorotate endpoint
RUN pip install --no-cache-dir ocrmypdf

COPY app.py gunicorn_conf.py ./

RUN mkdir -p ${TMP_DIR} && chmod 700 ${TMP_DIR}

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/health || exit 1

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn_conf.py", "app:app"]
