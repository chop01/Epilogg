# Registro giochi (app.py + static/). Dopo modifiche al codice o alle versioni: docker compose build.
FROM python:3.13.9-alpine3.22
RUN apk add --no-cache tzdata \
 && pip install --no-cache-dir fastapi==0.165.0 uvicorn==0.54.0 httpx==0.28.1 openpyxl==3.1.5 psnawp==3.1.0
COPY app.py store.py connectors.py import_xlsx.py /app/
COPY static /app/static
WORKDIR /app
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
