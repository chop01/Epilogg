# Epilogg — log of the games you finished. Data (SQLite + covers) in /data.
FROM python:3.13.9-alpine3.22
LABEL org.opencontainers.image.title="Epilogg" \
      org.opencontainers.image.description="Self-hosted log of the games you finished, with Steam/Xbox/PlayStation import"
RUN apk add --no-cache tzdata \
 && pip install --no-cache-dir fastapi==0.165.0 uvicorn==0.54.0 httpx==0.28.1 openpyxl==3.1.5 psnawp==3.1.0
COPY app.py store.py connectors.py import_xlsx.py /app/
COPY static /app/static
WORKDIR /app
ENV DATA_DIR=/data
VOLUME /data
EXPOSE 8000
USER 1000:1000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
