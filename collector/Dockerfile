FROM python:3.11-slim

# Cài Chrome (keyring mới, không dùng apt-key)
RUN set -eux; \
    apt-get update && apt-get install -y curl gnupg ca-certificates unzip && rm -rf /var/lib/apt/lists/*; \
    mkdir -p /etc/apt/keyrings; \
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-linux-signing-key.gpg; \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list; \
    apt-get update && apt-get install -y google-chrome-stable && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crawl_data.py .
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "crawl_data.py"]
