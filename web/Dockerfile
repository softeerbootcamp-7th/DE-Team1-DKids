FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    fontconfig \
    fonts-nanum \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]