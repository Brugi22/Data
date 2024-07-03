FROM python:3.8-slim-buster

WORKDIR /app

ADD . /app

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]