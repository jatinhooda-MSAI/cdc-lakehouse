FROM python:3.11.17-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
