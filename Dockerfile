FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
COPY settings.toml ./
ENV SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1
ENTRYPOINT ["weather-pipeline"]
CMD ["run"]
