FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates openjdk-21-jdk \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/sbt/sbt/releases/download/v1.11.7/sbt-1.11.7.tgz \
    | tar -xz -C /opt \
    && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "-m", "src.lab7_mart_pipeline", "--bootstrap-if-empty", "--datamart-config", "conf/datamart-docker.conf"]
