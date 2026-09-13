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
ENV SBT_BIN=/opt/sbt/bin/sbt

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Warm Scala dependencies to reduce startup time in cluster.
RUN cd /app/datamart && /opt/sbt/bin/sbt -batch -Dsbt.log.noformat=true -Dsbt.supershell=false -Dsbt.color=false compile || true

CMD ["python3", "-m", "src.lab8_k8s_runner", "--stage", "lab7", "--bootstrap-if-empty"]
