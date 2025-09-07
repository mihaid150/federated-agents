# syntax=docker/dockerfile:1.7
FROM python:3.10-slim

WORKDIR /app

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
      iputils-ping dnsutils iproute2 && rm -rf /var/lib/apt/lists/*


COPY cloud/requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/requirements.txt

COPY common /app/common
COPY cloud /app/cloud
COPY policies /app/policies

ENV PYTHONPATH=/app

CMD ["python", "-m", "cloud.app_factory"]