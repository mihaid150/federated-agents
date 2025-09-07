FROM python:3.10-slim

WORKDIR /app

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
      iputils-ping dnsutils iproute2 && rm -rf /var/lib/apt/lists/*

COPY edge/requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/requirements.txt

COPY common /app/common
COPY edge /app/edge

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["python", "-m", "edge.app_factory"]
