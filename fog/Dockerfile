FROM python:3.10-slim

WORKDIR /app

# Minimal OS deps (handy for networking + sklearn openmp runtime)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
      iputils-ping dnsutils iproute2 && rm -rf /var/lib/apt/lists/*

# Python deps
COPY fog/requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/requirements.txt

# App code: only what's needed by fog-agent
COPY common /app/common
COPY fog /app/fog

ENV PYTHONPATH=/app

# Headless, event-driven process (no uvicorn)
CMD ["python", "-m", "fog.app_factory"]