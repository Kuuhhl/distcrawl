FROM mcr.microsoft.com/playwright:v1.50.0-noble

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    LOCAL_STORAGE_PATH=/data/crawler_results \
    PATH="/app/.venv/bin:$PATH"

RUN apt-get update && \
    apt-get install -y xvfb unzip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY uv.lock pyproject.toml ./
COPY common/pyproject.toml common/pyproject.toml
COPY worker/pyproject.toml worker/pyproject.toml
COPY scripts/pyproject.toml scripts/pyproject.toml

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --no-install-project --package distcrawl

COPY common/ common/
COPY worker/ worker/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --package distcrawl

COPY --chmod=755 worker/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

CMD ["dist-crawl"]
