FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY uv.lock pyproject.toml ./
COPY common/pyproject.toml common/pyproject.toml
COPY scripts/pyproject.toml scripts/pyproject.toml
COPY worker/pyproject.toml worker/pyproject.toml

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --no-install-project --package dist-scripts

COPY common/ common/
COPY scripts/ scripts/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --package dist-scripts

WORKDIR /app/scripts

CMD ["python"]
